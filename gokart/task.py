from __future__ import annotations

import functools
import hashlib
import inspect
import os
import random
import types
from collections.abc import Generator, Iterable
from importlib import import_module
from logging import getLogger
from typing import Any, Callable, Generic, TypeVar, overload

import luigi
import pandas as pd
from luigi.parameter import ParameterVisibility

import gokart
import gokart.target
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params, make_task_lock_params_for_run
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_run_with_lock
from gokart.file_processor import FileProcessor
from gokart.pandas_type_config import PandasTypeConfigMap
from gokart.parameter import ExplicitBoolParameter, ListTaskInstanceParameter, TaskInstanceParameter
from gokart.required_task_output import RequiredTaskOutput
from gokart.target import TargetOnKart
from gokart.task_complete_check import task_complete_check_wrapper
from gokart.utils import FlattenableItems, flatten, map_flattenable_items

logger = getLogger(__name__)


T = TypeVar('T')
K = TypeVar('K')


# NOTE: inherited from AssertionError for backward compatibility (Formerly, Gokart raises that exception when a task dumps an empty DataFrame).
class EmptyDumpError(AssertionError):
    """Raised when the task attempts to dump an empty DataFrame even though it is prohibited (``fail_on_empty_dump`` is set to True)"""


class TaskOnKart(luigi.Task, Generic[T]):
    """
    This is a wrapper class of luigi.Task.

    The key methods of a TaskOnKart are:

    * :py:meth:`make_target` - this makes output target with a relative file path.
    * :py:meth:`make_model_target` - this makes output target for models which generate multiple files to save.
    * :py:meth:`load` - this loads input files of this task.
    * :py:meth:`dump` - this save a object as output of this task.
    """

    workspace_directory: str = luigi.Parameter(
        default='./resources/', description='A directory to set outputs on. Please use a path starts with s3:// when you use s3.', significant=False
    )
    local_temporary_directory: str = luigi.Parameter(default='./resources/tmp/', description='A directory to save temporary files.', significant=False)
    rerun: bool = luigi.BoolParameter(default=False, description='If this is true, this task will run even if all output files exist.', significant=False)
    strict_check: bool = luigi.BoolParameter(
        default=False, description='If this is true, this task will not run only if all input and output files exist.', significant=False
    )
    modification_time_check: bool = luigi.BoolParameter(
        default=False,
        description='If this is true, this task will not run only if all input and output files exist,'
        ' and all input files are modified before output file are modified.',
        significant=False,
    )
    serialized_task_definition_check: bool = luigi.BoolParameter(
        default=False,
        description='If this is true, even if all outputs are present,this task will be executed if any changes have been made to the code.',
        significant=False,
    )
    delete_unnecessary_output_files: bool = luigi.BoolParameter(
        default=False, description='If this is true, delete unnecessary output files.', significant=False
    )
    significant: bool = luigi.BoolParameter(
        default=True, description='If this is false, this task is not treated as a part of dependent tasks for the unique id.', significant=False
    )
    fix_random_seed_methods: tuple[str] = luigi.ListParameter(
        default=['random.seed', 'numpy.random.seed'], description='Fix random seed method list.', significant=False
    )
    FIX_RANDOM_SEED_VALUE_NONE_MAGIC_NUMBER = -42497368
    fix_random_seed_value: int = luigi.IntParameter(
        default=FIX_RANDOM_SEED_VALUE_NONE_MAGIC_NUMBER, description='Fix random seed method value.', significant=False
    )  # FIXME: should fix with OptionalIntParameter after newer luigi (https://github.com/spotify/luigi/pull/3079) will be released

    redis_host: str | None = luigi.OptionalParameter(default=None, description='Task lock check is deactivated, when None.', significant=False)
    redis_port: int | None = luigi.OptionalIntParameter(
        default=None,
        description='Task lock check is deactivated, when None.',
        significant=False,
    )
    redis_timeout: int = luigi.IntParameter(default=180, description='Redis lock will be released after `redis_timeout` seconds', significant=False)

    fail_on_empty_dump: bool = ExplicitBoolParameter(default=False, description='Fail when task dumps empty DF', significant=False)
    store_index_in_feather: bool = ExplicitBoolParameter(
        default=True, description='Wether to store index when using feather as a output object.', significant=False
    )

    cache_unique_id: bool = ExplicitBoolParameter(default=True, description='Cache unique id during runtime', significant=False)
    should_dump_supplementary_log_files: bool = ExplicitBoolParameter(
        default=True,
        description='Whether to dump supplementary files (task_log, random_seed, task_params, processing_time, module_versions) or not. \
         Note that when set to False, task_info functions (e.g. gokart.tree.task_info.make_task_info_as_tree_str()) cannot be used.',
        significant=False,
    )
    complete_check_at_run: bool = ExplicitBoolParameter(
        default=True, description='Check if output file exists at run. If exists, run() will be skipped.', significant=False
    )
    should_lock_run: bool = ExplicitBoolParameter(default=False, significant=False, description='Whether to use redis lock or not at task run.')

    @property
    def priority(self):
        return random.Random().random()  # seed is fixed, so we need to use random.Random().random() instead f random.random()

    def __init__(self, *args, **kwargs):
        self._add_configuration(kwargs, 'TaskOnKart')
        # 'This parameter is dumped into "workspace_directory/log/task_log/" when this task finishes with success.'
        self.task_log = dict()
        self.task_unique_id = None
        super().__init__(*args, **kwargs)
        self._rerun_state = self.rerun
        self._lock_at_dump = True

        # Cache to_str_params to avoid slow task creation in a deep task tree.
        # For example, gokart.build(RecursiveTask(dep=RecursiveTask(dep=RecursiveTask(dep=HelloWorldTask())))) results in O(n^2) calls to to_str_params.
        # However, @lru_cache cannot be used as a decorator because luigi.Task employs metaclass tricks.
        self.to_str_params = functools.lru_cache(maxsize=None)(self.to_str_params)  # type: ignore[method-assign]

        if self.complete_check_at_run:
            self.run = task_complete_check_wrapper(run_func=self.run, complete_check_func=self.complete)  # type: ignore

        if self.should_lock_run:
            self._lock_at_dump = False
            assert self.redis_host is not None, 'redis_host must be set when should_lock_run is True.'
            assert self.redis_port is not None, 'redis_port must be set when should_lock_run is True.'
            task_lock_params = make_task_lock_params_for_run(task_self=self)
            self.run = wrap_run_with_lock(run_func=self.run, task_lock_params=task_lock_params)  # type: ignore

    def input(self) -> FlattenableItems[TargetOnKart]:
        return super().input()

    def output(self) -> FlattenableItems[TargetOnKart]:
        return self.make_target()

    def requires(self) -> FlattenableItems[TaskOnKart]:
        tasks = self.make_task_instance_dictionary()
        return tasks or []  # when tasks is empty dict, then this returns empty list.

    def make_task_instance_dictionary(self) -> dict[str, TaskOnKart]:
        return {key: var for key, var in vars(self).items() if self.is_task_on_kart(var)}

    @staticmethod
    def is_task_on_kart(value):
        return isinstance(value, TaskOnKart) or (isinstance(value, tuple) and bool(value) and all([isinstance(v, TaskOnKart) for v in value]))

    @classmethod
    def _add_configuration(cls, kwargs, section):
        config = luigi.configuration.get_config()
        class_variables = dict(TaskOnKart.__dict__)
        class_variables.update(dict(cls.__dict__))
        if section not in config:
            return
        for key, value in dict(config[section]).items():
            if key not in kwargs and key in class_variables:
                kwargs[key] = class_variables[key].parse(value)

    def complete(self) -> bool:
        if self._rerun_state:
            for target in flatten(self.output()):
                target.remove()
            self._rerun_state = False
            return False

        is_completed = all([t.exists() for t in flatten(self.output())])

        if self.strict_check or self.modification_time_check:
            requirements = flatten(self.requires())
            inputs = flatten(self.input())
            is_completed = is_completed and all([task.complete() for task in requirements]) and all([i.exists() for i in inputs])

        if not self.modification_time_check or not is_completed or not self.input():
            return is_completed

        return self._check_modification_time()

    def _check_modification_time(self):
        common_path = set(t.path() for t in flatten(self.input())) & set(t.path() for t in flatten(self.output()))
        input_tasks = [t for t in flatten(self.input()) if t.path() not in common_path]
        output_tasks = [t for t in flatten(self.output()) if t.path() not in common_path]

        input_modification_time = max([target.last_modification_time() for target in input_tasks]) if input_tasks else None
        output_modification_time = min([target.last_modification_time() for target in output_tasks]) if output_tasks else None

        if input_modification_time is None or output_modification_time is None:
            return True

        # "=" must be required in the following statements, because some tasks use input targets as output targets.
        return input_modification_time <= output_modification_time

    def clone(self, cls=None, **kwargs):
        _SPECIAL_PARAMS = {'rerun', 'strict_check', 'modification_time_check'}
        if cls is None:
            cls = self.__class__

        new_k = {}
        for param_name, _ in cls.get_params():
            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name) and (param_name not in _SPECIAL_PARAMS):
                new_k[param_name] = getattr(self, param_name)

        return cls(**new_k)

    def make_target(self, relative_file_path: str | None = None, use_unique_id: bool = True, processor: FileProcessor | None = None) -> TargetOnKart:
        formatted_relative_file_path = (
            relative_file_path if relative_file_path is not None else os.path.join(self.__module__.replace('.', '/'), f'{type(self).__name__}.pkl')
        )
        file_path = os.path.join(self.workspace_directory, formatted_relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None

        task_lock_params = make_task_lock_params(
            file_path=file_path,
            unique_id=unique_id,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_timeout=self.redis_timeout,
            raise_task_lock_exception_on_collision=False,
        )

        return gokart.target.make_target(
            file_path=file_path, unique_id=unique_id, processor=processor, task_lock_params=task_lock_params, store_index_in_feather=self.store_index_in_feather
        )

    def make_large_data_frame_target(self, relative_file_path: str | None = None, use_unique_id: bool = True, max_byte=int(2**26)) -> TargetOnKart:
        formatted_relative_file_path = (
            relative_file_path if relative_file_path is not None else os.path.join(self.__module__.replace('.', '/'), f'{type(self).__name__}.zip')
        )
        file_path = os.path.join(self.workspace_directory, formatted_relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None
        task_lock_params = make_task_lock_params(
            file_path=file_path,
            unique_id=unique_id,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_timeout=self.redis_timeout,
            raise_task_lock_exception_on_collision=False,
        )

        return gokart.target.make_model_target(
            file_path=file_path,
            temporary_directory=self.local_temporary_directory,
            unique_id=unique_id,
            save_function=gokart.target.LargeDataFrameProcessor(max_byte=max_byte).save,
            load_function=gokart.target.LargeDataFrameProcessor.load,
            task_lock_params=task_lock_params,
        )

    def make_model_target(
        self, relative_file_path: str, save_function: Callable[[Any, str], None], load_function: Callable[[str], Any], use_unique_id: bool = True
    ):
        """
        Make target for models which generate multiple files in saving, e.g. gensim.Word2Vec, Tensorflow, and so on.

        :param relative_file_path: A file path to save.
        :param save_function: A function to save a model. This takes a model object and a file path.
        :param load_function: A function to load a model. This takes a file path and returns a model object.
        :param use_unique_id: If this is true, add an unique id to a file base name.
        """
        file_path = os.path.join(self.workspace_directory, relative_file_path)
        assert relative_file_path[-3:] == 'zip', f'extension must be zip, but {relative_file_path} is passed.'
        unique_id = self.make_unique_id() if use_unique_id else None
        task_lock_params = make_task_lock_params(
            file_path=file_path,
            unique_id=unique_id,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_timeout=self.redis_timeout,
            raise_task_lock_exception_on_collision=False,
        )

        return gokart.target.make_model_target(
            file_path=file_path,
            temporary_directory=self.local_temporary_directory,
            unique_id=unique_id,
            save_function=save_function,
            load_function=load_function,
            task_lock_params=task_lock_params,
        )

    @overload
    def load(self, target: None | str | TargetOnKart = None) -> Any: ...

    @overload
    def load(self, target: TaskOnKart[K]) -> K: ...

    @overload
    def load(self, target: list[TaskOnKart[K]]) -> list[K]: ...

    def load(self, target: None | str | TargetOnKart | TaskOnKart[K] | list[TaskOnKart[K]] = None) -> Any:
        def _load(targets):
            if isinstance(targets, list) or isinstance(targets, tuple):
                return [_load(t) for t in targets]
            if isinstance(targets, dict):
                return {k: _load(t) for k, t in targets.items()}
            return targets.load()

        return _load(self._get_input_targets(target))

    @overload
    def load_generator(self, target: None | str | TargetOnKart = None) -> Generator[Any, None, None]: ...

    @overload
    def load_generator(self, target: list[TaskOnKart[K]]) -> Generator[K, None, None]: ...

    def load_generator(self, target: None | str | TargetOnKart | list[TaskOnKart[K]] = None) -> Generator[Any, None, None]:
        def _load(targets):
            if isinstance(targets, list) or isinstance(targets, tuple):
                for t in targets:
                    yield from _load(t)
            elif isinstance(targets, dict):
                for k, t in targets.items():
                    yield from {k: _load(t)}
            else:
                yield targets.load()

        return _load(self._get_input_targets(target))

    @overload
    def dump(self, obj: T, target: None = None, custom_labels: dict[Any, Any] | None = None) -> None: ...

    @overload
    def dump(self, obj: Any, target: str | TargetOnKart, custom_labels: dict[Any, Any] | None = None) -> None: ...

    def dump(self, obj: Any, target: None | str | TargetOnKart = None, custom_labels: dict[str, Any] | None = None) -> None:
        PandasTypeConfigMap().check(obj, task_namespace=self.task_namespace)
        if self.fail_on_empty_dump:
            if isinstance(obj, pd.DataFrame) and obj.empty:
                raise EmptyDumpError()

        required_task_outputs = map_flattenable_items(
            lambda task: map_flattenable_items(lambda output: RequiredTaskOutput(task_name=task.get_task_family(), output_path=output.path()), task.output()),
            self.requires(),
        )

        self._get_output_target(target).dump(
            obj,
            lock_at_dump=self._lock_at_dump,
            task_params=super().to_str_params(only_significant=True, only_public=True),
            custom_labels=custom_labels,
            required_task_outputs=required_task_outputs,
        )

    @staticmethod
    def get_code(target_class) -> set[str]:
        def has_sourcecode(obj):
            return inspect.ismethod(obj) or inspect.isfunction(obj) or inspect.isframe(obj) or inspect.iscode(obj)

        return {inspect.getsource(t) for _, t in inspect.getmembers(target_class, has_sourcecode)}

    def get_own_code(self):
        gokart_codes = self.get_code(TaskOnKart)
        own_codes = self.get_code(self)
        return ''.join(sorted(list(own_codes - gokart_codes)))

    def make_unique_id(self) -> str:
        unique_id = self.task_unique_id or self._make_hash_id()
        if self.cache_unique_id:
            self.task_unique_id = unique_id
        return unique_id

    def _make_hash_id(self) -> str:
        def _to_str_params(task):
            if isinstance(task, TaskOnKart):
                return str(task.make_unique_id()) if task.significant else None

            if not isinstance(task, luigi.Task):
                raise ValueError(f'Task.requires method returns {type(task)}. You should return luigi.Task.')

            return task.to_str_params(only_significant=True)

        dependencies = [_to_str_params(task) for task in flatten(self.requires())]
        dependencies = [d for d in dependencies if d is not None]
        dependencies.append(self.to_str_params(only_significant=True))
        dependencies.append(self.__class__.__name__)
        if self.serialized_task_definition_check:
            dependencies.append(self.get_own_code())
        return hashlib.md5(str(dependencies).encode()).hexdigest()

    def _get_input_targets(self, target: None | str | TargetOnKart | TaskOnKart | list[TaskOnKart]) -> FlattenableItems[TargetOnKart]:
        if target is None:
            return self.input()
        if isinstance(target, str):
            input = self.input()
            assert isinstance(input, dict), f'input must be dict[str, TargetOnKart], but {type(input)} is passed.'
            result: FlattenableItems[TargetOnKart] = input[target]
            return result
        if isinstance(target, Iterable):
            return [self._get_input_targets(t) for t in target]
        if isinstance(target, TaskOnKart):
            requires_unique_ids = [task.make_unique_id() for task in flatten(self.requires())]
            assert target.make_unique_id() in requires_unique_ids, f'{target} should be in requires method'
            return target.output()
        return target

    def _get_output_target(self, target: None | str | TargetOnKart) -> TargetOnKart:
        if target is None:
            output = self.output()
            assert isinstance(output, TargetOnKart), f'output must be TargetOnKart, but {type(output)} is passed.'
            return output
        if isinstance(target, str):
            output = self.output()
            assert isinstance(output, dict), f'output must be dict[str, TargetOnKart], but {type(output)} is passed.'
            result = output[target]
            assert isinstance(result, TargetOnKart), f'output must be dict[str, TargetOnKart], but {type(output)} is passed.'
            return result
        return target

    def get_info(self, only_significant=False):
        params_str = {}
        params = dict(self.get_params())
        for param_name, param_value in self.param_kwargs.items():
            if (not only_significant) or params[param_name].significant:
                if isinstance(params[param_name], gokart.TaskInstanceParameter):
                    params_str[param_name] = type(param_value).__name__ + '-' + param_value.make_unique_id()
                else:
                    params_str[param_name] = params[param_name].serialize(param_value)
        return params_str

    def _get_task_log_target(self):
        return self.make_target(f'log/task_log/{type(self).__name__}.pkl')

    def get_task_log(self) -> dict:
        target = self._get_task_log_target()
        if self.task_log:
            return self.task_log
        if target.exists():
            return self.load(target)
        return dict()

    @luigi.Task.event_handler(luigi.Event.SUCCESS)
    def _dump_task_log(self):
        self.task_log['file_path'] = [target.path() for target in flatten(self.output())]
        if self.should_dump_supplementary_log_files:
            self.dump(self.task_log, self._get_task_log_target())

    def _get_task_params_target(self):
        return self.make_target(f'log/task_params/{type(self).__name__}.pkl')

    def get_task_params(self) -> dict:
        target = self._get_task_log_target()
        if target.exists():
            return self.load(target)
        return dict()

    @luigi.Task.event_handler(luigi.Event.START)
    def _set_random_seed(self):
        if self.should_dump_supplementary_log_files:
            random_seed = self._get_random_seed()
            seed_methods = self.try_set_seed(list(self.fix_random_seed_methods), random_seed)
            self.dump({'seed': random_seed, 'seed_methods': seed_methods}, self._get_random_seeds_target())

    def _get_random_seeds_target(self):
        return self.make_target(f'log/random_seed/{type(self).__name__}.pkl')

    @staticmethod
    def try_set_seed(methods: list[str], random_seed: int) -> list[str]:
        success_methods: list[str] = []
        for method_name in methods:
            try:
                for i, x in enumerate(method_name.split('.')):
                    if i == 0:
                        m = import_module(x)
                    else:
                        m = getattr(m, x)
                m(random_seed)  # type: ignore
                success_methods.append(method_name)
            except ModuleNotFoundError:
                pass
            except AttributeError:
                pass
        return success_methods

    def _get_random_seed(self):
        if self.fix_random_seed_value and (not self.fix_random_seed_value == self.FIX_RANDOM_SEED_VALUE_NONE_MAGIC_NUMBER):
            return self.fix_random_seed_value
        return int(self.make_unique_id(), 16) % (2**32 - 1)  # maximum numpy.random.seed

    @luigi.Task.event_handler(luigi.Event.START)
    def _dump_task_params(self):
        if self.should_dump_supplementary_log_files:
            self.dump(self.to_str_params(only_significant=True), self._get_task_params_target())

    def _get_processing_time_target(self):
        return self.make_target(f'log/processing_time/{type(self).__name__}.pkl')

    def get_processing_time(self) -> str:
        target = self._get_processing_time_target()
        if target.exists():
            return self.load(target)
        return 'unknown'

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def _dump_processing_time(self, processing_time):
        if self.should_dump_supplementary_log_files:
            self.dump(processing_time, self._get_processing_time_target())

    @classmethod
    def restore(cls, unique_id):
        params = TaskOnKart().make_target(f'log/task_params/{cls.__name__}_{unique_id}.pkl', use_unique_id=False).load()
        return cls.from_str_params(params)

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def _log_unique_id(self, exception):
        logger.info(f'FAILURE:\n    task name={type(self).__name__}\n    unique id={self.make_unique_id()}')

    @luigi.Task.event_handler(luigi.Event.START)
    def _dump_module_versions(self):
        if self.should_dump_supplementary_log_files:
            self.dump(self._get_module_versions(), self._get_module_versions_target())

    def _get_module_versions_target(self):
        return self.make_target(f'log/module_versions/{type(self).__name__}.txt')

    def _get_module_versions(self) -> str:
        module_versions = []
        for x in set([x.split('.')[0] for x in globals().keys() if isinstance(x, types.ModuleType) and '_' not in x]):
            module = import_module(x)
            if '__version__' in dir(module):
                if isinstance(module.__version__, str):
                    version = module.__version__.split(' ')[0]
                else:
                    version = '.'.join([str(v) for v in module.__version__])
                module_versions.append(f'{x}=={version}')
        return '\n'.join(module_versions)

    def __repr__(self):
        """
        Build a task representation like
        `MyTask[aca2f28555dadd0f1e3dee3d4b973651](param1=1.5, param2='5', data_task=DataTask(c1f5d06aa580c5761c55bd83b18b0b4e))`
        """
        return self._get_task_string()

    def __str__(self):
        """
        Build a human-readable task representation like
        `MyTask[aca2f28555dadd0f1e3dee3d4b973651](param1=1.5, param2='5', data_task=DataTask(c1f5d06aa580c5761c55bd83b18b0b4e))`
        This includes only public parameters
        """
        return self._get_task_string(only_public=True)

    def _get_task_string(self, only_public=False):
        """
        Convert a task representation like `MyTask(param1=1.5, param2='5', data_task=DataTask(id=35tyi))`
        """
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            param_obj = param_objs[param_name]
            if param_obj.significant and ((not only_public) or param_obj.visibility == ParameterVisibility.PUBLIC):
                repr_parts.append(f'{param_name}={self._make_representation(param_obj, param_value)}')

        task_str = f'{self.get_task_family()}[{self.make_unique_id()}]({", ".join(repr_parts)})'
        return task_str

    def _make_representation(self, param_obj: luigi.Parameter, param_value):
        if isinstance(param_obj, TaskInstanceParameter):
            return f'{param_value.get_task_family()}({param_value.make_unique_id()})'
        if isinstance(param_obj, ListTaskInstanceParameter):
            return f'[{", ".join(f"{v.get_task_family()}({v.make_unique_id()})" for v in param_value)}]'
        return param_obj.serialize(param_value)
