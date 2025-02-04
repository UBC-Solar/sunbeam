from abc import ABC, abstractmethod, ABCMeta
import logging

from data_tools.schema import FileLoader, File, Result
from data_tools.utils import configure_logger
from stage.stage_registry import stage_registry
from stage.context import Context
from logs import log_directory
from functools import wraps
from collections.abc import Iterable
from typing import Any, Dict, Generator
from prefect import task


class StageError(RuntimeError):
    """
    Error raised when a Stage has been improperly constructed.
    """

    def __init__(self, stage_name: str, message: str):
        self.stage_name = stage_name
        self.message = message

    def __str__(self):
        return f"Error in {self.stage_name}: {self.message}"


def validate_type(target_type: type, obj: Any) -> bool | type:
    """
    Validate that an object is an instance of a target type.
    For iterables, recursively validate their contents.

    :param Any obj: the object whose type is to be validated
    :param type target_type: the type that ``obj`` is required to be

    :returns: True if the type is valid, or the invalid type if not
    """
    # If it's a dictionary, check both keys and values
    if isinstance(obj, dict):
        for value in obj.values():
            validate_type(value, target_type)

    # If it's an iterable but not a string or bytes, check elements recursively
    elif isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
        for item in obj:
            validate_type(item, target_type)

    # If it's not the target type and not iterable, return the Type
    elif not isinstance(obj, target_type):
        return type(obj)

    return True


def ensure_dependencies_declared(func):
    """
    This decorator should only be applied to `Stage.extract`!

    Ensure that all provided `File`s to this `Stage` belong to a listed dependency, and raises
    an `AssertionError` if an undeclared dependency is detected.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        dependencies = self.dependencies()
        stage_name = self.__class__.get_stage_name()

        for arg in args:
            if isinstance(arg, FileLoader):
                source_name = arg.canonical_path.source
                if source_name not in dependencies:
                    raise StageError(stage_name, f"{source_name} must be "
                                                 f"declared in declared dependencies "
                                                 f"of {self.get_stage_name()}!")
        return func(self, *args, **kwargs)

    return wrapper


def ensure_outputs(func):
    """
    This decorator should only be applied to `Stage.load`!

    Ensure that all declared products of the stage have been set.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)

        for stage_product in self.products:
            if stage_product not in result.keys():
                raise StageError(self.__class__.get_stage_name(), f"{self.__class__.get_stage_name()} must"
                                                                  f"produce {stage_product} but it was not found!")

        return result

    return wrapper


def check_if_skip_stage(func):
    """
    This decorator should only be applied to `Stage.run`!

    Check if the stage should be run, and if not, skips running the stage and produces empty outputs
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.get_stage_name() not in self.context.stages_to_run:
            self.logger.info(f"Skipping stage {self.get_stage_name()}...")

            # If we need to skip this stage, populate the outputs with the correct number of
            # outputs which point to errors stating that this stage didn't run
            return {
                expected_output: FileLoader(
                    lambda _: Result.Err(
                        StageError(
                            stage_name=self.get_stage_name(),
                            message="Attempting to access the output of a "
                                    "stage that has not been run!"
                        )
                    ),
                    File.make_canonical_path(
                        origin=self.context.title,
                        path=self.get_stage_name(),
                        name=expected_output)
                ) for expected_output in self.expected_outputs
            }

        # Otherwise, just run the stage as normal
        else:
            return func(self, *args, **kwargs)

    return wrapper


class StageResult:
    def __init__(self, stage, *args):
        self._expected_results = stage.expected_outputs.copy()

        if len(args) != len(self._expected_results):
            raise StageError(stage.get_stage_name(), f"Unexpected number of outputs! "
                                                     f"Found: {len(args)} outputs and "
                                                     f"expected {len(self._expected_results)}")

        if len(args) == 1 and isinstance(args[0], Dict):
            # Handle the special case where we have one output, and it is a dictionary.
            # This idiom is used by the ingest stage to avoid having to handle like 30 outputs
            # and instead returns a single dictionary output which contains a bunch of actual outputs
            # as key-value pairs.
            self._outputs: dict[str, FileLoader] = args[0]

        else:
            # Associate each output with its expected output name
            self._outputs: dict[str, FileLoader] = {output: args[i] for i, output in enumerate(self._expected_results)}

    def __getitem__(self, item):
        return self._outputs[item]

    def __iter__(self) -> FileLoader | Generator[FileLoader, Any, Any]:
        # If our output contains just one value, return just that value (not a one-tuple)
        if len(self._expected_results) == 1:
            values: FileLoader = list(self._outputs.values())[0]
            return values

        # Otherwise, unpack the result dictionary into a correctly-ordered tuple
        return iter((self._outputs[result] for result in self._expected_results))


class StageMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        # Create the instance by calling __new__ and __init__
        instance = super(StageMeta, cls).__call__(*args, **kwargs)

        # Automatically call finalize after __init__
        if hasattr(instance, "__finalize__"):
            instance.__finalize__()
        else:
            raise StageError(cls.__name__, f"Class {cls.__name__} "
                                           f"must implement a `__finalize__` method")

        return instance


class Stage(ABC, metaclass=StageMeta):
    def __new__(cls, *args, **kwargs):
        if cls.get_stage_name() not in stage_registry:
            raise StageError(cls.get_stage_name(), f"Stage {cls.get_stage_name()} declared in {__file__} has not "
                                                   f"been registered to the stage registry!")

        return super().__new__(cls)

    def __init__(self, context: Context, *args, **kwargs):
        self._finalized = False

        self._context = context
        self._logger = logging.getLogger(self.__class__.get_stage_name())
        self._expected_outputs = []

        configure_logger(self._logger, log_directory / "sunbeam.log")

    def __setattr__(self, key, value):
        if hasattr(self, "_finalized"):
            if self._finalized and key != "_items":
                raise StageError(self.__class__.get_stage_name(), f"Trying to set stage property {key} "
                                                                  f"after initialization! ")

        super().__setattr__(key, value)

    def __finalize__(self):
        self._finalized = True

    def declare_output(self, output_name: str):
        self._expected_outputs.append(output_name)

    @property
    def expected_outputs(self):
        return self._expected_outputs

    def validate_stage_outputs(self):
        """
        Validate that all the declared items of a stage have been produced.
        No effect if the stage items are valid, and raises a RuntimeError otherwise.

        :raises RuntimeError: if an item of the stage has not been produced
        """
        pass

    @check_if_skip_stage
    @abstractmethod
    def run(self, *args) -> StageResult:
        # Here, we are annotating the stage functions at runtime as a Prefect task, then calling them
        extract = task(self.extract, name=f"{self.get_stage_name()} Extract")(*args)
        transform = task(self.transform, name=f"{self.get_stage_name()} Transform")(*extract)
        load = task(self.load, name=f"{self.get_stage_name()} Load")(*transform)

        return StageResult(self, *load)

    @staticmethod
    @abstractmethod
    def dependencies():
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_stage_name(cls):
        raise NotImplementedError

    @abstractmethod
    def extract(self, *args) -> Iterable[Result]:
        raise NotImplementedError

    @abstractmethod
    def transform(self, *args) -> Iterable[Result]:
        raise NotImplementedError

    @abstractmethod
    def load(self, *args) -> Iterable[FileLoader]:
        raise NotImplementedError

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def context(self) -> Context:
        return self._context
