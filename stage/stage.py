from abc import ABC, abstractmethod, ABCMeta
import logging

from data_tools.schema import FileLoader, Result
from stage.stage_registry import stage_registry
from stage.context import Context
from collections.abc import Iterable
from prefect import task
from logs import SunbeamLogger


class StageError(RuntimeError):
    """
    Error raised when a Stage has been improperly constructed.
    """

    def __init__(self, stage_name: str, message: str):
        self.stage_name = stage_name
        self.message = message

    def __str__(self):
        return f"Error in {self.stage_name}: {self.message}"


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

    def __init__(self, *args, **kwargs):
        self._finalized = False

        self._context = Context.get_instance()
        self._logger = SunbeamLogger(self.__class__.get_stage_name())
        self._expected_outputs = []

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

    @staticmethod
    # @check_if_skip_stage  # Make this just a part of run
    @abstractmethod
    def run(self, *args) -> tuple[FileLoader, ...]:
        # Here, we are annotating the stage functions at runtime as a Prefect task, then calling them
        extract = task(self.extract, name=f"{self.get_stage_name()} Extract")(*args)
        transform = task(self.transform, name=f"{self.get_stage_name()} Transform")(*extract)
        load = task(self.load, name=f"{self.get_stage_name()} Load")(*transform)

        return load

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
