from abc import ABC, abstractmethod
from typing import Union, Tuple
import logging
from data_tools.schema import FileLoader, File
from data_pipeline.stage.stage_registry import stage_registry
from functools import wraps
from data_pipeline.overseer import Overseer

type StageResult = Union[FileLoader, Tuple[FileLoader]]


def ensure_dependencies_declared(func):
    """
    This decorator should only be applied to `Stage.extract`!

    Ensure that all provided `File`s to this `Stage` belong to a listed dependency, and raises
    an `AssertionError` if an undeclared dependency is detected.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        dependencies = self.dependencies()

        for arg in args:
            if isinstance(arg, FileLoader):
                _, stage_name, _ = File.unwrap_canonical_path(arg.canonical_path)
                assert stage_name in dependencies, (f"{stage_name} must be declared in "
                                                    f"declared dependencies of {self._stage_name}!")
        return func(self, *args, **kwargs)

    return wrapper


class Stage(ABC):
    _stage_name = ""

    def __new__(cls, *args, **kwargs):
        assert cls.get_stage_name() in stage_registry, (f"Stage {cls._stage_name} declared in {__file__} has not "
                                                        f"been registered to the stage registry!")

        return super().__new__(cls)

    def __init__(self, overseer: Overseer, logger: logging.Logger, *args, **kwargs):
        self._overseer = overseer
        self._logger = logger

    @staticmethod
    @abstractmethod
    def dependencies():
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_stage_name(cls):
        raise NotImplementedError

    @abstractmethod
    @ensure_dependencies_declared
    def extract(self, logger: logging.Logger, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def transform(self, logger: logging.Logger, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    def load(self, logger: logging.Logger, **kwargs) -> StageResult:
        raise NotImplementedError
