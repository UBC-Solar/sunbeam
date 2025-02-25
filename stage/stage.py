from data_tools.schema import FileLoader, Result
from stage.stage_registry import stage_registry
from abc import ABC, abstractmethod, ABCMeta
from collections.abc import Iterable
from stage.context import Context
from logs import SunbeamLogger
from typing import Callable
from prefect import task
from pathlib import Path
import toml as tomllib
import numpy as np
import logging
import json
import dill
import csv
import os


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
        """
        Initialize the Stage base class.

        Optionally, for controlling which directories are loaded as static stage data, you may set `data_pattern` to
        a Callable which receives one `str` argument (the directory name) and returns a `bool` where True/False means
        Include/Exclude.
        """
        self._finalized = False

        self._context = Context.get_instance()
        self._logger = SunbeamLogger(self.__class__.get_stage_name())

        # Acquire stage data
        self._stage_data = {}
        self._fetch_data(kwargs.get("data_pattern"))

    def _fetch_data(self, predicate: Callable[[str], bool] = None):
        # If we haven't specified a predicate, allow all.
        if predicate is None:
            predicate = lambda _: True

        stage_data_path = Path(__file__).parent / self.get_stage_name()

        def load_item(dest: dict, path: Path):
            # If the path is to another directory, we want to load the items in that directory
            if os.path.isdir(path):
                # Set up a new dictionary for the subdirectory
                directory_name = os.path.split(path)[1]  # Get the tail

                # If the predicate fails, we want to skip this directory
                if not predicate(directory_name):
                    return

                dest[directory_name] = {}

                # Load each subitem into the subdirectory
                for item in os.listdir(path):
                    load_item(dest[directory_name], path / item)

            # The path is to a file. We want to load the file's contents.
            else:
                file_name, file_extension = os.path.splitext(os.path.split(path)[1])

                match file_extension.lower():
                    case ".json":
                        with open(path, "r") as f:
                            dest[file_name] = json.load(f)

                    case ".toml":
                        with open(path, "r") as f:
                            dest[file_name] = tomllib.load(f)

                    case ".pkl" | ".pickle":
                        with open(path, "rb") as f:
                            dest[file_name] = dill.load(f)

                    case ".npy":
                        with open(path, "rb") as f:
                            dest[file_name] = np.load(f)

                    case ".csv":
                        with open(path, "r") as f:
                            reader = csv.reader(f)
                            dest[file_name] = list(reader)

        if os.path.isdir(stage_data_path):
            stage_data_items = os.listdir(stage_data_path)
            for stage_data_item in stage_data_items:
                load_item(self._stage_data, stage_data_path / stage_data_item)

        else:
            self.logger.info(f"Did not find any stage data for {self.get_stage_name()}!")

    def __setattr__(self, key, value):
        if hasattr(self, "_finalized"):
            if self._finalized and key != "_items":
                raise StageError(self.__class__.get_stage_name(), f"Trying to set stage property {key} "
                                                                  f"after initialization! ")

        super().__setattr__(key, value)

    def __finalize__(self):
        self._finalized = True

    @staticmethod
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

    @property
    def stage_data(self) -> dict:
        return self._stage_data
