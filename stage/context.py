from data_tools.schema import DataSource
from typing import List


class SingletonMeta(type):
    """Metaclass to implement Singleton pattern."""
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class Context(metaclass=SingletonMeta):
    """Singleton class that holds global context information for Sunbeam."""

    def __init__(self, title: str, data_source: DataSource, stages_to_run: List[str]):
        """
        Initialize the global ``Context``.
        :param title: the title of the current Sunbeam pipeline that is running
        :param data_source: the ``DataSource`` for stages to acquire and store data
        :param stages_to_run: the list of stages that should be run (any others will be skipped)
        """
        if not hasattr(self, "_initialized"):  # Ensures __init__ runs only once
            self._title = title
            self._data_source = data_source
            self._stages_to_run = stages_to_run
            self._initialized = True
        else:
            raise RuntimeError("Context has already been initialized!")

    @property
    def title(self) -> str:
        """
        The title of the current Sunbeam pipeline that is running
        """
        return self._title

    @property
    def data_source(self) -> DataSource:
        """
        The ``DataSource`` for stages to acquire and store data
        """
        return self._data_source

    @property
    def stages_to_run(self) -> List[str]:
        """
        The list of stages that should be run (any others will be skipped)
        """
        return self._stages_to_run

    @classmethod
    def is_initialized(cls) -> bool:
        """
        Get if the global ``Context`` has been initialized.

        :return: ``True`` if it has, or ``False`` otherwise.
        """
        return cls._instance is not None

    @classmethod
    def get_instance(cls) -> "Context":
        """
        Acquire the global ``Context`` instance, if it has been initialized.

        :raises RuntimeError: If the global ``Context`` has not been initialized. Call Context() to initialize it.
        :return: a reference to the global ``Context``
        """
        if cls._instance is None:
            raise RuntimeError("Context instance has not been initialized. Call Context() first.")
        return cls._instance
