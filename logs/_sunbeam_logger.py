import logging
import sys

from prefect.exceptions import MissingContextError

from logs import log_directory
from logging import Logger
from logging.handlers import RotatingFileHandler
from prefect import get_run_logger
from prefect.context import get_run_context


MAX_FILE_SIZE_MB: int = 5


def is_prefect_context() -> bool:
    """
    Returns True if running inside a Prefect flow or task.
    """
    try:
        return get_run_context() is not None
    except RuntimeError:
        return False


class PrefectHandler(logging.Handler):
    """Custom logging handler that forwards logs to Prefect's logger."""

    def emit(self, record):
        try:
            prefect_logger = get_run_logger()
            log_method = getattr(prefect_logger, record.levelname.lower(), prefect_logger.info)
            log_method(self.format(record))  # Log message with Prefect
        except MissingContextError:
            pass


class SunbeamLogger(Logger):
    def __init__(self, name: str):
        """
        Configure a Logger to use stdout for INFO logs, and STDERR for error logs, and put DEBUG logs
        into a log file.
        """
        super().__init__(name)

        # If we didn't have this, the base class would also attach a handler and print every log twice!
        self.propagate = False

        std_formatter = logging.Formatter("%(name)s %(levelname)s: %(message)s")

        # log lower levels to stdout
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.addFilter(lambda rec: logging.INFO >= rec.levelno > logging.DEBUG)
        stdout_handler.setFormatter(std_formatter)

        # log higher levels to stderr (red)
        stderr_handler = logging.StreamHandler(stream=sys.stderr)
        stderr_handler.addFilter(lambda rec: rec.levelno > logging.INFO)
        stderr_handler.setFormatter(std_formatter)

        log_file_str: str = str(log_directory.absolute() / name)
        if not log_file_str.endswith(".log"):
            log_file_str += ".log"
        max_file_size = MAX_FILE_SIZE_MB * 1024 * 1024

        # Create a handler for file output
        file_handler = RotatingFileHandler(log_file_str, maxBytes=max_file_size, backupCount=3)
        file_handler.setLevel(logging.DEBUG)  # Log DEBUG and higher to the file
        file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)

        prefect_handler = PrefectHandler()
        prefect_handler.setFormatter(std_formatter)

        self.handlers.clear()

        # Add handlers to the logger
        self.addHandler(stdout_handler)
        self.addHandler(stderr_handler)
        self.addHandler(file_handler)

        # Add a handler that will forward logs to the Prefect API
        # if we can find a Prefect context to bind to.
        if is_prefect_context():
            prefect_handler = PrefectHandler()
            prefect_handler.setFormatter(std_formatter)
            self.addHandler(prefect_handler)

        self.setLevel(logging.DEBUG)
