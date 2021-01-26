import os
import sys
import logging
from typing import TextIO, Optional

from mmproteo.utils.utils import ensure_dir_exists

LOG_FORMAT: str = '%(asctime)s - %(name)s: %(message)s'
DEFAULT_LOG_SUFFIX: str = '.log'


class Logger:
    def __init__(self, logger):
        self.logger = logger

    def info(self, msg: str = "") -> None:
        self.logger.info(msg)

    def debug(self, msg: str = "") -> None:
        self.logger.debug("DEBUG: " + msg)

    def error(self, msg: str = "") -> None:
        self.info("ERROR: " + msg)

    def warning(self, msg: str = "") -> None:
        self.info("WARNING: " + msg)


class DummyLogger(Logger):
    def __init__(self, send_welcome=True):
        super().__init__(None)
        if send_welcome:
            self.info("Printing to Stdout")

    def info(self, msg: str = "") -> None:
        print("INFO: " + msg)

    def debug(self, msg: str = "") -> None:
        print("DEBUG: " + msg)

    def error(self, msg: str = "") -> None:
        print("ERROR: " + msg)

    def warning(self, msg: str = "") -> None:
        print("WARNING: " + msg)


def create_logger(name: str,
                  level: Optional[int] = None,
                  verbose: Optional[bool] = None,
                  filename: Optional[str] = None,
                  log_dir: str = ".",
                  log_to_std: Optional[TextIO] = sys.stderr) -> Logger:
    if verbose is not None:
        assert level is None, "level and verbose are exclusive parameters"
        if verbose:
            level = logging.DEBUG
        else:
            level = logging.INFO
    else:
        assert level is not None, "either level or verbose must be given as parameter"
    try:
        assert name is not None
        assert type(name) == str
        if filename is None:
            filename = name + DEFAULT_LOG_SUFFIX
        if log_dir is not None:
            filename = os.path.join(log_dir, filename)
        logger = logging.getLogger(name)
        logger.setLevel(level)

        formatter = logging.Formatter(LOG_FORMAT)

        # log to file
        dir_name = os.path.dirname(filename)
        ensure_dir_exists(dir_name)
        fh = logging.FileHandler(filename)
        fh.setFormatter(formatter)
        fh.setLevel(level)
        logger.addHandler(fh)

        if log_to_std is not None:
            oh = logging.StreamHandler(log_to_std)
            oh.flush = sys.stdout.flush
            oh.setFormatter(formatter)
            oh.setLevel(level)
            logger.addHandler(oh)

            logger.info("Logging to %s and to file %s" % (log_to_std.name, filename))
        else:
            logger.info("Logging to file %s" % filename)

        return Logger(logger)
    except Exception as e:
        logger = DummyLogger()
        logger.error("Failed to create logger " + str(name))
        logger.debug(str(e))
        logger.info("Returned print-only dummy logger")

        return logger


DUMMY_LOGGER: DummyLogger = DummyLogger(send_welcome=False)
