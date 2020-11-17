import os
import sys
import logging
from utils import ensure_dir_exists

LOG_FORMAT = '%(asctime)s - %(name)s: %(message)s'
DEFAULT_LOG_SUFFIX = '.log'


class Logger:
    def __init__(self, logger, name, filename, log_dir):
        self.logger = logger
        self.name = name
        self.log_dir = log_dir
        self.filename = filename

    def info(self, msg: str):
        self.logger.info(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)

    def error(self, msg: str):
        self.info("ERROR: " + msg)


class DummyLogger:
    def __init__(self, send_welcome=True):
        if send_welcome:
            self.info("Printing to Stdout")

    def info(self, msg):
        print(msg)

    def debug(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)


def create_logger(name: str,
                  level=None,
                  verbose: bool = None,
                  filename: str = None,
                  log_dir: str = ".",
                  log_to_std=sys.stderr):
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

        return Logger(logger, name, filename, log_dir)
    except Exception as e:
        print("Error: Failed to create logger", str(name))
        print(e)
        print("Returned print-only dummy logger")
        logger = DummyLogger()
        return logger
