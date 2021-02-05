import os
import sys
import logging
from typing import TextIO, Optional


LOG_FORMAT: str = '%(asctime)s - %(name)s: %(message)s'
DEFAULT_LOG_SUFFIX: str = '.log'


class Logger:
    def __init__(self,
                 logger: Optional[logging.Logger],
                 fail_early: bool = False,
                 terminate_process: bool = False):
        self.logger = logger
        self.fail_early = fail_early
        self.terminate_process = terminate_process

    def info(self, msg: str = "") -> None:
        if self.logger is not None:
            self.logger.info(msg)
        else:
            print("INFO: " + msg)

    def debug(self, msg: str = "") -> None:
        if self.logger is not None:
            self.logger.debug("DEBUG: " + msg)
        else:
            print("DEBUG: " + msg)

    def error(self, msg: str = "") -> None:
        if self.logger is not None:
            self.logger.info("ERROR: " + msg)
        else:
            print("ERROR: " + msg)
        if self.terminate_process:
            sys.exit(1)
        raise Exception(msg)

    def warning(self, msg: str = "") -> None:
        if self.logger is not None:
            self.logger.info("WARNING: " + msg)
        else:
            print("WARNING: " + msg)
        if self.fail_early:
            if self.terminate_process:
                self.info("Shutting down because of fail-early configuration")
                sys.exit(1)
            raise Exception(msg)

    def assert_true(self, condition: bool, error_msg: str) -> None:
        if not condition:
            self.error(error_msg)

    def is_verbose(self) -> bool:
        if self.logger is not None:
            return self.logger.getEffectiveLevel() == logging.DEBUG
        else:
            return True


class DummyLogger(Logger):
    def __init__(self, send_welcome: bool = True, fail_early: bool = False, terminate_process: bool = False):
        super().__init__(logger=None, fail_early=fail_early, terminate_process=terminate_process)
        if send_welcome:
            self.info("Printing to Stdout")


def create_logger(name: str,
                  level: Optional[int] = None,
                  verbose: Optional[bool] = None,
                  filename: Optional[str] = None,
                  log_dir: str = ".",
                  log_to_std: Optional[TextIO] = sys.stderr,
                  fail_early: bool = False) -> Logger:
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

        from mmproteo.utils.utils import ensure_dir_exists  # prevent circular import
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

        return Logger(logger, fail_early=fail_early)
    except Exception as e:
        logger = DummyLogger(send_welcome=True, fail_early=fail_early)
        logger.debug(str(e))
        logger.warning("Failed to create logger " + str(name))
        logger.info("Returned print-only dummy logger")

        return logger


DUMMY_LOGGER: DummyLogger = DummyLogger(send_welcome=False)
