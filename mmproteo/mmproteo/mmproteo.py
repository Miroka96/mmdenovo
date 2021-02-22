#!/usr/bin/python3
import sys
import os

from mmproteo.utils import log, commands
from mmproteo.utils.config import Config

APPLICATION_NAME: str = "MMProteo"
_HERE: str = "."


def create_logger(config: Config) -> log.Logger:
    if config.log_to_stdout:
        log_to_std = sys.stdout
    else:
        log_to_std = sys.stderr

    logger = log.create_logger(name=APPLICATION_NAME,
                               log_dir=config.storage_dir,
                               filename=config.log_file,
                               verbose=config.verbose,
                               log_to_std=log_to_std,
                               fail_early=config.fail_early)
    return logger


def main(config: Config = None, logger: log.Logger = None):
    if config is None:
        config = Config()
        config.parse_arguments()

    config.validate_arguments()

    if logger is not None:
        config.check(logger=logger)
    else:
        config.check()
        logger = create_logger(config)

    if config.storage_dir is not None:
        os.chdir(config.storage_dir)

    config.storage_dir = _HERE

    commands.DISPATCHER.dispatch_commands(config=config, logger=logger)


if __name__ == '__main__':
    main()
