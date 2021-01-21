#!/usr/bin/python3
import sys
import os
import argparse
from .utils import visualization, log, pride, utils

logger = log.DummyLogger(send_welcome=False)


class Config:
    command_str = "command"
    pride_project_str = "--pride-project"
    max_num_files_str = "--max-num-files"
    count_failed_files_str = "--count-failed-files"
    storage_dir_str = "--storage-dir"
    log_file_str = "--log-file"
    log_to_stdout_str = "--log-to-stdout"
    valid_file_extensions_str = "--valid-file-extensions"
    no_skip_existing_str = "--no-skip-existing"
    no_extract_str = "--no-extract"
    verbose_str = "--verbose"
    shown_columns_str = "--shown-columns"

    def __init__(self):
        self.pride_project = None
        self.max_num_files = None
        self.count_failed_files = None
        self.storage_dir = None
        self.log_file = None
        self.log_to_stdout = None
        self.valid_file_extensions = None
        self.skip_existing = None
        self.extract = None
        self.verbose = None
        self.shown_columns = None
        self.commands = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument(Config.command_str,
                            nargs='+',
                            choices=list(COMMAND_DISPATCHER.keys()),
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.")
        parser.add_argument("-p", Config.pride_project_str,
                            help="the name of the PRIDE project, e.g. 'PXD010000' " +
                                 "from 'https://www.ebi.ac.uk/pride/ws/archive/peptide/list/project/PXD010000'." +
                                 "For some commands, this parameter is required.")
        parser.add_argument("-n", Config.max_num_files_str,
                            default=0,
                            type=int,
                            help="the maximum number of files to be downloaded. Set it to '0' to download all files.")
        parser.add_argument(Config.count_failed_files_str,
                            action="store_false",
                            help="Count failed files and do not just skip them. " +
                                 "This is relevant for the max-num-files parameter.")
        parser.add_argument("-d", Config.storage_dir_str,
                            default=os.path.join(".", "pride"),
                            help="the name of the directory, in which the downloaded files and the log file will be "
                                 "stored.")
        parser.add_argument("-l", Config.log_file_str,
                            default="downloader.log",
                            help="the name of the log file, relative to the download directory.")
        parser.add_argument(Config.log_to_stdout_str,
                            action="store_true",
                            help="Log to stdout instead of stderr.")
        parser.add_argument("-t", Config.valid_file_extensions_str,
                            default="",
                            help="a list of comma-separated allowed file extensions to filter files for. " +
                                 "An empty list deactivates filtering. "
                                 "Capitalization does not matter.")
        parser.add_argument("-e", Config.no_skip_existing_str,
                            action="store_true",
                            help="Do not skip existing files.")
        parser.add_argument("-x", Config.no_extract_str,
                            action="store_true",
                            help="Do not try to extract downloaded files.")
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("-v", Config.verbose_str,
                            action="store_true",
                            help="Increase output verbosity to debug level.")
        parser.add_argument(Config.shown_columns_str,
                            default="",
                            help="a list of comma-separated column names. Some commands show their results as tables, "
                                 "so their output columns will be limited to those in this list. An empty list "
                                 "deactivates filtering. Capitalization matters.")

        args = parser.parse_args()

        self.pride_project = args.pride_project
        self.max_num_files = args.max_num_files
        self.count_failed_files = args.count_failed_files
        self.storage_dir = args.storage_dir
        self.log_file = args.log_file
        self.log_to_stdout = args.log_to_stdout
        self.valid_file_extensions = {ext.lower() for ext in args.valid_file_extensions.split(",") if len(ext) > 0}
        self.skip_existing = (not args.no_skip_existing)
        self.extract = (not args.no_extract)
        self.verbose = args.verbose
        self.shown_columns = [col for col in args.shown_columns.split(",") if len(col) > 0]

        self.commands = utils.deduplicate_list(args.command)

    def require_pride_project(self):
        assert self.pride_project is not None, Config.pride_project_str + " is missing"
        assert len(self.pride_project) > 0, Config.pride_project_str + " must not be empty"

    def validate_arguments(self):
        assert len(self.storage_dir) > 0, "download-dir must not be empty"
        assert len(self.log_file) > 0, "log-file must not be empty"

    def check(self):
        utils.ensure_dir_exists(self.storage_dir)


def set_logger(config: Config):
    global logger
    if config.log_to_stdout:
        log_to_std = sys.stdout
    else:
        log_to_std = sys.stderr

    logger = log.create_logger(name="PRIDE_Downloader",
                               log_dir=config.storage_dir,
                               filename=config.log_file,
                               verbose=config.verbose,
                               log_to_std=log_to_std)


def run_download(config: Config):
    downloaded_files = pride.download(project_name=config.pride_project,
                                      logger=logger,
                                      valid_file_extensions=config.valid_file_extensions,
                                      max_num_files=config.max_num_files,
                                      download_dir=config.storage_dir,
                                      skip_existing=config.skip_existing,
                                      extract=config.extract,
                                      count_failed_files=config.count_failed_files)
    visualization.print_df(df=downloaded_files,
                           max_num_files=None,
                           shown_columns=config.shown_columns,
                           logger=logger)


def validate_download(config: Config):
    config.require_pride_project()

    if len(config.valid_file_extensions) != 0 and config.max_num_files % len(config.valid_file_extensions) != 0:
        logger.info(
            "Warning: max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
            "files that belong together are also downloaded together")


def run_info(config: Config):
    project_info = pride.info(pride_project=config.pride_project, logger=logger)
    print(project_info)


def validate_info(config: Config):
    config.require_pride_project()


def run_ls(config: Config):
    project_files = pride.list_files(project_name=config.pride_project,
                                     file_extensions=config.valid_file_extensions,
                                     logger=logger)
    visualization.print_df(df=project_files,
                           max_num_files=config.max_num_files,
                           shown_columns=config.shown_columns,
                           logger=logger)


def validate_ls(config: Config):
    config.require_pride_project()


COMMAND_DISPATCHER = {
    "download": {
        "handler": run_download,
        "validator": validate_info,
    },
    "info": {
        "handler": run_info,
        "validator": validate_info,
    },
    "ls": {
        "handler": run_ls,
        "validator": validate_info,
    }
}


def get_command_config(command: str):
    command_config = COMMAND_DISPATCHER.get(command)
    if command_config is None:
        raise NotImplementedError("%s is no known command")
    return command_config


def dispatch_commands(config: Config):
    command_configs = [get_command_config(command) for command in config.commands]

    try:
        for command_config in command_configs:
            command_config["validator"](config)
    except Exception as e:
        logger.error(str(e))
        return

    for command_config in command_configs:
        command_config["handler"](config)


def main(config: Config = None):
    if config is None:
        config = Config()
        config.parse_arguments()
    config.validate_arguments()
    config.check()
    set_logger(config)

    dispatch_commands(config)


if __name__ == '__main__':
    main()
