#!/usr/bin/python3
import sys
import os
import argparse
from .utils import visualization, log, pride, utils, formats
from .__init__ import __version__

logger = log.DummyLogger(send_welcome=False)


class Config:
    pride_project_str = "--pride-project"

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
        self.pride_versions = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("command",
                            nargs='+',
                            choices=list(COMMAND_DISPATCHER.keys()),
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.")
        parser.add_argument("-p", Config.pride_project_str,
                            help="the name of the PRIDE project, e.g. 'PXD010000' " +
                                 "from 'https://www.ebi.ac.uk/pride/ws/archive/peptide/list/project/PXD010000'. " +
                                 "For some commands, this parameter is required.")
        parser.add_argument("-n", "--max-num-files",
                            default=0,
                            type=int,
                            help="the maximum number of files to be downloaded. Set it to '0' to download all files.")
        parser.add_argument("--count-failed-files",
                            action="store_false",
                            help="Count failed files and do not just skip them. " +
                                 "This is relevant for the max-num-files parameter.")
        parser.add_argument("-d", "--storage-dir",
                            default=os.path.join(".", "pride"),
                            help="the name of the directory, in which the downloaded files and the log file will be "
                                 "stored.")
        parser.add_argument("-l", "--log-file",
                            default="downloader.log",
                            help="the name of the log file, relative to the download directory.")
        parser.add_argument("--log-to-stdout",
                            action="store_true",
                            help="Log to stdout instead of stderr.")
        parser.add_argument("--shown-columns",
                            default="",
                            type=lambda s: [col for col in s.split(",") if len(col) > 0],
                            help="a list of comma-separated column names. Some commands show their results as tables, "
                                 "so their output columns will be limited to those in this list. An empty list "
                                 "deactivates filtering. Capitalization matters.")
        parser.add_argument("-t", "--valid-file-extensions",
                            default="",
                            type=lambda s: {ext.lower() for ext in s.split(',') if len(ext) > 0},
                            help="a list of comma-separated allowed file extensions to filter files for. " +
                                 "An empty list deactivates filtering. "
                                 "Capitalization does not matter.")
        parser.add_argument("-e", "--no-skip-existing",
                            action="store_true",
                            help="Do not skip existing files.")
        parser.add_argument("-x", "--no-extract",
                            action="store_true",
                            help="Do not try to extract downloaded files. Supported formats: [%s]" %
                                 formats.get_string_of_extractable_file_extensions())
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("-v", "--verbose",
                            action="store_true",
                            help="Increase output verbosity to debug level.")
        parser.add_argument('--version',
                            action='version',
                            version='%(prog)s ' + __version__,
                            help="show the version of this software")
        parser.add_argument('-i', '--pride-version',
                            choices=list(pride.PRIDE_APIS.keys()),
                            action="append",
                            default=[],
                            help="an API version for the PRIDE interactions. Only the specified versions will be used. "
                                 "This parameter can be given multiple times to allow multiple different "
                                 "API versions, one version per parameter appearance. "
                                 "The order of occurring API versions will be considered until the first API request "
                                 "fulfills its job. Every version should appear at most once. "
                                 "Duplicates are dropped after the first occurrence. "
                                 "An empty list (default) uses all api versions in the following order: [%s]" %
                                 pride.get_string_of_pride_api_versions())

        args = parser.parse_args()

        self.pride_project = args.pride_project
        self.max_num_files = args.max_num_files
        self.count_failed_files = args.count_failed_files
        self.storage_dir = args.storage_dir
        self.log_file = args.log_file
        self.log_to_stdout = args.log_to_stdout
        self.valid_file_extensions = args.valid_file_extensions
        self.skip_existing = (not args.no_skip_existing)
        self.extract = (not args.no_extract)
        self.verbose = args.verbose
        self.shown_columns = args.shown_columns
        self.pride_versions = utils.deduplicate_list(args.pride_version)

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

    logger = log.create_logger(name="MMProteo",
                               log_dir=config.storage_dir,
                               filename=config.log_file,
                               verbose=config.verbose,
                               log_to_std=log_to_std)


def run_download(config: Config):
    downloaded_files = pride.download(project_name=config.pride_project,
                                      api_versions=config.pride_versions,
                                      logger=logger,
                                      valid_file_extensions=config.valid_file_extensions,
                                      max_num_files=config.max_num_files,
                                      download_dir=config.storage_dir,
                                      skip_existing=config.skip_existing,
                                      extract=config.extract,
                                      count_failed_files=config.count_failed_files)
    if downloaded_files is None:
        return
    visualization.print_df(df=downloaded_files,
                           max_num_files=None,
                           shown_columns=config.shown_columns,
                           urlencode_columns=['downloadLink'],
                           logger=logger)


def validate_download(config: Config):
    config.require_pride_project()

    if len(config.valid_file_extensions) != 0 and config.max_num_files % len(config.valid_file_extensions) != 0:
        logger.warning(
            "max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
            "files that belong together are also downloaded together")


def run_info(config: Config):
    project_info = pride.info(project_name=config.pride_project, api_versions=config.pride_versions, logger=logger)
    if project_info is None:
        return
    print(project_info)


def validate_info(config: Config):
    config.require_pride_project()


def run_ls(config: Config):
    project_files = pride.list_files(project_name=config.pride_project,
                                     api_versions=config.pride_versions,
                                     file_extensions=config.valid_file_extensions,
                                     logger=logger)
    if project_files is None:
        return
    visualization.print_df(df=project_files,
                           max_num_files=config.max_num_files,
                           shown_columns=config.shown_columns,
                           urlencode_columns=['downloadLink'],
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
