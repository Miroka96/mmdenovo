#!/usr/bin/python3
import sys
import os
import argparse
from typing import Dict, Union, Callable, List, Optional
import pandas as pd

from mmproteo.utils import visualization, log, pride, utils, formats
from mmproteo.__init__ import __version__


class _MultiLineArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):

    def _split_lines(self, text: str, width: int) -> List[str]:
        lines = text.splitlines()
        wrapped_lines = []
        import textwrap
        for line in lines:
            command_description_parts = line.split(" : ")
            if len(command_description_parts) == 2 and command_description_parts[0].rstrip(" ").isalnum():
                description_width = width - len(command_description_parts[0]) - len(" : ")
                wrapped_description = textwrap.wrap(command_description_parts[1], description_width)
                wrapped_lines.append(command_description_parts[0] + " : " + wrapped_description[0])
                wrapped_lines += [" " * (width - description_width) + description_line
                                  for description_line in wrapped_description[1:]]
            else:
                wrapped_lines += textwrap.wrap(line, width)
        return wrapped_lines


class Config:
    _pride_project_str = "--pride-project"

    def __init__(self):
        self.pride_project: Optional[str] = None
        self.max_num_files: Optional[int] = None
        self.count_failed_files: Optional[bool] = None
        self.storage_dir: Optional[str] = None
        self.log_file: Optional[str] = None
        self.log_to_stdout: Optional[bool] = None
        self.valid_file_extensions: Optional[List[str]] = None
        self.skip_existing: Optional[bool] = None
        self.extract: Optional[bool] = None
        self.verbose: Optional[bool] = None
        self.shown_columns: Optional[List[str]] = None
        self.commands: Optional[List[str]] = None
        self.pride_versions: Optional[List[str]] = None
        self.fail_early: Optional[bool] = None

        self.processed_files: Optional[pd.DataFrame] = None
        self.file_name_column: str = "fileName"
        self.download_link_column: str = 'downloadLink'
        self.downloaded_files_column: str = 'downloaded_files'
        self.extracted_files_column: str = 'extracted_files'
        self.converted_mgf_files_column: str = 'converted_mgf_files'
        self.thermo_docker_container_name: str = "thermorawfileparser"
        self.thermo_docker_image: str = "quay.io/biocontainers/thermorawfileparser:1.2.3--1"
        self.thermo_start_container_command_template: str = \
            "docker run --rm -w /data -v %s:/data --name %s -d %s tail -f /dev/null"
        self.thermo_stop_container_command_template: str = "docker stop %s"

    def parse_arguments(self) -> None:
        parser = argparse.ArgumentParser(formatter_class=_MultiLineArgumentDefaultsHelpFormatter)

        parser.add_argument("command",
                            nargs='+',
                            choices=list(_COMMAND_DISPATCHER.keys()),
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.\n" +
                                 _get_command_descriptions_str())
        parser.add_argument("-p", Config._pride_project_str,
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
                            default=".",
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
        parser.add_argument("-f", "--no-fail-early",
                            action="store_true",
                            help="Do not fail already on warnings.")
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
        self.fail_early = (not args.no_fail_early)
        self.shown_columns = args.shown_columns
        self.pride_versions = utils.deduplicate_list(args.pride_version)

        self.commands = utils.deduplicate_list(args.command)

    def require_pride_project(self, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        logger.assert_true(self.pride_project is not None, Config._pride_project_str + " is missing")
        logger.assert_true(len(self.pride_project) > 0, Config._pride_project_str + " must not be empty")

    def validate_arguments(self, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        logger.assert_true(len(self.storage_dir) > 0, "download-dir must not be empty")
        logger.assert_true(len(self.log_file) > 0, "log-file must not be empty")

    def check(self, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        utils.ensure_dir_exists(self.storage_dir, logger=logger)


def create_logger(config: Config) -> log.Logger:
    if config.log_to_stdout:
        log_to_std = sys.stdout
    else:
        log_to_std = sys.stderr

    logger = log.create_logger(name="MMProteo",
                               log_dir=config.storage_dir,
                               filename=config.log_file,
                               verbose=config.verbose,
                               log_to_std=log_to_std,
                               fail_early=config.fail_early)
    return logger


def _run_download(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    downloaded_files = pride.download(project_name=config.pride_project,
                                      valid_file_extensions=config.valid_file_extensions,
                                      max_num_files=config.max_num_files,
                                      download_dir=config.storage_dir,
                                      skip_existing=config.skip_existing,
                                      extract=config.extract,
                                      count_failed_files=config.count_failed_files,
                                      file_name_column=config.file_name_column,
                                      download_link_column=config.download_link_column,
                                      downloaded_files_column=config.downloaded_files_column,
                                      extracted_files_column=config.extracted_files_column,
                                      api_versions=config.pride_versions,
                                      logger=logger)
    if downloaded_files is None:
        return
    if config.processed_files is None:
        config.processed_files = downloaded_files
    else:
        config.processed_files = config.processed_files.append(downloaded_files, ignore_index=True)

    visualization.print_df(df=downloaded_files,
                           max_num_files=None,
                           shown_columns=config.shown_columns,
                           urlencode_columns=[config.download_link_column],
                           logger=logger)


def _validate_download(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    config.require_pride_project(logger=logger)

    if len(config.valid_file_extensions) != 0 and config.max_num_files % len(config.valid_file_extensions) != 0:
        logger.info(
            "max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
            "files that belong together are also downloaded together")


def _run_info(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    project_info = pride.info(project_name=config.pride_project, api_versions=config.pride_versions, logger=logger)
    if project_info is None:
        return
    print(project_info)


def _validate_info(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    config.require_pride_project(logger=logger)


def _run_ls(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    project_files = pride.list_files(project_name=config.pride_project,
                                     api_versions=config.pride_versions,
                                     file_extensions=config.valid_file_extensions,
                                     logger=logger)
    # TODO cache project_files df for later downloading

    if project_files is None:
        return
    visualization.print_df(df=project_files,
                           max_num_files=config.max_num_files,
                           shown_columns=config.shown_columns,
                           urlencode_columns=[config.download_link_column],
                           logger=logger)


def _validate_ls(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    config.require_pride_project(logger=logger)


def _run_extract(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    if config.processed_files is not None and config.downloaded_files_column in config.processed_files.columns:
        files_to_extract = config.processed_files[config.downloaded_files_column]
    else:
        paths_in_storage_dir = [os.path.join(config.storage_dir, file) for file in os.listdir(config.storage_dir)]
        files_in_storage_dir = [path for path in paths_in_storage_dir if os.path.isfile(path)]
        files_to_extract = files_in_storage_dir

    files_to_extract = formats.filter_files_list(filenames=files_to_extract,
                                                 file_extensions=config.valid_file_extensions,
                                                 max_num_files=config.max_num_files,
                                                 sort=True,
                                                 drop_duplicates=True,
                                                 logger=logger)
    extracted_files = formats.extract_files(filenames=files_to_extract,
                                            skip_existing=config.skip_existing,
                                            logger=logger)
    result_df = pd.DataFrame(data=extracted_files, columns=[config.extracted_files_column])
    if config.processed_files is None:
        config.processed_files = result_df
    else:
        config.processed_files.append(result_df)


def _validate_extract(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _run_convertraw(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    formats.start_thermo_docker_container()


def _validate_convertraw(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _run_mgf2parquet(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _validate_mgf2parquet(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _run_mz2parquet(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _validate_mz2parquet(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


_COMMAND_DISPATCHER: Dict[str, Dict[str, Union[Callable[[Config, log.Logger], None], str]]] = {
    "download": {
        "handler": _run_download,
        "validator": _validate_download,
        "description": "download files from a given project"
    },
    "info": {
        "handler": _run_info,
        "validator": _validate_info,
        "description": "request project information for a given project"
    },
    "ls": {
        "handler": _run_ls,
        "validator": _validate_ls,
        "description": "list files and their attributes in a given project"
    },
    "extract": {
        "handler": _run_extract,
        "validator": _validate_extract,
        "description": "extract all downloaded archive files or, if none were downloaded, those in the data directory. "
                       "Currently, the following archive formats are supported: " +
                       formats.get_string_of_extractable_file_extensions()
    },
    "convertraw": {
        "handler": _run_convertraw,
        "validator": _validate_convertraw,
        "description": "convert all downloaded or extracted raw files or, if none were downloaded or extracted, "
                       "those raw files in the data "
                       "directory, into the given thermo output format using the ThermoRawFileParser"
    },
    "mgf2parquet": {
        "handler": _run_mgf2parquet,
        "validator": _validate_mgf2parquet,
        "description": "convert all downloaded, extracted, or converted mgf files into parquet format, or, "
                       "if no files were previously processed, convert the mgf files in the data directory"
    },
    "mz2parquet": {
        "handler": _run_mz2parquet,
        "validator": _validate_mz2parquet,
        "description": "merge and convert all downloaded or extracted mzid and mzml files into parquet format"
                       ", or, if no files were previously processed, merge and convert the files in the data directory."
    }
}


def _pad_command(command: str, width: int) -> str:
    return command.ljust(width)


def _get_command_descriptions_str() -> str:
    longest_command_length = max([len(command) for command in _COMMAND_DISPATCHER.keys()])

    return '\n'.join([_pad_command(command, longest_command_length) + " : " + config['description']
                      for command, config in _COMMAND_DISPATCHER.items()])


def _get_command_config(command: str):
    command_config = _COMMAND_DISPATCHER.get(command)
    if command_config is None:
        raise NotImplementedError("%s is no known command")
    return command_config


def _dispatch_commands(config: Config, logger: log.Logger = log.DUMMY_LOGGER):
    command_configs = [_get_command_config(command) for command in config.commands]

    try:
        for command_config in command_configs:
            command_config["validator"](config, logger)
    except Exception as e:
        logger.warning(str(e))

    for command_config in command_configs:
        command_config["handler"](config, logger)


def main(config: Config = None, logger: log.Logger = None):
    if config is None:
        config = Config()
        config.parse_arguments()
    config.validate_arguments()
    if logger is not None:
        config.check(logger=logger)
    else:
        config.check()
    if logger is None:
        logger = create_logger(config)

    if config.storage_dir is not None:
        os.chdir(config.storage_dir)
        config.storage_dir = "."

    _dispatch_commands(config=config, logger=logger)


if __name__ == '__main__':
    main()
