import argparse
from typing import Optional, List

import pandas as pd
from mmproteo.utils import log
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

    default_storage_dir = "."
    default_file_name_column: str = "fileName"
    default_download_link_column: str = 'downloadLink'
    default_downloaded_files_column: str = 'downloaded_files'
    default_extracted_files_column: str = 'extracted_files'
    default_converted_mgf_files_column: str = 'converted_mgf_files'
    default_thermo_docker_container_name: str = "thermorawfileparser"
    default_thermo_docker_image: str = "quay.io/biocontainers/thermorawfileparser:1.2.3--1"
    default_thermo_start_container_command_template: str = \
        "docker run --rm -w /data -v %s:/data --name %s -d %s tail -f /dev/null"
    default_thermo_stop_container_command_template: str = "docker stop %s"
    default_thermo_output_format: str = "mgf"
    # TODO find correct exec command
    default_thermo_exec_command: str = "docker exec -it %s ThermoRawFileParser -f %d -i '%s'"
    default_option_quote: str = '"'
    default_option_separator: str = ", "
    default_skip_existing: bool = True
    default_filter_sort: bool = True
    default_filter_drop_duplicates: bool = True
    default_count_failed_files: bool = False

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

        # cache
        self.processed_files: Optional[pd.DataFrame] = None

    def parse_arguments(self) -> None:
        from mmproteo.utils import commands, formats, pride, utils
        parser = argparse.ArgumentParser(formatter_class=_MultiLineArgumentDefaultsHelpFormatter)

        parser.add_argument("command",
                            nargs='+',
                            choices=commands.get_command_names(),
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.\n" +
                                 commands.get_command_descriptions_str())
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
                            default=self.default_storage_dir,
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
        logger.assert_true(self.storage_dir is None or len(self.storage_dir) > 0, "storage-dir must not be empty")
        logger.assert_true(self.log_file is None or len(self.log_file) > 0, "log-file must not be empty")

    def check(self, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        from mmproteo.utils import utils
        utils.ensure_dir_exists(self.storage_dir, logger=logger)

