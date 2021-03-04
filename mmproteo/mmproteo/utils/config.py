import argparse
import re
from operator import attrgetter
from typing import Any, List, Optional, Set, Tuple, Union

import pandas as pd

import mmproteo.utils.filters
from mmproteo._version import get_versions
from mmproteo.utils import log, utils

__version__ = get_versions()['version']

del get_versions


class _MultiLineArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):

    def add_arguments(self, actions):
        actions = sorted(actions, key=attrgetter('option_strings'))
        super(_MultiLineArgumentDefaultsHelpFormatter, self).add_arguments(actions)

    def _format_usage(self, usage, actions, groups, prefix):
        actions = sorted(actions, key=attrgetter('option_strings'))
        return super(_MultiLineArgumentDefaultsHelpFormatter, self)._format_usage(usage=usage,
                                                                                  actions=actions,
                                                                                  groups=groups,
                                                                                  prefix=prefix)

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
    default_application_name = "mmproteo"

    _pride_project_parameter_str: str = "--pride-project"

    default_storage_dir: str = "."
    default_log_file: str = default_application_name + ".log"

    default_file_name_column: str = "fileName"
    default_download_link_column: str = 'downloadLink'
    default_downloaded_files_column: str = 'downloaded_files'
    default_extracted_files_column: str = 'extracted_files'
    default_converted_raw_files_column: str = 'converted_raw_files'
    default_mzmlid_parquet_files_column: str = 'converted_mzmlid_parquet_files'
    default_mgf_parquet_files_column: str = 'converted_mgf_parquet_files'

    special_column_names: Set[str] = {
        default_file_name_column,
        default_download_link_column,
        default_downloaded_files_column,
        default_extracted_files_column,
        default_converted_raw_files_column,
        default_mzmlid_parquet_files_column,
        default_mgf_parquet_files_column
    }

    default_thermo_docker_container_name: str = "thermorawfileparser"
    default_thermo_docker_image: str = "quay.io/biocontainers/thermorawfileparser:1.3.2--h1341992_1"
    default_thermo_start_container_command_template: str = \
        "docker run --rm -w /data -v {abs_storage_dir}:/data --name {container_name} -d {image_name} tail -f /dev/null"
    default_thermo_stop_container_command_template: str = "docker stop {container_name}"
    default_thermo_output_format: str = "mgf"
    default_thermo_exec_command: str = "docker exec -i {container_name} ThermoRawFileParser -f {format} " \
                                       "-i '/data/{input}' -o /data"
    default_thermo_keep_container_running: bool = False
    default_option_quote: str = '"'
    default_option_separator: str = ", "
    default_skip_existing: bool = True
    default_filter_sort: bool = True
    default_filter_drop_duplicates: bool = True
    default_filter_separator_regex: str = "[=!]="
    default_filter_or_separator: str = " or "
    default_count_failed_files: bool = False
    default_keep_null_values: bool = False
    default_pre_filter_files: bool = True
    default_mzml_key_columns: List[str] = ['mzml_filename', 'id']
    default_mzid_key_columns: List[str] = ['name', 'spectrumID']
    default_mzmlid_parquet_file_postfix: str = "_mzmlid.parquet"

    def __init__(self):
        from mmproteo.utils import formats

        self.pride_project: Optional[str] = None
        self.max_num_files: int = 0
        self.count_failed_files: bool = self.default_count_failed_files
        self.storage_dir: str = self.default_storage_dir
        self.log_file: str = self.default_log_file
        self.log_to_stdout: bool = False
        self.valid_file_extensions: List[str] = []
        self.skip_existing: bool = self.default_skip_existing
        self.verbose: bool = False
        self.dummy_logger: bool = False
        self.shown_columns: List[str] = []
        self.commands: Optional[List[str]] = None
        self.pride_versions: List[str] = []
        self.column_filter: Optional[mmproteo.utils.filters.AbstractFilterConditionNode] = None
        self.fail_early: bool = True
        self.terminate_process: bool = False
        self.thermo_output_format: str = self.default_thermo_output_format
        self.thermo_keep_container_running: bool = self.default_thermo_keep_container_running

        # cache
        self.processed_files: Optional[pd.DataFrame] = None
        self.project_files: Optional[pd.DataFrame] = None

    @staticmethod
    def get_string_of_special_column_names(extension_quote: str = default_option_quote,
                                           separator: str = default_option_separator) -> str:
        return utils.concat_set_of_options(options=Config.special_column_names,
                                           option_quote=extension_quote,
                                           separator=separator)

    def clear_cache(self):
        self.processed_files = None
        self.project_files = None

    def cache(self,
              data_list: Optional[List[Any]] = None,
              column_names: Optional[Union[str, List[str]]] = None,
              data_df: pd.DataFrame = None) -> Optional[pd.DataFrame]:
        assert (data_list is not None and column_names is not None and data_df is None) \
               or (data_list is None and column_names is None), "data_list and column_names must always be given " \
                                                                "together, but never at the same time as data_df"

        if data_df is None:
            if len(data_list) == 0:
                return None

            if type(column_names) == str:
                column_names = [column_names]

            data_df = pd.DataFrame(data=data_list, columns=column_names)

        if self.processed_files is None:
            self.processed_files = data_df
        else:
            self.processed_files = self.processed_files.append(data_df, ignore_index=True)
        return data_df

    def parse_arguments(self) -> None:
        from mmproteo.utils import commands, pride, utils, formats
        parser = argparse.ArgumentParser(formatter_class=_MultiLineArgumentDefaultsHelpFormatter, add_help=False)

        parser.add_argument("command",
                            nargs='+',
                            choices=commands.DISPATCHER.get_command_names(),
                            metavar="COMMAND",
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.\n \n" +
                                 commands.DISPATCHER.get_command_descriptions_str())
        parser.add_argument(self._pride_project_parameter_str, "-p",
                            metavar="PROJECT",
                            help="the name of the PRIDE project, e.g. 'PXD010000' " +
                                 "from 'https://www.ebi.ac.uk/pride/ws/archive/peptide/list/project/PXD010000'. " +
                                 "For some commands, this parameter is required.")
        parser.add_argument("--max-num-files", "-n",
                            metavar="N",
                            default=self.max_num_files,
                            type=int,
                            help="the maximum number of files to be downloaded. Set it to '0' to download all files.")
        parser.add_argument("--count-failed-files",
                            action="store_" + str(self.count_failed_files).lower(),
                            help="Count failed files and do not just skip them. " +
                                 "This is relevant for the max-num-files parameter.")
        parser.add_argument("--storage-dir", "-d",
                            metavar="DIR",
                            default=self.storage_dir,
                            help="the name of the directory, in which the downloaded files and the log file will be "
                                 "stored.")
        parser.add_argument("--log-file", "-l",
                            metavar="FILE",
                            default=self.log_file,
                            help="the name of the log file, relative to the download directory. "
                                 "Set it to an empty string (\"\") to disable file logging.")
        parser.add_argument("--log-to-stdout",
                            action="store_" + str(not self.log_to_stdout).lower(),
                            help="Log to stdout instead of stderr.")
        parser.add_argument("--shown-columns", "-c",
                            metavar="COLUMNS",
                            default="",
                            type=lambda s: [col for col in s.split(",") if len(col) > 0],
                            help="a list of comma-separated column names. Some commands show their results as tables, "
                                 "so their output columns will be limited to those in this list. An empty list "
                                 "deactivates filtering. Capitalization matters.")
        parser.add_argument("--file-extensions", "-e",
                            metavar="EXT",
                            default="",
                            type=lambda s: {ext.lower() for ext in s.split(',') if len(ext) > 0},
                            help="a list of comma-separated allowed file extensions to filter files for. "
                                 "Archive extensions will be automatically appended. " +
                                 "An empty list deactivates filtering. "
                                 "Capitalization does not matter.")
        parser.add_argument("--no-skip-existing",
                            action="store_" + str(self.skip_existing).lower(),
                            help="Do not skip existing files.")
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("--verbose", "-v",
                            action="store_" + str(not self.verbose).lower(),
                            help="Increase output verbosity to debug level.")
        parser.add_argument("--no-fail-early",
                            action="store_" + str(self.fail_early).lower(),
                            help="Do not fail commands already on failed assertions. The code will run until "
                                 "a real exception is encountered or it even succeeds.")
        parser.add_argument('--help', "-h",
                            action='help',
                            default=argparse.SUPPRESS,
                            help='Show this help message and exit.')
        parser.add_argument('--version',
                            action='version',
                            version='%(prog)s ' + __version__,
                            help="Show the version of this software.")
        parser.add_argument('--pride-version', '-i',
                            choices=pride.get_pride_api_versions(),
                            action="append",
                            default=self.pride_versions,
                            help="an API version for the PRIDE interactions. Only the specified versions will be used. "
                                 "This parameter can be given multiple times to allow multiple different "
                                 "API versions, one version per parameter appearance. "
                                 "The order of occurring API versions will be considered until the first API request "
                                 "fulfills its job. Every version should appear at most once. "
                                 "Duplicates are dropped after the first occurrence. "
                                 "An empty list (default) uses all api versions in the following order: [%s]" %
                                 pride.get_string_of_pride_api_versions())
        parser.add_argument("--dummy-logger",
                            action="store_" + str(not self.dummy_logger).lower(),
                            help="Use a simpler log format and log to stdout.")
        parser.add_argument("--thermo-output-format",
                            default=self.thermo_output_format,
                            choices=formats.get_thermo_raw_file_parser_output_formats(),
                            help="the output format into which the raw file will be converted. This parameter only "
                                 f"applies to the {commands.ConvertRawCommand().get_command()} command.")
        parser.add_argument("--thermo-keep-running",
                            action="store_" + str(not self.thermo_keep_container_running).lower(),
                            help="Keep the ThermoRawFileParser Docker container running after conversion. This can "
                                 "speed up batch processing and ease debugging.")
        parser.add_argument('--filter', '-f',
                            metavar=f"COLUMN{self.default_filter_separator_regex}REGEX",
                            action="append",
                            type=mmproteo.utils.filters.create_or_filter_from_str,
                            default=[],
                            help="a filter condition for file filtering. The condition must be of the form "
                                 f"'columnName{self.default_filter_separator_regex}valueRegex'. Therefore, the "
                                 "comparison operator can either be '==' or '!='. "
                                 "The column name must not contain these character patterns. "
                                 "The value will be interpreted using Python's rules for regular expressions (from the "
                                 "Python 're' package). This parameter can be given multiple times to enforce multiple "
                                 "filters simultaneously, meaning the filters will be logically connected using a "
                                 "boolean 'and'. Boolean 'or' operations can be specified as "
                                 f"'{self.default_filter_or_separator}' within any filter parameter, "
                                 "for example like this (representing (a==1 or b==2) and (c==3 or (not d==4))): "
                                 f"'{self.default_application_name} -f \"a==1{self.default_filter_or_separator}b==2\" "
                                 f"-f \"c==3{self.default_filter_or_separator}d!=4\" list'. "
                                 "A condition can be negated using '!=' instead of '=='. For the filtering process, "
                                 "the filter columns will be converted to strings. Non-existent columns will be "
                                 "ignored. Capitalization matters. All these rules add up to a "
                                 "conjunctive normal form. As some commands can be pipelined to use previous results, "
                                 "there are also the following special column names available: " +
                                 f"[{self.get_string_of_special_column_names()}]. An empty list disables this filter.")

        args = parser.parse_args()

        self.pride_project = args.pride_project
        self.max_num_files = args.max_num_files
        self.count_failed_files = args.count_failed_files
        self.storage_dir = args.storage_dir
        self.log_file = args.log_file
        self.log_to_stdout = args.log_to_stdout
        self.valid_file_extensions = args.file_extensions
        self.skip_existing = (not args.no_skip_existing)
        self.verbose = args.verbose
        self.dummy_logger = args.dummy_logger
        self.fail_early = (not args.no_fail_early)
        self.shown_columns = args.shown_columns
        self.pride_versions = utils.deduplicate_list(args.pride_version)
        self.column_filter = mmproteo.utils.filters.NoneFilterConditionNode(
            condition=mmproteo.utils.filters.AndFilterConditionNode(conditions=args.filter),
            none_value=True,
        )
        self.thermo_output_format = args.thermo_output_format
        self.thermo_keep_container_running = args.thermo_keep_running

        self.commands = utils.deduplicate_list(args.command)

    def require_pride_project(self, logger: log.Logger = log.DEFAULT_LOGGER) -> None:
        logger.assert_true(self.pride_project is not None, Config._pride_project_parameter_str + " is missing")
        logger.assert_true(len(self.pride_project) > 0, Config._pride_project_parameter_str + " must not be empty")

    def validate_arguments(self, logger: log.Logger = log.DEFAULT_LOGGER) -> None:
        logger.assert_true(self.storage_dir is None or len(self.storage_dir) > 0, "storage-dir must not be empty")
        logger.assert_true(self.max_num_files >= 0, "max-num-files must be >= 0; use 0 to process all files")

    def check(self, logger: log.Logger = log.DEFAULT_LOGGER) -> None:
        from mmproteo.utils import utils
        utils.ensure_dir_exists(self.storage_dir, logger=logger)
