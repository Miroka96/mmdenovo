import time
from typing import Optional, Set, Callable, Union, Any, Dict, List

from pyteomics import mgf, mzid, mzml
import pandas as pd
import os

from pyteomics.mgf import MGFBase
from pyteomics.mzid import MzIdentML
from pyteomics.mzml import MzML

from mmproteo.utils import log, utils, visualization
from mmproteo.utils.config import Config


def iter_entries(iterator: Union[MGFBase, MzIdentML, MzML], logger: log.Logger = log.DUMMY_LOGGER) \
        -> List[Dict[str, Any]]:
    logger.debug("Iterator type = " + str(type(iterator)))
    entries = list(iterator)
    logger.debug("Length: %d" % len(entries))
    if len(entries) > 0:
        logger.debug("Example:\n" + visualization.pretty_print_json(entries[0]))
    return entries


def read_mgf(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mgf.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_mzid(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mzid.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_mzml(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mzml.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_parquet(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    return pd.read_parquet(filename)


_FILE_READING_CONFIG: Dict[str, Callable[[str, log.Logger], pd.DataFrame]] = {
    "mgf": read_mgf,
    "mzid": read_mzid,
    "mzml": read_mzml,
    "parquet": read_parquet,
}


def get_readable_file_extensions() -> Set[str]:
    return set(_FILE_READING_CONFIG.keys())


def read(filename: str, filename_col: Optional[str] = "%s_filename", logger: log.Logger = log.DUMMY_LOGGER) \
        -> pd.DataFrame:
    _, ext = separate_extension(filename=filename, extensions=get_readable_file_extensions())

    if len(ext) > 0:
        logger.debug("Started reading %s file '%s'" % (ext, filename))
        df = _FILE_READING_CONFIG[ext](filename, logger)
        logger.info("Finished reading %s file '%s'" % (ext, filename))
    else:
        raise NotImplementedError
    if filename_col is not None:
        if "%s" in filename_col:
            col = filename_col % ext
        else:
            col = filename_col
        if col in df.columns:
            logger.warning("'%s' already exists in DataFrame columns. Please choose a different column name.")
        else:
            df[col] = filename.split(os.sep)[-1]
    return df


_FILE_EXTRACTION_CONFIG: Dict[str, Dict[str, str]] = {
    "gz": {
        "command": 'gunzip "%s"'
    },
    "zip": {
        "command": 'unzip "%s"'
    },
}


def get_extractable_file_extensions() -> Set[str]:
    return set(_FILE_EXTRACTION_CONFIG.keys())


def get_string_of_extractable_file_extensions(extension_quote: str = Config.default_option_quote,
                                              separator: str = Config.default_option_separator) -> str:
    return utils.concat_set_of_options(options=get_readable_file_extensions(),
                                       option_quote=extension_quote,
                                       separator=separator)


def separate_extension(filename: str, extensions: Set[str]) -> (str, str):
    lower_filename = filename.lower()
    longest_extension = ""
    for ext in extensions:
        if lower_filename.endswith("." + ext) and len(longest_extension) < len(ext):
            longest_extension = ext

    real_filename = filename[:len(filename) - len(longest_extension) - len(".")]
    return real_filename, longest_extension


def extract_file_if_possible(filename: Optional[str],
                             skip_existing: bool = Config.default_skip_existing,
                             logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    extracted_filename, file_ext = separate_extension(filename.lower(), get_extractable_file_extensions())
    if len(file_ext) == 0:
        logger.debug("Cannot extract file '%s', unknown extension" % extracted_filename)
        return filename

    extraction_command = _FILE_EXTRACTION_CONFIG[file_ext]["command"] % filename

    if skip_existing and os.path.isfile(extracted_filename):
        logger.info('Skipping extraction, because "%s" already exists' % extracted_filename)
        return extracted_filename

    logger.info("Extracting file using '%s'" % extraction_command)
    return_code = os.system(extraction_command)
    if return_code == 0:
        logger.info("Extracted file")
    else:
        logger.warning('Failed extracting file "%s" (return code = %d)' % (filename, return_code))

    return extracted_filename


def create_file_extension_filter(required_file_extensions: Set[str],
                                 optional_file_extensions: Optional[Set[str]] = None) -> Callable[[str], bool]:
    if optional_file_extensions is None:
        file_extensions = set(required_file_extensions)
    else:
        file_extensions = {required_extension + "." + optional_extension
                           for required_extension in required_file_extensions
                           for optional_extension in optional_file_extensions}
        file_extensions.update(required_file_extensions)  # add all

    def filter_file_extension(filename: str) -> bool:
        return filename.lower().endswith(tuple(file_extensions))

    return filter_file_extension


def extract_files(filenames: List[Optional[str]],
                  skip_existing: bool = Config.default_skip_existing,
                  logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [extract_file_if_possible(filename, skip_existing=skip_existing, logger=logger)
            for filename in filenames]


def filter_files_df(files_df: Optional[pd.DataFrame],
                    file_name_column: str = "fileName",
                    file_extensions: Optional[Set[str]] = None,
                    max_num_files: Optional[int] = None,
                    sort: bool = Config.default_filter_sort,
                    logger: log.Logger = log.DUMMY_LOGGER) -> Optional[pd.DataFrame]:
    if files_df is None:
        return None

    if file_extensions is None or len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = files_df
    else:
        logger.assert_true(file_name_column in files_df.columns, "Could not find '%s' column in files_df columns" %
                           file_name_column)
        required_file_extensions = file_extensions
        optional_file_extensions = get_extractable_file_extensions()

        required_file_extensions_list_str = "\", \"".join(required_file_extensions)
        optional_file_extensions_list_str = "\", \"".join(optional_file_extensions)
        if len(optional_file_extensions_list_str) > 0:
            optional_file_extensions_list_str = '"' + optional_file_extensions_list_str + '"'
        logger.info("Filtering files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (required_file_extensions_list_str,
                                                                 optional_file_extensions_list_str))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = files_df[files_df[file_name_column].apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    if sort:
        # sort, such that files with same prefixes but different extensions come in pairs
        sorted_files = filtered_files.sort_values(by=file_name_column)
    else:
        sorted_files = filtered_files

    if max_num_files is None or max_num_files == 0:
        limited_files = sorted_files
    else:
        limited_files = sorted_files[:max_num_files]

    return limited_files


def filter_files_list(filenames: List[Optional[str]],
                      file_extensions: Optional[Set[str]] = None,
                      max_num_files: Optional[int] = None,
                      sort: bool = Config.default_filter_sort,
                      drop_duplicates: bool = Config.default_filter_drop_duplicates,
                      logger: log.Logger = log.DUMMY_LOGGER) -> List[str]:
    filenames = [filename for filename in filenames if filename is not None]
    df = pd.DataFrame(data=filenames, columns=["fileName"])
    if drop_duplicates:
        df = df.drop_duplicates()
    filtered_df = filter_files_df(files_df=df,
                                  file_name_column="fileName",
                                  file_extensions=file_extensions,
                                  max_num_files=max_num_files,
                                  sort=sort,
                                  logger=logger)
    return filtered_df["fileName"].to_list()


def start_thermo_docker_container(storage_dir: str = Config.default_storage_dir,
                                  thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                                  thermo_docker_image: str = Config.default_thermo_docker_image,
                                  thermo_start_container_command_template: str =
                                  Config.default_thermo_start_container_command_template,
                                  logger: log.Logger = log.DUMMY_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"

    if utils.is_docker_container_running(thermo_docker_container_name):
        logger.info(subject + " is already running")
        return

    command = thermo_start_container_command_template % (storage_dir, thermo_docker_container_name, thermo_docker_image)

    logger.debug("Starting %s using '%s'" % (subject, command))
    return_code = os.system(command=command)  # TODO prevent command injection, e.g. disable shell
    logger.assert_true(return_code == 0, "Failed to start " + subject)

    time.sleep(1)
    logger.assert_true(condition=utils.is_docker_container_running(thermo_docker_container_name),
                       error_msg=subject + " doesn't seem to be running")
    logger.info("Started " + subject)


def stop_thermo_docker_container(thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                                 thermo_stop_container_command_template: str =
                                 Config.default_thermo_stop_container_command_template,
                                 logger: log.Logger = log.DUMMY_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"

    if not utils.is_docker_container_running(thermo_docker_container_name):
        logger.info(subject + " is already stopped")
        return

    command = thermo_stop_container_command_template % thermo_docker_container_name

    logger.debug("Stopping %s using '%s'" % (subject, command))
    os.system(command=command)  # TODO prevent command injection, e.g. disable shell
    logger.assert_true(condition=not utils.is_docker_container_running(thermo_docker_container_name),
                       error_msg=subject + " still seems to be running")
    logger.info("Stopped " + subject)


_THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS = {
    "mgf": 0,
    "mzml": 1,
    "imzml": 2,
    "parquet": 3,
}


def get_thermo_raw_file_parser_output_formats() -> Set[str]:
    return set(_THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS.keys())


def get_string_of_thermo_raw_file_parser_output_formats(format_quote: str = Config.default_option_quote,
                                                        separator: str = Config.default_option_separator) -> str:
    return utils.concat_set_of_options(options=get_thermo_raw_file_parser_output_formats(),
                                       option_quote=format_quote,
                                       separator=separator)


def convert_raw_file(filename: Optional[str],
                     output_format: str = Config.default_thermo_output_format,
                     skip_existing: bool = Config.default_skip_existing,
                     thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                     thermo_exec_command: str = Config.default_thermo_exec_command,
                     logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    logger.assert_true(output_format in get_thermo_raw_file_parser_output_formats(),
                       "Invalid output format '%s'. Currently allowed formats are: [%s]"
                       % (output_format, get_string_of_thermo_raw_file_parser_output_formats()))

    if filename is None:
        return None

    base_filename, file_ext = separate_extension(filename=filename,
                                                 extensions=set("raw"))
    if len(file_ext) == 0:
        logger.debug("Cannot convert file '%s', unknown extension" % filename)
        return None

    converted_filename = base_filename + "." + output_format

    if skip_existing and os.path.isfile(converted_filename):
        logger.info('Skipping conversion, because "%s" already exists' % converted_filename)
        return converted_filename

    logger.assert_true(utils.is_docker_container_running(thermo_docker_container_name),
                       "There is no running ThermoRawFileParser Docker container with the name " +
                       thermo_docker_container_name)

    output_format_id = _THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS[output_format]

    command = thermo_exec_command % (thermo_docker_container_name, output_format_id, filename)

    logger.debug("Converting file using command '%s'" % command)
    return_code = os.system(command)
    if return_code == 0:
        logger.info("Converted file " + filename)
        return converted_filename
    else:
        logger.warning('Failed converting file "%s" (return code = %d)' % (filename, return_code))
        return None


def convert_raw_files(filenames: List[Optional[str]],
                      output_format: str = Config.default_thermo_output_format,
                      skip_existing: bool = Config.default_skip_existing,
                      thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                      thermo_exec_command: str = Config.default_thermo_exec_command,
                      logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [convert_raw_file(filename=filename,
                             output_format=output_format,
                             skip_existing=skip_existing,
                             thermo_docker_container_name=thermo_docker_container_name,
                             thermo_exec_command=thermo_exec_command,
                             logger=logger)
            for filename in filenames]


def convert_mgf_file_to_parquet(filename: Optional[str],
                                skip_existing: bool = Config.default_skip_existing,
                                logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    base_filename, file_ext = separate_extension(filename=filename,
                                                 extensions=set("mgf"))
    if len(file_ext) == 0:
        logger.debug("Cannot convert file '%s', unknown extension" % filename)
        return None

    converted_filename = base_filename + ".parquet"

    if skip_existing and os.path.isfile(converted_filename):
        logger.info('Skipping conversion, because "%s" already exists' % converted_filename)
        return converted_filename

    try:
        df = read_mgf(filename=filename, logger=logger)
        df.to_parquet(path=converted_filename)
        logger.info("Converted file " + filename)
        return converted_filename
    except Exception as e:
        logger.warning('Failed converting file "%s" (%s)' % (filename, e))
        return None


def convert_mgf_files_to_parquet(filenames: List[Optional[str]],
                                 skip_existing: bool = Config.default_skip_existing,
                                 logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [convert_mgf_file_to_parquet(filename=filename,
                                        skip_existing=skip_existing,
                                        logger=logger)
            for filename in filenames]


def merge_mzml_and_mzid_dfs(mzml_df: pd.DataFrame,
                            mzid_df: pd.DataFrame,
                            mzml_key_columns: List[str] = None,
                            mzid_key_columns: List[str] = None,
                            logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    if mzml_key_columns is None:
        mzml_key_columns = Config.default_mzml_key_columns
    if mzid_key_columns is None:
        mzid_key_columns = Config.default_mzid_key_columns

    input_length = min(len(mzml_df), len(mzid_df))

    logger.debug("Started merging MzML and MzID dataframes")
    merged_df = mzml_df.merge(right=mzid_df,
                              how='inner',
                              left_on=mzml_key_columns,
                              right_on=mzid_key_columns)
    output_length = len(merged_df)
    unmatched_rows = input_length - output_length
    if unmatched_rows < 0:
        if logger.is_verbose():
            unique_mzml_length = len(mzml_df.drop_duplicates(inplace=False, subset=mzml_key_columns))
            unique_mzid_length = len(mzid_df.drop_duplicates(inplace=False, subset=mzid_key_columns))
            duplicate_mzml_rows = len(mzml_df) - unique_mzml_length
            duplicate_mzid_rows = len(mzid_df) - unique_mzid_length
            logger.debug("Duplicate MzML rows = %d" % duplicate_mzml_rows)
            logger.debug("Duplicate MzID rows = %d" % duplicate_mzid_rows)
        logger.warning("The key columns were no real key columns, because duplicates were found.")

    logger.debug("Finished merging MzML and MzID dataframes and matched %d x %d -> %d rows" %
                 (len(mzml_df), len(mzid_df), output_length))
    return merged_df


def merge_mzml_and_mzid_files(mzml_filename: str,
                              mzid_filename: str,
                              mzml_key_columns: Optional[List[str]] = None,
                              mzid_key_columns: Optional[List[str]] = None,
                              logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    if mzml_key_columns is None:
        mzml_key_columns = Config.default_mzml_key_columns
    if mzid_key_columns is None:
        mzid_key_columns = Config.default_mzid_key_columns

    logger.debug("Started merging MzML file '%s' and MzID file '%s'" % (mzml_filename, mzid_filename))
    mzml_df = read(filename=mzml_filename, logger=logger)
    mzid_df = read(filename=mzid_filename, logger=logger)

    merged_df = merge_mzml_and_mzid_dfs(mzml_df=mzml_df,
                                        mzid_df=mzid_df,
                                        mzml_key_columns=mzml_key_columns,
                                        mzid_key_columns=mzid_key_columns,
                                        logger=logger)
    logger.info("Finished merging MzML file '%s' and MzID file '%s' to dataframe" % (mzml_filename, mzid_filename))
    return merged_df


def merge_mzml_and_mzid_files_to_parquet(filenames: List[Optional[str]],
                                         skip_existing: bool = Config.default_skip_existing,
                                         mzml_key_columns: Optional[List[str]] = None,
                                         mzid_key_columns: Optional[List[str]] = None,
                                         prefix_length_tolerance: int = 0,
                                         target_filename_postfix: str = "_mzmlid.parquet",
                                         logger: log.Logger = log.DUMMY_LOGGER) -> List[str]:
    filenames_and_extensions = [(filename, separate_extension(filename=filename, extensions={"mzml", "mzid"}))
                                for filename in filenames if filename is not None]
    filenames_and_extensions = [(filename, (file, ext)) for filename, (file, ext) in filenames_and_extensions if
                                len(ext) > 0]
    if len(filenames_and_extensions) < 2:
        logger.warning("No MzML and MzID files left to merge")
        return []
    filenames_and_extensions = sorted(filenames_and_extensions)

    parquet_files = []
    merge_jobs = []

    last_filename, (last_filename_prefix, last_extension) = filenames_and_extensions[0]
    for filename, (filename_prefix, extension) in filenames_and_extensions[1:]:
        if last_filename is not None and extension != last_extension:
            common_filename_prefix_length = len(os.path.commonprefix([filename_prefix, last_filename_prefix]))
            required_filename_prefix_length = min(len(filename_prefix),
                                                  len(last_filename_prefix)) - prefix_length_tolerance
            if common_filename_prefix_length >= required_filename_prefix_length:
                # found a possible merging pair
                if extension == "mzml":
                    mzml_filename = filename
                    mzid_filename = last_filename
                else:
                    mzml_filename = last_filename
                    mzid_filename = filename

                target_filename = filename[:common_filename_prefix_length] + target_filename_postfix
                parquet_files.append(target_filename)

                if skip_existing and os.path.exists(target_filename):
                    logger.info("Skipping merging into '%s', because this file already exists" % target_filename)
                else:
                    merge_jobs.append((mzml_filename, mzid_filename, target_filename))

                filename = None  # skip next iteration to prevent merging the same file with multiple others
        last_filename = filename
        last_filename_prefix = filename_prefix
        last_extension = extension

    for i, (mzml_filename, mzid_filename, target_filename) in enumerate(merge_jobs):
        merged_df = merge_mzml_and_mzid_files(mzml_filename=mzml_filename,
                                              mzid_filename=mzid_filename,
                                              mzml_key_columns=mzml_key_columns,
                                              mzid_key_columns=mzid_key_columns,
                                              logger=logger)
        merged_df.to_parquet(path=target_filename)
        logger.info("Stored merged dataframe %d / %d as '%s'" % (i+1, len(merge_jobs), target_filename))

    return parquet_files
