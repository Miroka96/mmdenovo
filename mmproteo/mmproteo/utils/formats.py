import os
import time
from typing import Any, Callable, Dict, List, NoReturn, Optional, Set, Union

import pandas as pd
from pyteomics import mgf, mzid, mzml
from pyteomics.mgf import MGFBase
from pyteomics.mzid import MzIdentML
from pyteomics.mzml import MzML

from mmproteo.utils import log, utils, visualization
from mmproteo.utils.config import Config
from mmproteo.utils.filters import AbstractFilterConditionNode, filter_files_list


def iter_entries(iterator: Union[MGFBase, MzIdentML, MzML], logger: log.Logger = log.DEFAULT_LOGGER) \
        -> List[Dict[str, Any]]:
    logger.debug("Iterator type = " + str(type(iterator)))
    entries = list(iterator)
    logger.debug("Length: %d" % len(entries))
    if len(entries) > 0:
        logger.debug("Example:\n" + visualization.pretty_print_json(entries[0]))
    return entries


def read_mgf(filename: str, logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mgf.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_mzid(filename: str, logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mzid.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_mzml(filename: str, logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mzml.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_parquet(filename: str, logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    return pd.read_parquet(filename)


_FILE_READING_CONFIG: Dict[str, Callable[[str, log.Logger], pd.DataFrame]] = {
    "mgf": read_mgf,
    "mzid": read_mzid,
    "mzml": read_mzml,
    "parquet": read_parquet,
}


def get_readable_file_extensions() -> Set[str]:
    return set(_FILE_READING_CONFIG.keys())


def read(filename: str, filename_col: Optional[str] = "%s_filename", logger: log.Logger = log.DEFAULT_LOGGER) \
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
    return utils.concat_set_of_options(options=get_extractable_file_extensions(),
                                       option_quote=extension_quote,
                                       separator=separator)


def separate_extension(filename: str, extensions: Set[str]) -> (str, str):
    lower_filename = filename.lower()
    longest_extension = ""
    for ext in extensions:
        if lower_filename.endswith("." + ext) and len(longest_extension) < len(ext):
            longest_extension = ext

    if len(longest_extension) > 0:
        base_filename = filename[:len(filename) - len(longest_extension) - len(".")]
        return base_filename, longest_extension
    else:
        return filename, ""


def extract_file_if_possible(filename: Optional[str],
                             skip_existing: bool = Config.default_skip_existing,
                             logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    extracted_filename, file_ext = separate_extension(filename, get_extractable_file_extensions())
    if len(file_ext) == 0:
        logger.debug(f"Cannot extract file '{extracted_filename}', unknown extension")
        return None

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


def extract_files(filenames: List[Optional[str]],
                  skip_existing: bool = Config.default_skip_existing,
                  max_num_files: Optional[int] = None,
                  column_filter: Optional[AbstractFilterConditionNode] = None,
                  keep_null_values: bool = Config.default_keep_null_values,
                  pre_filter_files: bool = Config.default_pre_filter_files,
                  logger: log.Logger = log.DEFAULT_LOGGER) -> List[Optional[str]]:
    if pre_filter_files:
        filenames = filter_files_list(filenames=filenames,
                                      file_extensions=get_extractable_file_extensions(),
                                      max_num_files=max_num_files,
                                      column_filter=column_filter,
                                      keep_null_values=keep_null_values,
                                      drop_duplicates=not keep_null_values,
                                      sort=not keep_null_values,
                                      logger=logger)

    def file_processor(filename: Optional[str]) -> Optional[str]:
        return extract_file_if_possible(filename, skip_existing=skip_existing, logger=logger)

    return _process_files(filenames=filenames,
                          file_processor=file_processor,
                          action_name="extract",
                          max_num_files=max_num_files,
                          keep_null_values=keep_null_values,
                          logger=logger)


def start_thermo_docker_container(storage_dir: str = Config.default_storage_dir,
                                  thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                                  thermo_docker_image: str = Config.default_thermo_docker_image,
                                  thermo_start_container_command_template: str =
                                  Config.default_thermo_start_container_command_template,
                                  logger: log.Logger = log.DEFAULT_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"

    if utils.is_docker_container_running(thermo_docker_container_name):
        logger.info(subject + " is already running")
        return

    storage_dir = os.path.abspath(storage_dir)

    command = thermo_start_container_command_template.format(abs_storage_dir=storage_dir,
                                                             container_name=thermo_docker_container_name,
                                                             image_name=thermo_docker_image)

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
                                 logger: log.Logger = log.DEFAULT_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"

    if not utils.is_docker_container_running(thermo_docker_container_name):
        logger.info(subject + " is already stopped")
        return

    command = thermo_stop_container_command_template.format(container_name=thermo_docker_container_name)

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


def get_thermo_raw_file_parser_output_formats() -> List[str]:
    return sorted(_THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS.keys())


def get_string_of_thermo_raw_file_parser_output_formats(format_quote: str = Config.default_option_quote,
                                                        separator: str = Config.default_option_separator) -> str:
    return utils.concat_set_of_options(options=get_thermo_raw_file_parser_output_formats(),
                                       option_quote=format_quote,
                                       separator=separator)


def assert_valid_thermo_output_format(output_format: str, logger: log.Logger = log.DEFAULT_LOGGER) -> \
        Optional[NoReturn]:
    logger.assert_true(output_format in get_thermo_raw_file_parser_output_formats(),
                       "Invalid output format '%s'. Currently allowed formats are: [%s]"
                       % (output_format, get_string_of_thermo_raw_file_parser_output_formats()))


def convert_raw_file(filename: Optional[str],
                     output_format: str = Config.default_thermo_output_format,
                     skip_existing: bool = Config.default_skip_existing,
                     thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                     thermo_exec_command: str = Config.default_thermo_exec_command,
                     logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[str]:
    assert_valid_thermo_output_format(output_format=output_format, logger=logger)

    if filename is None:
        return None

    base_filename, file_ext = separate_extension(filename=filename,
                                                 extensions={"raw"})
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

    command = thermo_exec_command.format(container_name=thermo_docker_container_name,
                                         format=output_format_id,
                                         input=filename)

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
                      column_filter: Optional[AbstractFilterConditionNode] = None,
                      max_num_files: Optional[int] = None,
                      keep_null_values: bool = Config.default_keep_null_values,
                      pre_filter_files: bool = Config.default_pre_filter_files,
                      thermo_docker_container_name: str = Config.default_thermo_docker_container_name,
                      thermo_exec_command: str = Config.default_thermo_exec_command,
                      logger: log.Logger = log.DEFAULT_LOGGER) -> List[Optional[str]]:
    assert_valid_thermo_output_format(output_format=output_format, logger=logger)

    if pre_filter_files:
        filenames = filter_files_list(filenames=filenames,
                                      file_extensions={"raw"},
                                      max_num_files=max_num_files,
                                      column_filter=column_filter,
                                      keep_null_values=keep_null_values,
                                      drop_duplicates=not keep_null_values,
                                      sort=not keep_null_values,
                                      logger=logger)

    def file_processor(filename: Optional[str]) -> Optional[str]:
        return convert_raw_file(filename=filename,
                                output_format=output_format,
                                skip_existing=skip_existing,
                                thermo_docker_container_name=thermo_docker_container_name,
                                thermo_exec_command=thermo_exec_command,
                                logger=logger)

    return _process_files(filenames=filenames,
                          file_processor=file_processor,
                          action_name=f"raw2{output_format}-convert",
                          max_num_files=max_num_files,
                          keep_null_values=keep_null_values,
                          logger=logger)


def convert_mgf_file_to_parquet(filename: Optional[str],
                                skip_existing: bool = Config.default_skip_existing,
                                logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    base_filename, file_ext = separate_extension(filename=filename,
                                                 extensions={"mgf"})
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
        logger.warning(f'Failed converting file "{filename}" ({e})')
        return None


def _process_files(filenames: List[Optional[str]],
                   file_processor: Callable[[str], Optional[str]],
                   action_name: str,
                   max_num_files: Optional[int] = None,
                   keep_null_values: bool = Config.default_keep_null_values,
                   logger: log.Logger = log.DEFAULT_LOGGER) -> List[Optional[str]]:
    files = [file for file in filenames if file is not None]
    if not keep_null_values:
        filenames = files

    if len(files) == 0:
        logger.warning("No files available to " + action_name)
        return filenames

    if max_num_files is not None and max_num_files > 0:
        files_to_process_count = min(len(files), max_num_files)
        files = files[:files_to_process_count]

    logger.debug(f"Trying to {action_name} {len(files)} files")

    processed_files = list()
    processed_files_count = 0

    for filename in filenames:
        if filename is None:
            processed_file = None
        else:
            processed_file = file_processor(filename)

        processed_files.append(processed_file)

        if processed_file is not None:
            processed_files_count += 1

            if max_num_files is not None and processed_files_count >= max_num_files > 0:
                break

    # there might be new None values in the processed_files
    existing_processed_files = [file for file in processed_files if file is not None]

    if len(existing_processed_files) > 0:
        logger.info(f"Successfully {action_name}ed {len(existing_processed_files)} files")
    else:
        logger.info(f"No files were {action_name}ed")

    if keep_null_values:
        return processed_files
    else:
        return existing_processed_files


def convert_mgf_files_to_parquet(filenames: List[Optional[str]],
                                 skip_existing: bool = Config.default_skip_existing,
                                 max_num_files: Optional[int] = None,
                                 column_filter: Optional[AbstractFilterConditionNode] = None,
                                 keep_null_values: bool = Config.default_keep_null_values,
                                 pre_filter_files: bool = Config.default_pre_filter_files,
                                 logger: log.Logger = log.DEFAULT_LOGGER) -> List[Optional[str]]:
    if pre_filter_files:
        filenames = filter_files_list(filenames=filenames,
                                      file_extensions={"mgf"},
                                      max_num_files=max_num_files,
                                      column_filter=column_filter,
                                      keep_null_values=keep_null_values,
                                      drop_duplicates=not keep_null_values,
                                      sort=not keep_null_values,
                                      logger=logger)

    def file_processor(filename: Optional[str]) -> Optional[str]:
        return convert_mgf_file_to_parquet(filename=filename,
                                           skip_existing=skip_existing,
                                           logger=logger)

    return _process_files(filenames=filenames,
                          file_processor=file_processor,
                          action_name="mgf2parquet-convert",
                          max_num_files=max_num_files,
                          keep_null_values=keep_null_values,
                          logger=logger)


def merge_mzml_and_mzid_dfs(mzml_df: pd.DataFrame,
                            mzid_df: pd.DataFrame,
                            mzml_key_columns: List[str] = None,
                            mzid_key_columns: List[str] = None,
                            logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
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
                              logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    if mzml_key_columns is None:
        mzml_key_columns = Config.default_mzml_key_columns
    if mzid_key_columns is None:
        mzid_key_columns = Config.default_mzid_key_columns

    logger.debug("Started Merge: '%s' + '%s' -> dataframe" % (mzml_filename, mzid_filename))
    mzml_df = read(filename=mzml_filename, logger=logger)
    mzid_df = read(filename=mzid_filename, logger=logger)

    merged_df = merge_mzml_and_mzid_dfs(mzml_df=mzml_df,
                                        mzid_df=mzid_df,
                                        mzml_key_columns=mzml_key_columns,
                                        mzid_key_columns=mzid_key_columns,
                                        logger=logger)
    logger.debug("Finished Merge: '%s' + '%s' -> dataframe" % (mzml_filename, mzid_filename))
    return merged_df


def merge_mzml_and_mzid_files_to_parquet(filenames: List[Optional[str]],
                                         skip_existing: bool = Config.default_skip_existing,
                                         max_num_files: Optional[int] = None,
                                         column_filter: Optional[AbstractFilterConditionNode] = None,
                                         mzml_key_columns: Optional[List[str]] = None,
                                         mzid_key_columns: Optional[List[str]] = None,
                                         prefix_length_tolerance: int = 0,
                                         target_filename_postfix: str = Config.default_mzmlid_parquet_file_postfix,
                                         logger: log.Logger = log.DEFAULT_LOGGER) -> List[str]:
    filenames = filter_files_list(filenames=filenames,
                                  column_filter=column_filter,
                                  sort=False,
                                  logger=logger)

    filenames_and_extensions = [(filename, separate_extension(filename=filename, extensions={"mzml", "mzid"}))
                                for filename in filenames if filename is not None]
    filenames_and_extensions = [(filename, (file, ext)) for filename, (file, ext) in filenames_and_extensions if
                                len(ext) > 0]
    if len(filenames_and_extensions) < 2:
        logger.warning("No MzML and MzID files available for merging")
        return []
    filenames_and_extensions = sorted(filenames_and_extensions)

    merge_jobs = []

    last_filename, (last_filename_prefix, last_extension) = filenames_and_extensions[0]
    for filename, (filename_prefix, extension) in filenames_and_extensions[1:]:
        if max_num_files is not None and 0 < max_num_files <= len(merge_jobs):
            break

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

                merge_jobs.append((mzml_filename, mzid_filename, target_filename))

                filename = None  # skip next iteration to prevent merging the same file with multiple others
        last_filename = filename
        last_filename_prefix = filename_prefix
        last_extension = extension

    parquet_files = []

    for i, (mzml_filename, mzid_filename, target_filename) in enumerate(merge_jobs):
        if skip_existing and os.path.exists(target_filename):
            logger.info("Skipping Merge %d/%d: '%s' + '%s' -> '%s' already exists" %
                        (i + 1, len(merge_jobs), mzml_filename, mzid_filename, target_filename))
            continue

        logger.info("Started Merge %d/%d: '%s' + '%s' -> '%s'" %
                    (i + 1, len(merge_jobs), mzml_filename, mzid_filename, target_filename))
        merged_df = merge_mzml_and_mzid_files(mzml_filename=mzml_filename,
                                              mzid_filename=mzid_filename,
                                              mzml_key_columns=mzml_key_columns,
                                              mzid_key_columns=mzid_key_columns,
                                              logger=logger)
        merged_df.to_parquet(path=target_filename)
        logger.info("Finished Merge %d/%d: '%s' + '%s' -> '%s'" %
                    (i + 1, len(merge_jobs), mzml_filename, mzid_filename, target_filename))
        parquet_files.append(target_filename)

    return parquet_files
