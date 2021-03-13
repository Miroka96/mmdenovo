import os
import time
from typing import List, Optional, NoReturn

from mmproteo.utils import log, utils
from mmproteo.utils.config import Config
from mmproteo.utils.filters import AbstractFilterConditionNode, filter_files_list
from mmproteo.utils.formats.read import separate_extension
from mmproteo.utils.processing import ItemProcessor


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

    return list(ItemProcessor(items=filenames,
                              item_processor=file_processor,
                              action_name=f"raw2{output_format}-convert",
                              max_num_items=max_num_files,
                              keep_null_values=keep_null_values,
                              logger=logger).process())
