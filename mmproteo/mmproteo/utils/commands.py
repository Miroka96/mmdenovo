import os
from typing import Dict, Union, Callable, Set

import pandas as pd

from mmproteo.utils import log, pride, visualization, formats
from mmproteo.utils.config import Config


def _run_download(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    downloaded_files = pride.download(project_name=config.pride_project,
                                      valid_file_extensions=config.valid_file_extensions,
                                      max_num_files=config.max_num_files,
                                      download_dir=config.storage_dir,
                                      skip_existing=config.skip_existing,
                                      extract=config.extract,
                                      count_failed_files=config.count_failed_files,
                                      file_name_column=config.default_file_name_column,
                                      download_link_column=config.default_download_link_column,
                                      downloaded_files_column=config.default_downloaded_files_column,
                                      extracted_files_column=config.default_extracted_files_column,
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
                           urlencode_columns=[config.default_download_link_column],
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
                           urlencode_columns=[config.default_download_link_column],
                           logger=logger)


def _validate_ls(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    config.require_pride_project(logger=logger)


def _run_extract(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    if config.processed_files is not None and config.default_downloaded_files_column in config.processed_files.columns:
        files_to_extract = config.processed_files[config.default_downloaded_files_column]
    else:
        paths_in_storage_dir = [os.path.join(config.storage_dir, file) for file in os.listdir(config.storage_dir)]
        files_in_storage_dir = [path for path in paths_in_storage_dir if os.path.isfile(path)]
        files_to_extract = files_in_storage_dir

    files_to_extract = formats.filter_files_list(filenames=files_to_extract,
                                                 file_extensions=config.valid_file_extensions,
                                                 max_num_files=config.max_num_files,
                                                 sort=Config.default_filter_sort,
                                                 drop_duplicates=Config.default_filter_drop_duplicates,
                                                 logger=logger)
    extracted_files = formats.extract_files(filenames=files_to_extract,
                                            skip_existing=config.skip_existing,
                                            logger=logger)
    result_df = pd.DataFrame(data=extracted_files, columns=[config.default_extracted_files_column])
    if config.processed_files is None:
        config.processed_files = result_df
    else:
        config.processed_files.append(result_df)


def _validate_extract(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _run_convertraw(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    files = []
    if config.processed_files is not None:
        if config.default_downloaded_files_column in config.processed_files.columns:
            files += config.processed_files[config.default_downloaded_files_column]
        if config.default_extracted_files_column in config.processed_files.columns:
            files += config.processed_files[config.default_extracted_files_column]

        files = list(set(files))
    if len(files) == 0:
        paths_in_storage_dir = [os.path.join(config.storage_dir, file) for file in os.listdir(config.storage_dir)]
        files = [path for path in paths_in_storage_dir if os.path.isfile(path)]

    formats.start_thermo_docker_container(storage_dir=config.storage_dir,
                                          thermo_docker_container_name=Config.default_thermo_docker_container_name,
                                          thermo_docker_image=Config.default_thermo_docker_image,
                                          thermo_start_container_command_template=
                                          Config.default_thermo_start_container_command_template,
                                          logger=logger)

    converted_files = formats.convert_raw_files(filenames=files,
                                                output_format=Config.default_thermo_output_format,
                                                skip_existing=config.skip_existing,
                                                thermo_docker_container_name=Config.default_thermo_docker_container_name,
                                                thermo_exec_command=Config.default_thermo_exec_command,
                                                logger=logger)

    result_df = pd.DataFrame(data=converted_files, columns=[config.default_converted_mgf_files_column])
    if config.processed_files is None:
        config.processed_files = result_df
    else:
        config.processed_files.append(result_df)

    formats.stop_thermo_docker_container(
        thermo_docker_container_name=Config.default_thermo_docker_container_name,
        thermo_stop_container_command_template=Config.default_thermo_stop_container_command_template,
        logger=logger
    )


def _validate_convertraw(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    pass


def _run_mgf2parquet(config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    files = []
    if config.processed_files is not None:
        if config.default_downloaded_files_column in config.processed_files.columns:
            files += config.processed_files[config.default_downloaded_files_column]
        if config.default_extracted_files_column in config.processed_files.columns:
            files += config.processed_files[config.default_extracted_files_column]
        if config.default_converted_mgf_files_column in config.processed_files.columns:
            files += config.processed_files[config.default_converted_mgf_files_column]
        files = list(set(files))

    if len(files) == 0:
        paths_in_storage_dir = [os.path.join(config.storage_dir, file) for file in os.listdir(config.storage_dir)]
        files = [path for path in paths_in_storage_dir if os.path.isfile(path)]

    formats.convert_mgf_files_to_parquet(filenames=files,
                                         skip_existing=config.skip_existing,
                                         logger=logger)


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
                       " or, if no files were previously processed, merge and convert the files in the data directory."
    }
}


def get_command_names() -> Set[str]:
    return set(_COMMAND_DISPATCHER.keys())


def _pad_command(command: str, width: int) -> str:
    return command.ljust(width)


def get_command_descriptions_str() -> str:
    longest_command_length = max([len(command) for command in _COMMAND_DISPATCHER.keys()])

    return '\n'.join([_pad_command(command, longest_command_length) + " : " + config['description']
                      for command, config in _COMMAND_DISPATCHER.items()])


def _get_command_config(command: str):
    command_config = _COMMAND_DISPATCHER.get(command)
    if command_config is None:
        raise NotImplementedError("%s is no known command")
    return command_config


def dispatch_commands(config: Config, logger: log.Logger = log.DUMMY_LOGGER):
    command_configs = [_get_command_config(command) for command in config.commands]

    try:
        for command_config in command_configs:
            command_config["validator"](config, logger)
    except Exception as e:
        logger.warning(str(e))

    for command_config in command_configs:
        command_config["handler"](config, logger)
