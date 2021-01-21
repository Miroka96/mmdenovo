from typing import Optional

import requests
import json
import pandas as pd
from mmproteo.utils import log, download as dl, formats
from mmproteo.utils.download import create_file_extension_filter
from mmproteo.utils.visualization import pretty_print_json

PRIDE_API_LIST_PROJECT_FILES = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
PRIDE_API_GET_PROJECT_SUMMARY = "https://www.ebi.ac.uk:443/pride/ws/archive/project/%s"


def get_project_link__list_project_files(project_name: str) -> str:
    return PRIDE_API_LIST_PROJECT_FILES % project_name


def get_project_link__get_project_summary(project_name: str) -> str:
    return PRIDE_API_GET_PROJECT_SUMMARY % project_name


def get_project_summary(project_name: str, logger: log.Logger = log.DUMMY_LOGGER) -> dict:
    """Get the project as a json and return it as a dataframe"""
    project_summary_link = get_project_link__get_project_summary(project_name)
    logger.info("Requesting project summary from " + project_summary_link)
    response = requests.get(project_summary_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_summary_link, len(response.text)))
    response = json.loads(response.text)
    logger.info("Received project summary for project \"%s\"" % project_name)
    return response


def info(pride_project: str, logger: log.Logger = log.DUMMY_LOGGER) -> str:
    summary_dict = get_project_summary(project_name=pride_project, logger=logger)
    return pretty_print_json(summary_dict)


def get_project_files(project_name: str,
                      logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    """Get the project as a json and return it as a dataframe"""
    project_files_link = get_project_link__list_project_files(project_name)
    logger.info("Requesting list of project files from " + project_files_link)
    response = requests.get(project_files_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_files_link, len(response.text)))
    files_dict = json.loads(response.text)
    files_df = pd.DataFrame(pd.json_normalize(files_dict['list']))
    logger.info("Received list of %d project files" % len(files_df))
    return files_df


def download(project_name: str, logger: log.Logger = log.DUMMY_LOGGER, **kwargs) -> pd.DataFrame:
    project_files = get_project_files(project_name=project_name, logger=logger)
    return dl.download(project_files, logger=logger, **kwargs)


def filter_files(files_df: pd.DataFrame,
                 file_extensions: Optional[set] = None,
                 max_num_files: Optional[int] = None,
                 sort: bool = True,
                 logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    if file_extensions is None or len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = files_df
    else:
        required_file_extensions = file_extensions
        optional_file_extensions = formats.get_extractable_file_extensions()

        required_file_extensions_list_str = "\", \"".join(required_file_extensions)
        optional_file_extensions_list_str = "\", \"".join(optional_file_extensions)
        if len(optional_file_extensions_list_str) > 0:
            optional_file_extensions_list_str = '"' + optional_file_extensions_list_str + '"'
        logger.info("Filtering repository files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (required_file_extensions_list_str,
                                                                 optional_file_extensions_list_str))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = files_df[files_df.fileName.apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    if sort:
        # sort, such that files with same prefixes but different extensions come in pairs
        sorted_files = filtered_files.sort_values(by='fileName')
    else:
        sorted_files = filtered_files

    if max_num_files is None or max_num_files == 0:
        limited_files = sorted_files
    else:
        limited_files = sorted_files[:max_num_files]

    return limited_files


def list_files(project_name: str,
               file_extensions: Optional[set] = None,
               logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    project_files = get_project_files(project_name=project_name, logger=logger)
    return filter_files(files_df=project_files, file_extensions=file_extensions, sort=False, logger=logger)
