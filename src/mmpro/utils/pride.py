from typing import Optional

import requests
import json
import pandas as pd
from mmpro.utils import log, download as dl
from mmpro.utils.utils import pretty_print_json

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


def download(pride_project: str, logger: log.Logger = log.DUMMY_LOGGER, **kwargs) -> pd.DataFrame:
    project_files = get_project_files(project_name=pride_project, logger=logger)
    return dl.download(project_files, logger=logger, **kwargs)
