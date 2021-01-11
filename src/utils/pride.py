import requests
import json
import pandas as pd
from utils import log, download
from utils.utils import pretty_print_json

PRIDE_API_LIST_PROJECT_FILES = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
PRIDE_API_GET_PROJECT_SUMMARY = "https://www.ebi.ac.uk:443/pride/ws/archive/project/%s"

DUMMY_LOGGER = log.DummyLogger(send_welcome=False)

def get_repo_link_list_project_files(repo_name: str) -> str:
    return PRIDE_API_LIST_PROJECT_FILES % repo_name


def get_project_link_get_project_summary(project_name: str):
    return PRIDE_API_GET_PROJECT_SUMMARY % project_name


def get_project_summary(project_name: str, logger: log.Logger = DUMMY_LOGGER) -> dict:
    """Get the project as a json and return it as a dataframe"""
    project_summary_link = get_project_link_get_project_summary(project_name)
    logger.info("Requesting project summary from " + project_summary_link)
    response = requests.get(project_summary_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_summary_link, len(response.text)))
    response = json.loads(response.text)
    logger.info("Received project summary for project \"%s\"" % project_name)
    return response


def info(pride_project: str) -> str:
    summary_dict = get_project_summary(project_name=pride_project)
    return pretty_print_json(summary_dict)


def get_project_files(project_name: str, logger: log.Logger = DUMMY_LOGGER) -> pd.DataFrame:
    """Get the project as a json and return it as a dataframe"""
    project_files_link = get_repo_link_list_project_files(project_name)
    logger.info("Requesting list of project files from " + project_files_link)
    response = requests.get(project_files_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_files_link, len(response.text)))
    response = json.loads(response.text)
    df = pd.DataFrame(pd.json_normalize(response['list']))
    logger.info("Received list of %d project files" % len(df))
    return df


def download(pride_project: str, **kwargs):
    project_files = get_project_files(project_name=pride_project)
    download.download(project_files, **kwargs)
