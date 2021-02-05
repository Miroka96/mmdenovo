from json import JSONDecodeError
from typing import Optional, List, Set, Dict

import requests
import json
import pandas as pd
from requests import Response

from mmproteo.utils import log, download as dl, utils
from mmproteo.utils.formats import filter_files_df
from mmproteo.utils.visualization import pretty_print_json


# HTTP 204 - No Content
def _handle_204_response(response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    logger.warning("Repository does not exist")


# HTTP 401 - Unauthorized
def _handle_401_response(response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    message = response_dict.get('message', "?")
    developer_message = response_dict.get('developerMessage', "?")
    more_info_url = response_dict.get('moreInfoUrl', "?")
    logger.warning("%s (%s) -> %s" % (message, developer_message, more_info_url))


def _handle_unknown_response(status_code: int, response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    logger.warning("Received unknown response code %d or content" % status_code)
    logger.debug(pretty_print_json(response_dict))


def _handle_non_200_response_codes(response: Optional[Response], logger: log.Logger = log.DUMMY_LOGGER) -> None:
    if response is None:
        return None
    if response.status_code == 204:
        return _handle_204_response(logger=logger)
    try:
        response_dict = json.loads(response.text)
    except JSONDecodeError:
        logger.warning("Received unknown non-JSON response with response code %d" % response.status_code)
        logger.debug("Response text: '%s'" % response.text)
        return
    if response.status_code == 401:
        return _handle_401_response(response_dict, logger)
    return _handle_unknown_response(response.status_code, response_dict, logger)


def _request_json(url: str, subject_name: str, logger: log.Logger = log.DUMMY_LOGGER) -> Optional[dict]:
    logger.info("Requesting %s from %s" % (subject_name, url))
    response = requests.get(url)
    logger.debug("Received response from %s with length of %d bytes and status code %d" %
                 (url, len(response.text), response.status_code))

    if response.status_code == 200:
        response_dict = json.loads(response.text)
        return response_dict
    else:
        _handle_non_200_response_codes(response, logger)
        return None


class AbstractPrideApi:
    LIST_PROJECT_FILES_URL = None
    GET_PROJECT_SUMMARY_URL = None

    def __init__(self, version: str, logger: log.Logger = log.DUMMY_LOGGER):
        self.logger = logger
        self.version = version

    def get_project_summary(self, project_name: str) -> Optional[dict]:
        project_summary_link = self.GET_PROJECT_SUMMARY_URL % project_name
        return _request_json(project_summary_link, "project summary using API version " + self.version, self.logger)

    def get_project_files(self, project_name: str):
        raise NotImplementedError


class PrideApiV1(AbstractPrideApi):
    LIST_PROJECT_FILES_URL = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
    GET_PROJECT_SUMMARY_URL = "https://www.ebi.ac.uk/pride/ws/archive/project/%s"

    def get_project_files(self, project_name: str) -> Optional[pd.DataFrame]:
        project_files_link = self.LIST_PROJECT_FILES_URL % project_name
        response_dict = _request_json(url=project_files_link,
                                      subject_name="list of project files using API version " + self.version,
                                      logger=self.logger)
        if response_dict is None:
            return None
        try:
            files_df = pd.DataFrame(pd.json_normalize(response_dict['list']))
            return files_df
        except:
            return None


class PrideApiV2(AbstractPrideApi):
    LIST_PROJECT_FILES_URL = "https://www.ebi.ac.uk/pride/ws/archive/v2/files/byProject?accession=%s"
    GET_PROJECT_SUMMARY_URL = "https://www.ebi.ac.uk/pride/ws/archive/v2/projects/%s"

    @staticmethod
    def _get_ftp_or_http_file_location(file_location_entries: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
        compatible_file_location_entries = [entry for entry in file_location_entries
                                            if 'value' in entry
                                            and entry['value'].lower().startswith(('ftp://', 'http://', 'https://'))]
        if len(compatible_file_location_entries) > 0:
            return compatible_file_location_entries[0]
        return None

    @staticmethod
    def _format_file_entry(file_entry: dict) -> dict:
        if 'publicFileLocations' in file_entry:
            try:
                file_entry['publicFileLocations'] = PrideApiV2._get_ftp_or_http_file_location(
                    file_entry['publicFileLocations'])
                file_entry['publicFileLocation'] = file_entry.pop('publicFileLocations')  # rename to singular
            except:
                pass
        return file_entry

    def get_project_files(self, project_name: str) -> Optional[pd.DataFrame]:
        project_files_link = self.LIST_PROJECT_FILES_URL % project_name
        response_dict_list = _request_json(url=project_files_link,
                                           subject_name="list of project files using API version " + self.version,
                                           logger=self.logger)
        if response_dict_list is None:
            return None
        response_dict_list = [self._format_file_entry(response_dict) for response_dict in response_dict_list]

        try:
            files_df = pd.DataFrame(pd.json_normalize(response_dict_list))
        except:
            return None

        files_df = files_df.rename(columns={"publicFileLocation.value": "downloadLink"})
        return files_df


PRIDE_APIS = {
    "1": PrideApiV1,
    "2": PrideApiV2,
}

DEFAULT_PRIDE_API_VERSIONS = ["2", "1"]


def get_string_of_pride_api_versions(extension_quote: str = '"', separator: str = ", ") -> str:
    return utils.concat_set_of_options(options=DEFAULT_PRIDE_API_VERSIONS,
                                       option_quote=extension_quote,
                                       separator=separator)


def get_project_summary(project_name: str,
                        api_versions: List[str] = None,
                        logger: log.Logger = log.DUMMY_LOGGER) -> Optional[dict]:
    """Get the project as a json and return it as a dataframe"""
    if api_versions is None or len(api_versions) == 0:
        api_versions = DEFAULT_PRIDE_API_VERSIONS

    for api_version in api_versions:
        api = PRIDE_APIS[api_version](api_version, logger)
        response_dict = api.get_project_summary(project_name)
        if response_dict is not None:
            logger.info("Received project summary using API version %s for project \"%s\"" %
                        (api_version, project_name))
            return response_dict

    logger.warning("Could not get project summary.")
    return None


def info(project_name: str, api_versions: List[str] = None, logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    summary_dict = get_project_summary(project_name=project_name, api_versions=api_versions, logger=logger)
    if summary_dict is None:
        return None
    return pretty_print_json(summary_dict)


def get_project_files(project_name: str,
                      api_versions: List[str] = None,
                      logger: log.Logger = log.DUMMY_LOGGER) -> Optional[pd.DataFrame]:
    """Get the project as a json and return it as a dataframe"""
    if api_versions is None or len(api_versions) == 0:
        api_versions = DEFAULT_PRIDE_API_VERSIONS

    for api_version in api_versions:
        api = PRIDE_APIS[api_version](api_version, logger)
        files_df = api.get_project_files(project_name)
        if files_df is not None:
            logger.info("Received project file list using API version %s with %d files for project \"%s\"" %
                        (api_version, len(files_df), project_name))
            return files_df

    logger.warning("Could not get list of project files.")
    return None


def download(project_name: str,
             valid_file_extensions: Optional[Set[str]] = None,
             max_num_files: Optional[int] = None,
             download_dir: str = "download",
             skip_existing: bool = True,
             extract: bool = False,
             count_failed_files: bool = False,
             file_name_column: str = "fileName",
             download_link_column: str = 'downloadLink',
             downloaded_files_column: str = 'downloaded_files',
             extracted_files_column: str = 'extracted_files',
             api_versions: List[str] = None,
             logger: log.Logger = log.DUMMY_LOGGER) \
        -> Optional[pd.DataFrame]:
    project_files = get_project_files(project_name=project_name, api_versions=api_versions, logger=logger)
    if project_files is None:
        return None
    return dl.download(project_files=project_files,
                       valid_file_extensions=valid_file_extensions,
                       max_num_files=max_num_files,
                       download_dir=download_dir,
                       skip_existing=skip_existing,
                       extract=extract,
                       count_failed_files=count_failed_files,
                       file_name_column=file_name_column,
                       download_link_column=download_link_column,
                       downloaded_files_column=downloaded_files_column,
                       extracted_files_column=extracted_files_column,
                       logger=logger)


def list_files(project_name: str,
               api_versions: List[str] = None,
               file_extensions: Optional[Set[str]] = None,
               logger: log.Logger = log.DUMMY_LOGGER) -> Optional[pd.DataFrame]:
    project_files = get_project_files(project_name=project_name, api_versions=api_versions, logger=logger)
    if project_files is None:
        return None
    return filter_files_df(files_df=project_files, file_extensions=file_extensions, sort=False, logger=logger)
