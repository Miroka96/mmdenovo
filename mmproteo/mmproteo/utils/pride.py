from json import JSONDecodeError
from typing import Optional, List, Set, Dict

import requests
import json
import pandas as pd
from requests import Response

from mmproteo.utils import log, download as dl, formats
from mmproteo.utils.formats import create_file_extension_filter
from mmproteo.utils.visualization import pretty_print_json


# HTTP 204 - No Content
def _handle_204_response(response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    logger.error("Repository does not exist")


# HTTP 401 - Unauthorized
def _handle_401_response(response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    message = response_dict.get('message', "?")
    developer_message = response_dict.get('developerMessage', "?")
    more_info_url = response_dict.get('moreInfoUrl', "?")
    logger.error("%s (%s) -> %s" % (message, developer_message, more_info_url))


def _handle_unknown_response(status_code: int, response_dict: dict, logger: log.Logger = log.DUMMY_LOGGER) -> None:
    logger.error("Received unknown response code %d or content" % status_code)
    logger.debug(pretty_print_json(response_dict))


def _handle_non_200_response_codes(response: Optional[Response], logger: log.Logger = log.DUMMY_LOGGER) -> None:
    if response is None:
        return None
    if response.status_code == 204:
        return _handle_204_response(logger=logger)
    try:
        response_dict = json.loads(response.text)
    except JSONDecodeError:
        logger.error("Received unknown non-JSON response with response code %d" % response.status_code)
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
    return separator.join([extension_quote + version + extension_quote for version in DEFAULT_PRIDE_API_VERSIONS])


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
            logger.info("Received project summary (using API version %s) for project \"%s\"" % (api_version, project_name))
            return response_dict

    logger.error("Could not get project summary.")
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
            logger.info("Received project file list (using API version %s) with %d files for project \"%s\"" %
                        (api_version, len(files_df), project_name))
            return files_df

    logger.error("Could not get list of project files.")
    return None


def download(project_name: str, api_versions: List[str] = None, logger: log.Logger = log.DUMMY_LOGGER, **kwargs) \
        -> Optional[pd.DataFrame]:
    project_files = get_project_files(project_name=project_name, api_versions=api_versions, logger=logger)
    if project_files is None:
        return None
    return dl.download(project_files, logger=logger, **kwargs)


def filter_files(files_df: Optional[pd.DataFrame],
                 file_extensions: Optional[Set[str]] = None,
                 max_num_files: Optional[int] = None,
                 sort: bool = True,
                 logger: log.Logger = log.DUMMY_LOGGER) -> Optional[pd.DataFrame]:
    if files_df is None:
        return None

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
               api_versions: List[str] = None,
               file_extensions: Optional[Set[str]] = None,
               logger: log.Logger = log.DUMMY_LOGGER) -> Optional[pd.DataFrame]:
    project_files = get_project_files(project_name=project_name, api_versions=api_versions, logger=logger)
    if project_files is None:
        return None
    return filter_files(files_df=project_files, file_extensions=file_extensions, sort=False, logger=logger)
