import json
import os
from typing import List, NoReturn, Optional, Set, Union

import pandas as pd
import wget
from requests import Response
import requests

import mmproteo.utils.filters
from mmproteo.utils import formats, log
from mmproteo.utils.config import Config
from mmproteo.utils.visualization import pretty_print_json


def download_file(link: str, skip_existing: bool = Config.default_skip_existing) -> (str, str):
    filename = link.split("/")[-1]
    downloaded_file_name = None

    found_downloaded_file = False
    found_extracted_file = False

    skip_reason = None

    if skip_existing:
        if os.path.isfile(filename):
            found_downloaded_file = True
            downloaded_file_name = filename
            skip_reason = 'file "%s" already exists' % downloaded_file_name

        extracted_file_name, extension = formats.separate_extension(filename, formats.get_extractable_file_extensions())
        file_is_extractable = len(extension) > 0

        if file_is_extractable:
            # gunzip deletes the archive file after extraction, other programmes keep it
            if os.path.isfile(extracted_file_name):
                found_extracted_file = True
                skip_reason = 'extracted file "%s" already exists' % extracted_file_name

    if not (skip_existing and (found_downloaded_file or found_extracted_file)):
        downloaded_file_name = wget.download(link)  # might raise an EXCEPTION

    return downloaded_file_name, skip_reason


def download_files(links: List[str],
                   skip_existing: bool = Config.default_skip_existing,
                   count_failed_files: bool = Config.default_count_failed_files,
                   logger: log.Logger = log.DEFAULT_LOGGER) -> List[str]:
    num_files = len(links)

    if num_files > 1:
        plural_s = "s"
    else:
        plural_s = ""
    logger.info(f"Downloading {num_files} file{plural_s}")

    files_downloaded_count = 0
    files_processed = 1

    downloaded_files_names = []

    for link in links:
        logger.info("Downloading file %d/%d: %s" % (files_processed, num_files, link))

        try:
            downloaded_file_name, skip_reason = download_file(link, skip_existing)
        except Exception as e:
            downloaded_file_name, skip_reason = None, None
            logger.info('Failed to download file %d/%d ("%s") because of "%s"' %
                        (files_processed, num_files, link, e))
        downloaded_file_available = downloaded_file_name is not None
        download_skipped = skip_reason is not None
        download_succeeded = downloaded_file_available or download_skipped

        if download_succeeded:
            if download_skipped:
                logger.info('Skipped download, because ' + skip_reason)
            else:
                files_downloaded_count += 1
                logger.info('Downloaded file %d/%d: "%s"' % (files_processed, num_files, link))

        downloaded_files_names.append(downloaded_file_name)

        if download_succeeded or count_failed_files:
            files_processed += 1

    if files_downloaded_count > 1:
        plural_s = "s"
    else:
        plural_s = ""
    logger.info(f"Finished downloading {files_downloaded_count} file{plural_s}")
    return downloaded_files_names


def download_filtered_files(project_files: pd.DataFrame,
                            valid_file_extensions: Optional[Set[str]] = None,
                            max_num_files: Optional[int] = None,
                            column_filter: Optional[mmproteo.utils.filters.AbstractFilterConditionNode] = None,
                            download_dir: str = Config.default_storage_dir,
                            skip_existing: bool = Config.default_skip_existing,
                            count_failed_files: bool = Config.default_count_failed_files,
                            file_name_column: str = Config.default_file_name_column,
                            download_link_column: str = Config.default_download_link_column,
                            downloaded_files_column: str = Config.default_downloaded_files_column,
                            logger: log.Logger = log.DEFAULT_LOGGER) -> pd.DataFrame:
    filtered_files = mmproteo.utils.filters.filter_files_df(files_df=project_files,
                                                            file_name_column=file_name_column,
                                                            file_extensions=valid_file_extensions,
                                                            max_num_files=max_num_files,
                                                            column_filter=column_filter,
                                                            sort=True,
                                                            logger=logger)
    logger.assert_true(download_link_column in filtered_files.columns,
                       "Could not find column '%s' in filtered_files dataframe" % download_link_column)

    initial_directory = os.getcwd()
    os.chdir(download_dir)

    filtered_files[downloaded_files_column] = download_files(links=filtered_files[download_link_column],
                                                             skip_existing=skip_existing,
                                                             count_failed_files=count_failed_files,
                                                             logger=logger)

    os.chdir(initial_directory)
    return filtered_files


class AbstractDownloader:
    def __init__(self, logger: log.Logger = log.DEFAULT_LOGGER):
        self.logger = logger

    @staticmethod
    # HTTP 204 - No Content
    def _handle_204_response(response_dict: dict, logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[NoReturn]:
        logger.warning("Repository does not exist")

    @staticmethod
    # HTTP 401 - Unauthorized
    def _handle_401_response(response_dict: dict, logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[NoReturn]:
        message = response_dict.get('message', "?")
        developer_message = response_dict.get('developerMessage', "?")
        more_info_url = response_dict.get('moreInfoUrl', "?")
        logger.warning("%s (%s) -> %s" % (message, developer_message, more_info_url))

    @staticmethod
    def _handle_unknown_response(status_code: int, response_dict: dict,
                                 logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[NoReturn]:
        logger.warning("Received unknown response code %d or content" % status_code)
        logger.debug(pretty_print_json(response_dict))

    def _handle_non_200_response_codes(self, response: Optional[Response],
                                       logger: log.Logger = log.DEFAULT_LOGGER) -> Optional[NoReturn]:
        if response is None:
            return None
        if response.status_code == 204:
            return self._handle_204_response(logger=logger)
        try:
            response_dict = json.loads(response.text)
        except json.JSONDecodeError:
            logger.warning("Received unknown non-JSON response with response code %d" % response.status_code)
            logger.debug("Response text: '%s'" % response.text)
            return
        if response.status_code == 401:
            return self._handle_401_response(response_dict, logger)
        return self._handle_unknown_response(response.status_code, response_dict, logger)

    def request_json(self, url: str, subject_name: str, logger: log.Logger = log.DEFAULT_LOGGER) -> Union[
        Optional[dict],
        Optional[NoReturn],
    ]:
        logger.info("Requesting %s from %s" % (subject_name, url))
        response = requests.get(url)
        logger.debug("Received response from %s with length of %d bytes and status code %d" %
                     (url, len(response.text), response.status_code))

        if response.status_code == 200:
            response_dict = json.loads(response.text)
            return response_dict
        else:
            self._handle_non_200_response_codes(response, logger)
            return None
