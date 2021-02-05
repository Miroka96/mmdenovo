from typing import Optional, List, Set

import mmproteo.utils.formats
import pandas as pd
import wget
import os
from mmproteo.utils import log, formats, pride
from mmproteo.utils.formats import extract_files


def download_file(link: str, skip_existing: bool = True) -> (str, str):
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
                   skip_existing: bool = True,
                   count_failed_files: bool = False,
                   logger: log.Logger = log.DUMMY_LOGGER) -> List[str]:
    num_files = len(links)

    logger.info("Downloading %d files" % num_files)

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

    logger.info("Finished downloading %d files" % files_downloaded_count)
    return downloaded_files_names


def download(project_files: pd.DataFrame,
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
             logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    filtered_files = formats.filter_files_df(files_df=project_files,
                                             file_name_column=file_name_column,
                                             file_extensions=valid_file_extensions,
                                             max_num_files=max_num_files,
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

    if extract:
        filtered_files[extracted_files_column] = extract_files(filenames=filtered_files[downloaded_files_column],
                                                               skip_existing=skip_existing,
                                                               logger=logger)

    os.chdir(initial_directory)
    return filtered_files
