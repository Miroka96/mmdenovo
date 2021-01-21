from typing import Optional, List, Callable
import pandas as pd
import wget
import os
from mmproteo.utils import log, formats, pride


def create_file_extension_filter(required_file_extensions: set,
                                 optional_file_extensions: Optional[set] = None) -> Callable[[str], bool]:
    if optional_file_extensions is not None:
        assert len(required_file_extensions.intersection(optional_file_extensions)) == 0, "Extension sets must be " \
                                                                                          "distinct."

    def filter_file_extension(filename: str):
        filename = filename.lower()
        extensions = reversed(filename.split("."))
        extension = next(extensions)
        if optional_file_extensions is not None:
            if extension in optional_file_extensions:
                extension = next(extensions, extension)
        return extension in required_file_extensions

    return filter_file_extension


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

        extracted_file_name, extension = formats.separate_archive_extension(filename)
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


def extract_files(filenames: List[Optional[str]],
                  skip_existing: bool = True,
                  logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [formats.extract_file_if_possible(filename, skip_existing=skip_existing, logger=logger)
            for filename in filenames]


def download(project_files: pd.DataFrame,
             valid_file_extensions: set,
             max_num_files: Optional[int],
             download_dir: str,
             skip_existing: bool,
             extract: bool,
             count_failed_files: bool,
             logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    filtered_files = pride.filter_files(files_df=project_files,
                                        file_extensions=valid_file_extensions,
                                        max_num_files=max_num_files,
                                        logger=logger)
    initial_directory = os.getcwd()
    os.chdir(download_dir)

    filtered_files['downloaded_files'] = download_files(links=filtered_files.downloadLink,
                                                        skip_existing=skip_existing,
                                                        count_failed_files=count_failed_files,
                                                        logger=logger)

    if extract:
        filtered_files['extracted_files'] = extract_files(filenames=filtered_files.downloaded_files,
                                                          skip_existing=skip_existing,
                                                          logger=logger)

    os.chdir(initial_directory)
    return filtered_files
