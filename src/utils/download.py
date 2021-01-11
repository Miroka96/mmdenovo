import sys
import requests
import json
import pandas as pd
import wget
import os
import argparse
from utils import log, pride
from utils.utils import ensure_dir_exists

EXTRACTABLE_FILE_EXTENSIONS = {
    "gz": {
        "command": 'gunzip "%s"'
    },
    "zip": {
        "command": 'unzip "%s"'
    }}

DUMMY_LOGGER = log.DummyLogger(send_welcome=False)


def create_file_extension_filter(required_file_extensions: set, optional_file_extensions: set = None):
    def filter_file_extension(filename: str):
        filename = filename.lower()
        extensions = reversed(filename.split("."))
        extension = next(extensions)
        if optional_file_extensions is not None:
            if extension in optional_file_extensions:
                extension = next(extensions, extension)
        return extension in required_file_extensions

    return filter_file_extension


def filter_files(files_df: pd.DataFrame, file_extensions: set = None, logger: log.Logger = DUMMY_LOGGER):
    assert file_extensions is None or type(file_extensions) == set, "file_extensions must be a set"

    if len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = files_df
    else:
        required_file_extensions = file_extensions
        optional_file_extensions = set(EXTRACTABLE_FILE_EXTENSIONS.keys())

        logger.info("Filtering repository files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (
                        "\", \"".join(required_file_extensions),
                        "\", \"".join(optional_file_extensions)))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = files_df[files_df.fileName.apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    # sort, such that files with different extensions come in pairs according to the same sample.
    sorted_files = filtered_files.sort_values(by='fileName')
    return sorted_files


def extract_if_possible(filename: str, skip_existing=True, logger: log.Logger = DUMMY_LOGGER):
    lower_filename = filename.lower()

    file_ext = lower_filename.split('.')[-1]
    if file_ext in EXTRACTABLE_FILE_EXTENSIONS:
        command = EXTRACTABLE_FILE_EXTENSIONS[file_ext]["command"] % filename
        new_filename = filename[:-(len(file_ext) + 1)]
    else:
        return filename

    if skip_existing and os.path.isfile(new_filename):
        logger.info('Skipping extraction, because "%s" already exists' % new_filename)
    else:
        logger.info("Extracting downloaded file using '%s'" % command)
        return_code = os.system(command)
        if return_code == 0:
            logger.info("Extracted downloaded file")
        else:
            logger.info('Failed extracting downloaded file "%s" (return code = %d)' % (filename, return_code))

    return new_filename


def strip_archive_extension(filename: str):
    parts = filename.split(".")
    if parts[-1].lower() in EXTRACTABLE_FILE_EXTENSIONS:
        return ".".join(parts[:-1]), True
    else:
        return filename, False


def download_files(links: list, 
                   max_num_files: int, 
                   skip_existing=True, 
                   extract=True, 
                   count_failed_files=False, 
                   logger: log.Logger = DUMMY_LOGGER):
    logger.info("Downloading %d files" % max_num_files)
    files_downloaded_count = 0
    files_processed = 1
    for link in links:
        if 0 < max_num_files < files_processed:
            break
        logger.info("Downloading file %d/%d: %s" % (files_processed, max_num_files, link))
        filename = link.split("/")[-1]
        downloaded = False
        if skip_existing:
            if os.path.isfile(filename):
                downloaded = True

            if extract:
                # gunzip deletes the archive file after extraction, other programmes keep it
                extracted_filename, compressed = strip_archive_extension(filename)
                if compressed and os.path.isfile(extracted_filename):
                    logger.info('Skipping download and extraction, because extracted file "%s" already exists' %
                                extracted_filename)
                    files_processed += 1
                    continue

        if downloaded:
            logger.info('Skipping download, because "%s" already exists' % filename)
            downloaded_file = filename
        else:
            try:
                downloaded_file = wget.download(link)
            except Exception as e:
                file_of_n = ""
                if count_failed_files:
                    files_processed += 1
                    file_of_n = "file %d/%d: " % (files_processed, max_num_files)
                logger.info('Failed to download %s"%s" because of "%s"' % (file_of_n, link, e))
                continue
            else:
                files_downloaded_count += 1
                logger.info('Downloaded file %d/%d: "%s"' % (files_processed, max_num_files, link))

        if extract:
            extract_if_possible(downloaded_file, skip_existing=skip_existing)
        files_processed += 1
    logger.info("Finished downloading %d files" % files_downloaded_count)


def download(project_files: pd.DataFrame,
             valid_file_extensions: set,
             max_num_files: int,
             download_dir: str,
             skip_existing: bool,
             extract: bool,
             count_failed_files: bool):
    filtered_files = filter_files(files_df=project_files,
                                  file_extensions=valid_file_extensions)
    max_num_files = min(max_num_files, len(filtered_files))
    initial_directory = os.getcwd()
    os.chdir(download_dir)
    download_files(links=filtered_files.downloadLink,
                   max_num_files=max_num_files,
                   skip_existing=skip_existing,
                   extract=extract,
                   count_failed_files=count_failed_files)
    os.chdir(initial_directory)
