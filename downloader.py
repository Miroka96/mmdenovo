#!/usr/bin/python3

import requests
import json
import pandas as pd
import wget
import os
import argparse
import log
from utils import ensure_dir_exists

PRIDE_API_LIST_REPO_FILES = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
DEFAULT_VALID_FILE_EXTENSIONS = ["mzid", "mzml"]
EXTRACTABLE_FILE_EXTENSIONS = {
    "gz": {
        "command": 'gunzip "%s"'
    },
    "zip": {
        "command": 'unzip "%s"'
    }}

logger = log.DummyLogger(send_welcome=False)


def deduplicate(lst: list):
    already_inserted = set()
    deduplicated_list = []
    for e in lst:
        if e not in already_inserted:
            already_inserted.add(e)
        deduplicated_list.append(e)
    return deduplicated_list


class Config:
    def __init__(self):
        self.pride_repo = None
        self.max_num_files = None
        self.download_dir = None
        self.log_file = None
        self.valid_file_extensions = None
        self.skip_existing = None
        self.extract = None
        self.verbose = None
        self.commands = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("pride-repo",
                            help="the name of the PRIDE repository, e.g. 'PXD010000' " +
                                 "from 'https://www.ebi.ac.uk/pride/ws/archive/peptide/list/project/PXD010000'")
        parser.add_argument("command",
                            nargs='*',
                            default="download",
                            choices=["download"],
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.")

        parser.add_argument("-n", "--max-num-files",
                            default=2,
                            type=int,
                            help="the maximum number of files to be downloaded. Set it to '0' to download all files.")
        parser.add_argument("-d", "--download-dir",
                            default="./pride",
                            help="the name of the directory, in which the downloaded files and the log file will be "
                                 "stored.")
        parser.add_argument("-l", "--log-file",
                            default="downloader.log",
                            help="the name of the log file, relative to the download directory.")
        parser.add_argument("-t", "--valid-file-extensions",
                            nargs='+',
                            default=DEFAULT_VALID_FILE_EXTENSIONS,
                            help="allowed file extensions to filter the files to be downloaded. " +
                                 "An empty lists, created by two double quotes (\"\"), deactivates filtering. " +
                                 "Capitalization does not matter.")
        parser.add_argument("-e", "--no-skip-existing",
                            action="store_true",
                            help="Do not skip existing files.")
        parser.add_argument("-x", "--no-extract",
                            action="store_true",
                            help="Do not try to extract downloaded files.")
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("-v", "--verbose",
                            action="store_true",
                            help="Increase output verbosity to debug level.")

        args = parser.parse_args()

        self.pride_repo = args.pride_repo
        self.max_num_files = args.max_num_files
        self.download_dir = args.download_dir
        self.log_file = args.log_file
        self.valid_file_extensions = {ext.lower() for ext in args.valid_file_extensions if len(ext) > 0}
        self.skip_existing = (not args.no_skip_existing)
        self.extract = (not args.no_extract)
        self.verbose = args.verbose

        self.commands = deduplicate(args.command)

    def validate_arguments(self):
        assert len(self.pride_repo) > 0, "PRIDE_REPO must not be empty"
        assert len(self.download_dir) > 0, "download-dir must not be empty"
        assert len(self.log_file) > 0, "log-file must not be empty"

    def check(self):
        if len(self.valid_file_extensions) != 0 and self.max_num_files % len(self.valid_file_extensions) != 0:
            logger.info(
                "Warning: max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
                "files that belong together are also downloaded together")
        ensure_dir_exists(self.download_dir)


def set_logger(config: Config):
    global logger
    logger = log.create_logger(name="PRIDE_Downloader",
                               log_dir=config.download_dir,
                               filename=config.log_file,
                               verbose=config.verbose)


def get_repo_link(repo_name: str):
    return PRIDE_API_LIST_REPO_FILES % repo_name


def get_repo_files(repo_name: str):
    """Get the project as a json and return it as a dataframe"""
    repo_link = get_repo_link(repo_name)
    logger.info("Requesting list of repository files from " + repo_link)
    response = requests.get(repo_link)
    logger.debug("Received response from %s with length of %d bytes" % (repo_link, len(response.text)))
    response = json.loads(response.text)
    df = pd.DataFrame(pd.json_normalize(response['list']))
    logger.info("Received list of %d repository files" % len(df))
    return df


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


def filter_files(repo_df: pd.DataFrame, file_extensions: set = None):
    if file_extensions is None:
        file_extensions = set(DEFAULT_VALID_FILE_EXTENSIONS)
    else:
        assert type(file_extensions) == set, "file_extensions must be a set"

    if len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = repo_df
    else:
        required_file_extensions = file_extensions
        optional_file_extensions = set(EXTRACTABLE_FILE_EXTENSIONS.keys())

        logger.info("Filtering repository files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (
                        "\", \"".join(required_file_extensions),
                        "\", \"".join(optional_file_extensions)))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = repo_df[repo_df.fileName.apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    # sort, such that files with different extensions come in pairs according to the same sample.
    sorted_files = filtered_files.sort_values(by='fileName')
    return sorted_files


def extract_if_possible(filename: str, skip_existing=True):
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


def download_files(links: list, max_num_files: int, skip_existing=True, extract=True):
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
                logger.info('Failed to download "%s" because of "%s"' % (link, e))
                continue
            else:
                files_downloaded_count += 1
                logger.info("Downloaded file %d/%d: %s" % (files_processed, max_num_files, link))

        if extract:
            extract_if_possible(downloaded_file, skip_existing=skip_existing)
        files_processed += 1
    logger.info("Finished downloading %d files" % files_downloaded_count)


def run_downloader(config: Config = None):
    if config is None:
        config = Config()
        config.parse_arguments()
    config.validate_arguments()
    set_logger(config)
    config.check()

    repo_files = get_repo_files(repo_name=config.pride_repo)
    filtered_files = filter_files(repo_df=repo_files,
                                  file_extensions=config.valid_file_extensions)
    config.max_num_files = min(config.max_num_files, len(filtered_files))
    initial_directory = os.getcwd()
    os.chdir(config.download_dir)
    download_files(links=filtered_files.downloadLink,
                   max_num_files=config.max_num_files,
                   skip_existing=config.skip_existing,
                   extract=config.extract)
    os.chdir(initial_directory)


if __name__ == '__main__':
    run_downloader()
