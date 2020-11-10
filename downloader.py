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


class Config:
    def __init__(self):
        self.pride_repo = None
        self.max_num_files = None
        self.download_dir = None
        self.log_file = None
        self.valid_file_extensions = None
        self.verbose = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("-r", "--pride-repo",
                            help="the name of the PRIDE repository",
                            required=True)
        parser.add_argument("-n", "--max-num-files",
                            default=2,
                            type=int,
                            help="the maximum number of files to be downloaded")
        parser.add_argument("-d", "--download-dir",
                            default="./pride",
                            help="the name of the directory, in which the downloaded files will be stored")
        parser.add_argument("-l", "--log-file",
                            default="downloader.log",
                            help="the name of the log file, relative to the download directory")
        parser.add_argument("-e", "--valid-file-extensions",
                            nargs='*',
                            default=DEFAULT_VALID_FILE_EXTENSIONS,
                            help="allowed file extensions to filter the files to be downloaded. An empty lists "
                                 "deactivates filtering.")
        parser.add_argument("-s", "--skip-existing",
                            action="store_true",
                            help="skip existing files as far as possible")
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("-v", "--verbose",
                            action="store_true",
                            help="increase output verbosity to debug level")

        args = parser.parse_args()

        self.pride_repo = args.pride_repo
        self.max_num_files = args.max_num_files
        self.download_dir = args.download_dir
        self.log_file = args.log_file
        self.valid_file_extensions = {ext.lower() for ext in args.valid_file_extensions}
        self.verbose = args.verbose

    def validate_arguments(self):
        assert len(self.pride_repo) > 0, "PRIDE_REPO must not be empty"
        assert len(self.download_dir) > 0, "download-dir must not be empty"
        assert len(self.log_file) > 0, "log-file must not be empty"

    def check(self):
        if self.max_num_files % len(self.valid_file_extensions) != 0:
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


def create_file_extension_validator(required_file_extensions: set, optional_file_extensions: set = None):
    def validate_file_extension(filename: str):
        filename = filename.lower()
        extensions = reversed(filename.split("."))
        extension = next(extensions)
        if optional_file_extensions is not None:
            if extension in optional_file_extensions:
                extension = next(extensions, extension)
        return extension in required_file_extensions

    return validate_file_extension


def get_file_links(repo_df: pd.DataFrame, num_files: int, file_extensions: set = None):
    if file_extensions is None:
        file_extensions = DEFAULT_VALID_FILE_EXTENSIONS
    else:
        assert type(file_extensions) == set, "file_extensions must be a set"

    if len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = repo_df
    else:
        required_file_extensions = file_extensions
        optional_file_extensions = set(EXTRACTABLE_FILE_EXTENSIONS.keys())

        logger.info("Filtering repository files based on the following required file extensions [%s] and the "
                    "following optional file extensions [%s]" % (
                        ", ".join(required_file_extensions),
                        ", ".join(optional_file_extensions)))

        file_extension_validator = create_file_extension_validator(required_file_extensions, optional_file_extensions)
        filtered_files = repo_df[repo_df.fileName.apply(file_extension_validator)]

    logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    download_links = filtered_files.downloadLink

    # sort, such that files with different extensions come in pairs according to the same sample.
    sorted_filenames = filtered_files.fileName.sort_values()
    sorted_download_links = download_links.loc[sorted_filenames.index][:num_files]

    logger.info("File name filtering resulted in %d download links" % len(sorted_download_links))
    return list(sorted_download_links)


def extract(filename: str, skip_existing=True):
    lower_filename = filename.lower()
    command = None
    new_filename = filename

    for ext, conf in EXTRACTABLE_FILE_EXTENSIONS.items():
        if lower_filename.endswith("." + ext):
            command = conf["command"] % filename
            new_filename = filename[:-(len(ext) + 1)]
            break

    if command is None:
        return new_filename

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


def strip_last_extension(filename):
    return ".".join(filename.split(".")[:-1])


def download_files(links: list, skip_existing=True):
    link_count = len(links)
    logger.info("Downloading %d files" % link_count)
    files_downloaded_count = 0
    for i, link in enumerate(links):
        logger.info("Downloading file %d/%d: %s" % (i + 1, link_count, link))
        filename = link.split("/")[-1]
        if skip_existing:
            if os.path.isfile(filename):
                logger.info('Skipping download, because "%s" already exists' % filename)
                downloaded_file = filename
            else:
                stripped_filename = strip_last_extension(filename)
                if os.path.isfile(stripped_filename):
                    logger.info('Skipping download and extraction, because extracted file "%s" already exists' %
                                stripped_filename)
                continue
        else:
            downloaded_file = wget.download(link)
            files_downloaded_count += 1
            logger.info("Downloaded file %d/%d: %s" % (i + 1, link_count, link))
        extract(downloaded_file, skip_existing=skip_existing)
    logger.info("Finished downloading %d files" % files_downloaded_count)


def run_downloader(config: Config = None):
    if config is None:
        config = Config()
        config.parse_arguments()
    config.validate_arguments()
    set_logger(config)
    config.check()

    repo_files = get_repo_files(repo_name=config.pride_repo)
    download_links = get_file_links(repo_df=repo_files,
                                    num_files=config.max_num_files,
                                    file_extensions=config.valid_file_extensions)
    initial_directory = os.getcwd()
    os.chdir(config.download_dir)
    download_files(download_links)
    os.chdir(initial_directory)


if __name__ == '__main__':
    run_downloader()
