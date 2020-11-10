#!/usr/bin/python3

import requests
import json
from pandas.io.json import json_normalize
import pandas as pd
import wget
import os
import argparse
import log
from utils import ensure_dir_exists

PRIDE_API_LIST_REPO_FILES = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
DEFAULT_VALID_FILE_EXTENSIONS = ["mzid", "mzML"]

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
        self.valid_file_extensions = args.valid_file_extensions
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
    df = pd.DataFrame(json_normalize(response['list']))
    logger.info("Received list of %d repository files" % len(df))
    return df


def get_file_links(repo_df: pd.DataFrame, num_files: int, file_extensions: list = None):
    if file_extensions is None:
        file_extensions = DEFAULT_VALID_FILE_EXTENSIONS
    else:
        assert type(file_extensions) == list, "valid_file_extensions must be a list"

    if len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = repo_df
    else:
        file_extensions = ["." + ext for ext in file_extensions]
        logger.info("Filtering repository files based on the following file extensions: " + ", ".join(file_extensions))
        valid_files = repo_df.fileName.str.endswith(file_extensions[0])
        for ext in file_extensions[1:]:
            valid_files = valid_files | repo_df.fileName.str.endswith(ext)
        filtered_files = repo_df[valid_files]
    logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    download_links = filtered_files.downloadLink

    # sort, such that files with different extensions come in pairs according to the same sample.
    sorted_filenames = filtered_files.fileName.sort_values()
    sorted_download_links = download_links.loc[sorted_filenames.index][:num_files]

    logger.info("File name filtering resulted in %d download links" % len(sorted_download_links))
    return list(sorted_download_links)


def download_files(links: list):
    link_count = len(links)
    logger.info("Downloading %d files" % link_count)
    for i, link in enumerate(links):
        logger.info("Downloading file %d/%d: %s" % (i+1, link_count, link))
        downloaded_file = wget.download(link)
        logger.info("Downloaded file %d/%d: %s" % (i+1, link_count, link))
        if downloaded_file.endswith(".gz"):
            logger.info("Extracting downloaded file " + downloaded_file)
            os.system('gunzip ' + downloaded_file)
            logger.info("Extracted downloaded file")


def main(config: Config = None):
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
    main()
