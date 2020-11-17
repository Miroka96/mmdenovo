#!/usr/bin/python3
import sys

import requests
import json
import pandas as pd
import wget
import os
import argparse
import log
from utils import ensure_dir_exists

PRIDE_API_LIST_PROJECT_FILES = "https://www.ebi.ac.uk/pride/ws/archive/file/list/project/%s"
PRIDE_API_GET_PROJECT_SUMMARY = "https://www.ebi.ac.uk:443/pride/ws/archive/project/%s"
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

    command_str = "command"
    pride_project_str = "--pride-project"
    max_num_files_str = "--max-num-files"
    count_failed_files_str = "--count-failed-files"
    storage_dir_str = "--storage-dir"
    log_file_str = "--log-file"
    log_to_stdout_str = "--log-to-stdout"
    valid_file_extensions_str = "--valid-file-extensions"
    no_skip_existing_str = "--no-skip-existing"
    no_extract_str = "--no-extract"
    verbose_str = "--verbose"

    def __init__(self):
        self.pride_project = None
        self.max_num_files = None
        self.count_failed_files = None
        self.storage_dir = None
        self.log_file = None
        self.log_to_stdout = None
        self.valid_file_extensions = None
        self.skip_existing = None
        self.extract = None
        self.verbose = None
        self.commands = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument(Config.command_str,
                            nargs='*',
                            choices=list(COMMAND_DISPATCHER.keys()),
                            help="the list of actions to be performed on the repository. " +
                                 "Every action can only occur once. " +
                                 "Duplicates are dropped after the first occurrence.")
        parser.add_argument("-p", Config.pride_project_str,
                            help="the name of the PRIDE project, e.g. 'PXD010000' " +
                                 "from 'https://www.ebi.ac.uk/pride/ws/archive/peptide/list/project/PXD010000'." +
                                 "For some commands, this parameter is required.")
        parser.add_argument("-n", Config.max_num_files_str,
                            default=2,
                            type=int,
                            help="the maximum number of files to be downloaded. Set it to '0' to download all files.")
        parser.add_argument(Config.count_failed_files_str,
                            action="store_false",
                            help="Count failed files and do not just skip them. " +
                                 "This is relevant for the max-num-files parameter.")
        parser.add_argument("-d", Config.storage_dir_str,
                            default="./pride",
                            help="the name of the directory, in which the downloaded files and the log file will be "
                                 "stored.")
        parser.add_argument("-l", Config.log_file_str,
                            default="downloader.log",
                            help="the name of the log file, relative to the download directory.")
        parser.add_argument(Config.log_to_stdout_str,
                            action="store_true",
                            help="Log to stdout instead of stderr.")
        parser.add_argument("-t", Config.valid_file_extensions_str,
                            nargs='+',
                            default=DEFAULT_VALID_FILE_EXTENSIONS,
                            help="allowed file extensions to filter the files to be downloaded. " +
                                 "An empty lists, created by two double quotes (\"\"), deactivates filtering. " +
                                 "Capitalization does not matter.")
        parser.add_argument("-e", Config.no_skip_existing_str,
                            action="store_true",
                            help="Do not skip existing files.")
        parser.add_argument("-x", Config.no_extract_str,
                            action="store_true",
                            help="Do not try to extract downloaded files.")
        # store_true turns "verbose" into a flag:
        # The existence of "verbose" equals True, the lack of existence equals False
        parser.add_argument("-v", Config.verbose_str,
                            action="store_true",
                            help="Increase output verbosity to debug level.")

        args = parser.parse_args()

        self.pride_project = args.pride_project
        self.max_num_files = args.max_num_files
        self.count_failed_files = args.count_failed_files
        self.storage_dir = args.storage_dir
        self.log_file = args.log_file
        self.log_to_stdout = args.log_to_stdout
        self.valid_file_extensions = {ext.lower() for ext in args.valid_file_extensions if len(ext) > 0}
        self.skip_existing = (not args.no_skip_existing)
        self.extract = (not args.no_extract)
        self.verbose = args.verbose

        self.commands = deduplicate(args.command)

    def require_pride_project(self):
        assert self.pride_project is not None, Config.pride_project_str + " is missing"
        assert len(self.pride_project) > 0, Config.pride_project_str + " must not be empty"

    def validate_arguments(self):
        assert len(self.storage_dir) > 0, "download-dir must not be empty"
        assert len(self.log_file) > 0, "log-file must not be empty"

    def check(self):
        ensure_dir_exists(self.storage_dir)


def set_logger(config: Config):
    global logger
    if config.log_to_stdout:
        log_to_std = sys.stdout
    else:
        log_to_std = sys.stderr

    logger = log.create_logger(name="PRIDE_Downloader",
                               log_dir=config.storage_dir,
                               filename=config.log_file,
                               verbose=config.verbose,
                               log_to_std=log_to_std)


def get_repo_link_list_project_files(repo_name: str):
    return PRIDE_API_LIST_PROJECT_FILES % repo_name


def get_project_files(project_name: str):
    """Get the project as a json and return it as a dataframe"""
    project_files_link = get_repo_link_list_project_files(project_name)
    logger.info("Requesting list of project files from " + project_files_link)
    response = requests.get(project_files_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_files_link, len(response.text)))
    response = json.loads(response.text)
    df = pd.DataFrame(pd.json_normalize(response['list']))
    logger.info("Received list of %d project files" % len(df))
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


def filter_files(files_df: pd.DataFrame, file_extensions: set = None):
    if file_extensions is None:
        file_extensions = set(DEFAULT_VALID_FILE_EXTENSIONS)
    else:
        assert type(file_extensions) == set, "file_extensions must be a set"

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


def download_files(links: list, max_num_files: int, skip_existing=True, extract=True, count_failed_files=False):
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


def download(pride_project: str,
             valid_file_extensions: set,
             max_num_files: int,
             download_dir: str,
             skip_existing: bool,
             extract: bool,
             count_failed_files: bool):
    project_files = get_project_files(project_name=pride_project)
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


def run_download(config: Config):
    return download(pride_project=config.pride_project,
                    valid_file_extensions=config.valid_file_extensions,
                    max_num_files=config.max_num_files,
                    download_dir=config.storage_dir,
                    skip_existing=config.skip_existing,
                    extract=config.extract,
                    count_failed_files=config.count_failed_files)


def validate_download(config: Config):
    config.require_pride_project()

    if len(config.valid_file_extensions) != 0 and config.max_num_files % len(config.valid_file_extensions) != 0:
        logger.info(
            "Warning: max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
            "files that belong together are also downloaded together")


def get_project_link_get_project_summary(project_name: str):
    return PRIDE_API_GET_PROJECT_SUMMARY % project_name


def get_project_summary(project_name: str):
    """Get the project as a json and return it as a dataframe"""
    project_summary_link = get_project_link_get_project_summary(project_name)
    logger.info("Requesting project summary from " + project_summary_link)
    response = requests.get(project_summary_link)
    logger.debug("Received response from %s with length of %d bytes" % (project_summary_link, len(response.text)))
    response = json.loads(response.text)
    logger.info("Received project summary for project \"%s\"" % project_name)
    return response


def pretty_print_json(dic: dict):
    return json.dumps(dic, indent=4)


def info(pride_project: str):
    summary_dict = get_project_summary(project_name=pride_project)
    print(pretty_print_json(summary_dict))


def run_info(config: Config):
    return info(pride_project=config.pride_project)


def validate_info(config: Config):
    config.require_pride_project()


COMMAND_DISPATCHER = {
    "download": {
        "handler": run_download,
        "validator": validate_info,
    },
    "info": {
        "handler": run_info,
        "validator": validate_info,
    }
}


def get_command_config(command: str):
    command_config = COMMAND_DISPATCHER.get(command)
    if command_config is None:
        raise NotImplementedError("%s is no known command")
    return command_config


def dispatch_commands(config: Config):
    command_configs = [get_command_config(command) for command in config.commands]

    try:
        for command_config in command_configs:
            command_config["validator"](config)
    except Exception as e:
        logger.error(e)
        return

    for command_config in command_configs:
        command_config["handler"](config)


def main(config: Config = None):
    if config is None:
        config = Config()
        config.parse_arguments()
    config.validate_arguments()
    config.check()
    set_logger(config)

    dispatch_commands(config)


if __name__ == '__main__':
    main()
