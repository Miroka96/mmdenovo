import pandas as pd
import wget
import os
from mmpro.utils import log, formats

DUMMY_LOGGER = log.DummyLogger(send_welcome=False)


def create_file_extension_filter(required_file_extensions: set, optional_file_extensions: set = None):
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


def filter_files(files_df: pd.DataFrame, file_extensions: set = None, logger: log.Logger = DUMMY_LOGGER):
    assert file_extensions is None or type(file_extensions) == set, "file_extensions must be a set"

    if len(file_extensions) == 0:
        logger.debug("Skipping file extension filtering")
        filtered_files = files_df
    else:
        required_file_extensions = file_extensions
        optional_file_extensions = formats.get_extractable_file_extensions()

        logger.info("Filtering repository files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (
                        "\", \"".join(required_file_extensions),
                        "\", \"".join(optional_file_extensions)))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = files_df[files_df.fileName.apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    # sort, such that files with same prefixes but different extensions come in pairs
    sorted_files = filtered_files.sort_values(by='fileName')
    return sorted_files


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
        downloaded_filename = None

        file_extracted = False

        if skip_existing:
            log_message = 'Skipping download%s, because%s file "%s" already exists'
            if os.path.isfile(filename):
                logger.info(log_message % ('', '', filename))
                downloaded_filename = filename

            extracted_filename, extension = formats.separate_archive_extension(filename)
            file_is_extractable = len(extension) > 0

            if extract and file_is_extractable:
                # gunzip deletes the archive file after extraction, other programmes keep it
                if os.path.isfile(extracted_filename):
                    logger.info(log_message % (' and extraction', ' extracted', extracted_filename))
                    file_extracted = True

        # download the file if it wasn't downloaded yet and there was no extracted file available
        if downloaded_filename is None and not file_extracted:
            try:
                downloaded_filename = wget.download(link)
                files_downloaded_count += 1
                logger.info('Downloaded file %d/%d: "%s"' % (files_processed, max_num_files, link))
            except Exception as e:
                logger.info('Failed to download file %d/%d ("%s") because of "%s"' %
                            (files_processed, max_num_files, link, e))

        if downloaded_filename is not None and extract and not file_extracted:
            formats.extract_file_if_possible(downloaded_filename, skip_existing=skip_existing, logger=logger)

        if downloaded_filename is not None or count_failed_files:
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
