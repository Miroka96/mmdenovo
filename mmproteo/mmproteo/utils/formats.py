import time
from typing import Optional, Set, Callable, Union, Any, Dict, List

from pyteomics import mgf, mzid
import pandas as pd
import os

from pyteomics.mgf import MGFBase
from pyteomics.mzid import MzIdentML

from mmproteo.utils import log, utils, visualization


def iter_entries(iterator: Union[MGFBase, MzIdentML], logger: log.Logger = log.DUMMY_LOGGER) -> List[Dict[str, Any]]:
    logger.debug(type(iterator))
    entries = list(iterator)
    logger.debug("Length: %d" % len(entries))
    if len(entries) > 0:
        logger.debug("Example:")
        logger.debug()
        try:
            logger.debug(visualization.pretty_print_json(entries[0]))
        except TypeError:
            logger.debug(entries[0])
    return entries


def read_mgf(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mgf.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def extract_features_from_mzid_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    result = utils.flatten_dict(entry)

    try:
        utils.flatten_dict(result.pop('SpectrumIdentificationItem')[0], result)
    except KeyError:
        pass

    try:
        utils.flatten_dict(result.pop('PeptideEvidenceRef')[0], result)
    except KeyError:
        pass

    return result


def read_mzid(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mzid.read(filename), logger=logger)
    extracted_entries = [extract_features_from_mzid_entry(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def read_parquet(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    return pd.read_parquet(filename)


FILE_READING_CONFIG: Dict[str, Callable[[str, log.Logger], pd.DataFrame]] = {
    "mgf": read_mgf,
    "mzid": read_mzid,
    "parquet": read_parquet,
}


def get_readable_file_extensions() -> Set[str]:
    return set(FILE_READING_CONFIG.keys())


def read(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    _, ext = separate_extension(filename=filename, extensions=get_readable_file_extensions())

    if len(ext) > 0:
        logger.debug("Started reading %s file '%s'" % (ext, filename))
        df = FILE_READING_CONFIG[ext](filename, logger)
        logger.debug("Finished reading %s file '%s'" % (ext, filename))
    else:
        raise NotImplementedError
    return df


FILE_EXTRACTION_CONFIG: Dict[str, Dict[str, str]] = {
    "gz": {
        "command": 'gunzip "%s"'
    },
    "zip": {
        "command": 'unzip "%s"'
    },
}


def get_extractable_file_extensions() -> Set[str]:
    return set(FILE_EXTRACTION_CONFIG.keys())


def get_string_of_extractable_file_extensions(extension_quote: str = '"', separator: str = ", ") -> str:
    return separator.join([extension_quote + ext + extension_quote for ext in get_extractable_file_extensions()])


def separate_extension(filename: str, extensions: Set[str]) -> (str, str):
    lower_filename = filename.lower()
    longest_extension = ""
    for ext in extensions:
        if lower_filename.endswith("." + ext) and len(longest_extension) < len(ext):
            longest_extension = ext

    real_filename = filename[:len(filename) - len(longest_extension) - len(".")]
    return real_filename, longest_extension


def extract_file_if_possible(filename: Optional[str],
                             skip_existing: bool = True,
                             logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    extracted_filename, file_ext = separate_extension(filename.lower(), get_extractable_file_extensions())
    if len(file_ext) == 0:
        logger.debug("Cannot extract file '%s', unknown extension" % extracted_filename)
        return filename

    extraction_command = FILE_EXTRACTION_CONFIG[file_ext]["command"] % filename

    if skip_existing and os.path.isfile(extracted_filename):
        logger.info('Skipping extraction, because "%s" already exists' % extracted_filename)
        return extracted_filename

    logger.info("Extracting file using '%s'" % extraction_command)
    return_code = os.system(extraction_command)
    if return_code == 0:
        logger.info("Extracted file")
    else:
        logger.warning('Failed extracting file "%s" (return code = %d)' % (filename, return_code))

    return extracted_filename


def create_file_extension_filter(required_file_extensions: Set[str],
                                 optional_file_extensions: Optional[Set[str]] = None) -> Callable[[str], bool]:
    if optional_file_extensions is None:
        file_extensions = set(required_file_extensions)
    else:
        file_extensions = {required_extension + "." + optional_extension
                           for required_extension in required_file_extensions
                           for optional_extension in optional_file_extensions}
        file_extensions.update(required_file_extensions)  # add all

    def filter_file_extension(filename: str) -> bool:
        return filename.lower().endswith(tuple(file_extensions))

    return filter_file_extension


def extract_files(filenames: List[Optional[str]],
                  skip_existing: bool = True,
                  logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [extract_file_if_possible(filename, skip_existing=skip_existing, logger=logger)
            for filename in filenames]


def filter_files_df(files_df: Optional[pd.DataFrame],
                    file_name_column: str = "fileName",
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
        logger.assert_true(file_name_column in files_df.columns, "Could not find '%s' column in files_df columns" %
                           file_name_column)
        required_file_extensions = file_extensions
        optional_file_extensions = get_extractable_file_extensions()

        required_file_extensions_list_str = "\", \"".join(required_file_extensions)
        optional_file_extensions_list_str = "\", \"".join(optional_file_extensions)
        if len(optional_file_extensions_list_str) > 0:
            optional_file_extensions_list_str = '"' + optional_file_extensions_list_str + '"'
        logger.info("Filtering files based on the following required file extensions [\"%s\"] and the "
                    "following optional file extensions [%s]" % (required_file_extensions_list_str,
                                                                 optional_file_extensions_list_str))

        file_extension_filter = create_file_extension_filter(required_file_extensions, optional_file_extensions)
        filtered_files = files_df[files_df[file_name_column].apply(file_extension_filter)]

        logger.debug("File extension filtering resulted in %d valid file names" % len(filtered_files))

    if sort:
        # sort, such that files with same prefixes but different extensions come in pairs
        sorted_files = filtered_files.sort_values(by=file_name_column)
    else:
        sorted_files = filtered_files

    if max_num_files is None or max_num_files == 0:
        limited_files = sorted_files
    else:
        limited_files = sorted_files[:max_num_files]

    return limited_files


def filter_files_list(filenames: List[Optional[str]],
                      file_extensions: Optional[Set[str]] = None,
                      max_num_files: Optional[int] = None,
                      sort: bool = True,
                      drop_duplicates: bool = True,
                      logger: log.Logger = log.DUMMY_LOGGER) -> List[str]:
    filenames = [filename for filename in filenames if filename is not None]
    df = pd.DataFrame(data=filenames, columns=["fileName"])
    if drop_duplicates:
        df = df.drop_duplicates()
    filtered_df = filter_files_df(files_df=df,
                                  file_name_column="fileName",
                                  file_extensions=file_extensions,
                                  max_num_files=max_num_files,
                                  sort=sort,
                                  logger=logger)
    return filtered_df["fileName"].to_list()


def start_thermo_docker_container(storage_dir: str = ".",
                                  thermo_docker_container_name: str = "thermorawfileparser",
                                  thermo_docker_image: str = "quay.io/biocontainers/thermorawfileparser:1.2.3--1",
                                  thermo_start_container_command_template: str =
                                  "docker run --rm -w /data -v %s:/data --name %s -d %s tail -f /dev/null",
                                  logger: log.Logger = log.DUMMY_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"
    command = thermo_start_container_command_template % (storage_dir, thermo_docker_container_name, thermo_docker_image)

    logger.debug("Starting %s using '%s'" % (subject, command))
    return_code = os.system(command=command)  # TODO prevent command injection, e.g. disable shell
    logger.assert_true(return_code == 0, "Failed to start " + subject)

    time.sleep(1)
    logger.assert_true(condition=utils.is_docker_container_running(thermo_docker_container_name),
                       error_msg=subject + " doesn't seem to be running")
    logger.info("Started " + subject)


def stop_thermo_docker_container(thermo_docker_container_name: str = "thermorawfileparser",
                                 thermo_stop_container_command_template: str = "docker stop %s",
                                 logger: log.Logger = log.DUMMY_LOGGER) -> None:
    subject = "ThermoRawFileParser Docker container"
    command = thermo_stop_container_command_template % thermo_docker_container_name

    logger.debug("Stopping %s using '%s'" % (subject, command))
    os.system(command=command)  # TODO prevent command injection, e.g. disable shell
    logger.assert_true(condition=not utils.is_docker_container_running(thermo_docker_container_name),
                       error_msg=subject + " still seems to be running")
    logger.info("Stopped " + subject)


_THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS = {
    "mgf": 0,
    "mzml": 1,
    "imzml": 2,
    "parquet": 3,
}


def get_thermo_raw_file_parser_output_formats() -> Set[str]:
    return set(_THERMO_RAW_FILE_PARSER_OUTPUT_FORMAT_IDS.keys())


def convert_raw_file_to_mgf(filename: Optional[str],
                            skip_existing: bool = True,
                            thermo_docker_container_name: str = "thermorawfileparser",
                            thermo_exec_command: str = "docker exec -it %s ThermoRawFileParser -f 0 -i %s",  # TODO find correct exec command
                            logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    converted_filename, file_ext = separate_extension(filename=filename.lower(),
                                                      extensions=set("raw"))
    if len(file_ext) == 0:
        logger.debug("Cannot convert file '%s', unknown extension" % converted_filename)
        return filename

    if skip_existing and os.path.isfile(converted_filename):
        logger.info('Skipping extraction, because "%s" already exists' % converted_filename)
        return converted_filename

    logger.assert_true(utils.is_docker_container_running(thermo_docker_container_name),
                       "There is no running ThermoRawFileParser Docker container with the name " +
                       thermo_docker_container_name)

    command = thermo_exec_command % (thermo_docker_container_name, filename)

    logger.info("Converting file using '%s'" % command)
    return_code = os.system(command)
    if return_code == 0:
        logger.info("Converted file")
    else:
        logger.warning('Failed converting file "%s" (return code = %d)' % (filename, return_code))

    return converted_filename


def convert_raw_files_to_mgf(filenames: List[Optional[str]],
                             skip_existing: bool = True,
                             logger: log.Logger = log.DUMMY_LOGGER) -> List[Optional[str]]:
    return [extract_file_if_possible(filename, skip_existing=skip_existing, logger=logger)
            for filename in filenames]
