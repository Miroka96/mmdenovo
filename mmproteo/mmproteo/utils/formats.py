from typing import Optional, Set

from pyteomics import mgf, mzid
import pandas as pd
import json
import os
from mmproteo.utils import log, utils


def iter_entries(iterator, logger: log.Logger = log.DUMMY_LOGGER) -> list:
    logger.debug(type(iterator))
    entries = list(iterator)
    logger.debug("Length: %d" % len(entries))
    if len(entries) > 0:
        logger.debug("Example:")
        logger.debug()
        try:
            logger.debug(json.dumps(entries[0], indent=4))
        except TypeError:
            logger.debug(entries[0])
    return entries


def read_mgf(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    entries = iter_entries(mgf.read(filename), logger=logger)
    extracted_entries = [utils.flatten_dict(entry) for entry in entries]
    return pd.DataFrame(data=extracted_entries)


def extract_features_from_mzid_entry(entry: dict) -> dict:
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


def read(filename: str, logger: log.Logger = log.DUMMY_LOGGER) -> pd.DataFrame:
    if filename.endswith('.mgf'):
        df = read_mgf(filename, logger=logger)
    elif filename.endswith('.mzid'):
        df = read_mzid(filename, logger=logger)
    else:
        raise NotImplementedError
    return df


FILE_EXTRACTION_CONFIG = {
    "gz": {
        "command": 'gunzip "%s"'
    },
    "zip": {
        "command": 'unzip "%s"'
    }}


def get_extractable_file_extensions() -> Set[str]:
    return set(FILE_EXTRACTION_CONFIG.keys())


def separate_archive_extension(filename: str) -> (str, str):
    lower_filename = filename.lower()
    longest_extension = ""
    for ext in FILE_EXTRACTION_CONFIG.keys():
        if lower_filename.endswith(ext) and len(longest_extension) < len(ext):
            longest_extension = ext

    real_filename = filename[:len(filename) - len(longest_extension)]
    return real_filename, longest_extension


def extract_file_if_possible(filename: Optional[str],
                             skip_existing: bool = True,
                             logger: log.Logger = log.DUMMY_LOGGER) -> Optional[str]:
    if filename is None:
        return None

    _, file_ext = separate_archive_extension(filename.lower())
    if len(file_ext) > 0:
        return filename

    extraction_command = FILE_EXTRACTION_CONFIG[file_ext]["command"] % filename
    new_filename = filename[:-(len(file_ext) + 1)]

    if skip_existing and os.path.isfile(new_filename):
        logger.info('Skipping extraction, because "%s" already exists' % new_filename)
        return new_filename

    logger.info("Extracting downloaded file using '%s'" % extraction_command)
    return_code = os.system(extraction_command)
    if return_code == 0:
        logger.info("Extracted downloaded file")
    else:
        logger.info('Failed extracting downloaded file "%s" (return code = %d)' % (filename, return_code))

    return new_filename
