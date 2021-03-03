import os
from typing import Any, Hashable, Iterable, List, Optional, Union

import numpy as np
import pandas as pd

from mmproteo.utils import log


def concat_set_of_options(options: Iterable[str], option_quote: str = '"', separator: str = ", ") -> str:
    if type(options) != list:
        options = sorted(options)
    return separator.join([option_quote + option + option_quote for option in options])


def deduplicate_list(lst: List[Hashable]) -> List[Hashable]:
    already_inserted = set()
    deduplicated_list = []
    for e in lst:
        if e not in already_inserted:
            already_inserted.add(e)
        deduplicated_list.append(e)
    return deduplicated_list


def _denumpyfy(element: Any) -> Any:
    if type(element) == np.int64:
        return int(element)
    if type(element) == np.float64:
        return float(element)
    if type(element) in [int, str, float]:
        return element
    if type(element) == dict:
        return {k: _denumpyfy(v) for k, v in element.items()}
    if type(element) == list:
        return [_denumpyfy(v) for v in element]
    if type(element) == set:
        return {_denumpyfy(v) for v in element}
    raise NotImplementedError(type(element))


def denumpyfy(element: Union[np.int64, np.float64, int, str, float, dict, list, set]) \
        -> Union[int, str, float, dict, list, set]:
    return _denumpyfy(element)


def ensure_dir_exists(directory: str, logger: log.Logger = log.DEFAULT_LOGGER) -> None:
    """Ensure the existence of a given directory (path) by creating it/them if they do not exist yet."""
    if len(directory) == 0:
        return
    try:
        os.makedirs(directory, exist_ok=True)
    except FileExistsError:
        logger.warning("'%s' already exists and is not a directory")


def extract_dict_or_inner_element(elem: Union[Iterable, Any]) -> Union[Iterable, Any]:
    try:
        # skip one-element lists or sets,...
        while type(elem) == list or type(elem) == set or type(elem) == tuple:
            if len(elem) == 1:
                elem = next(iter(elem))
                continue
            non_null_elements = [e for e in elem if e is not None]
            if len(non_null_elements) == 0:
                elem = None
                break
            if len(non_null_elements) == 1:
                elem = non_null_elements[0]
                continue
            break
    except Exception:
        pass
    return elem


def flatten_dict(input_dict: dict,
                 result_dict: dict = None,
                 overwrite: bool = False,
                 clean_keys: bool = True,
                 concat_keys: bool = True) -> dict:
    if result_dict is None:
        result_dict = dict()

    dict_queue = [("", input_dict)]

    while len(dict_queue) > 0:
        key_prefix, item = dict_queue[0]
        dict_queue = dict_queue[1:]

        for key, value in item.items():
            if clean_keys:
                key = str(key)
                key = key.replace(" ", "_")
                key = "".join([c for c in key if c.isalnum() or c == "_"])
            value = extract_dict_or_inner_element(value)

            if type(value) == dict:
                new_prefix = key + "__"
                dict_queue.append((new_prefix, value))
                continue

            if not overwrite and key not in result_dict:
                if concat_keys:
                    new_key = key_prefix + key
                else:
                    new_key = key
                result_dict[new_key] = value

    return result_dict


def is_docker_container_running(container_name: str,
                                docker_inspect_container_command_template: str =
                                "docker container inspect -f '{{.State.Status}}' {container_name} > /dev/null 2>&1") \
        -> bool:
    return_code = os.system(docker_inspect_container_command_template.format(container_name=container_name))
    return return_code == 0


def merge_column_values(df: Optional[pd.DataFrame], columns: List[str]) -> List[str]:
    if df is None:
        return list()

    values = set()

    for column in columns:
        if column in df.columns:
            values.update(df[column].dropna())

    if None in values:
        values.remove(None)

    return list(values)


def list_files_in_directory(directory_path: str) -> List[str]:
    paths_in_dir = [os.path.join(directory_path, element) for element in os.listdir(directory_path)]
    file_paths = [path for path in paths_in_dir if os.path.isfile(path)]
    return file_paths
