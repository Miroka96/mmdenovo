import os
import json


def ensure_dir_exists(dir):
    """Ensure the existence of given directories by creating them if non-existing."""
    if len(dir) == 0:
        return dir
    if not os.path.exists(dir) or not os.path.isdir(dir):
        os.makedirs(dir)
    return dir


def deduplicate_list(lst: list) -> list:
    already_inserted = set()
    deduplicated_list = []
    for e in lst:
        if e not in already_inserted:
            already_inserted.add(e)
        deduplicated_list.append(e)
    return deduplicated_list


def pretty_print_json(dic: dict) -> str:
    return json.dumps(dic, indent=4)


def flatten_dict(d: dict, result: dict = None, overwrite: bool = False) -> dict:
	if result is None:
		result = dict()
	if type(d) != dict:
		return result
	for key, value in d.items():
		if type(value) == dict:
			flatten_dict(value, result, overwrite)
			continue
		if not overwrite and key not in result:
			result[key] = value
	return result

