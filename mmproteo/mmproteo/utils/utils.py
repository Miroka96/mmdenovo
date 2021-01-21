import os


def ensure_dir_exists(directory):
    """Ensure the existence of a given directory (path) by creating it/them if they do not exist yet."""
    if len(directory) == 0:
        return directory
    if not os.path.exists(directory) or not os.path.isdir(directory):
        os.makedirs(directory)
    return directory


def deduplicate_list(lst: list) -> list:
    already_inserted = set()
    deduplicated_list = []
    for e in lst:
        if e not in already_inserted:
            already_inserted.add(e)
        deduplicated_list.append(e)
    return deduplicated_list


def extract_dict_or_inner_element(elem):
    try:
        # skip one-element lists or sets,...
        while type(elem) != dict and len(elem) == 1:
            elem = next(iter(elem))
    except:
        pass
    return elem


def flatten_dict(input_dict: dict, result_dict: dict = None, overwrite: bool = False) -> dict:
    if result_dict is None:
        result_dict = dict()

    dict_queue = [input_dict]

    while len(dict_queue) > 0:
        item = dict_queue[0]
        dict_queue = dict_queue[1:]

        for key, value in item.items():
            value = extract_dict_or_inner_element(value)

            if type(value) == dict:
                dict_queue.append(value)
                continue

            if not overwrite and key not in result_dict:
                result_dict[key] = value

    return result_dict
