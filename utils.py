import os


def ensure_dir_exists(dir):
    """Ensure the existence of given directories by creating them if non-existing."""
    if len(dir) == 0:
        return dir
    if not os.path.exists(dir) or not os.path.isdir(dir):
        os.makedirs(dir)
    return dir
