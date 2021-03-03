import tempfile
import shutil
import os

import pytest

from mmproteo.utils.config import Config
from .defaults import DEFAULT_PROJECT_DATASET_PATH, MZML_FILE_PATH, MZID_FILE_PATH


@pytest.fixture(scope="function")
def run_in_temp_directory():
    # before the test
    current_working_dir = os.getcwd()
    temporary_working_dir = tempfile.mkdtemp()
    os.chdir(temporary_working_dir)
    print(f"working in '{temporary_working_dir}'")

    yield  # run the test

    # after the test
    os.chdir(current_working_dir)
    shutil.rmtree(temporary_working_dir)
    print("removed temporary working directory")


@pytest.fixture(scope="session")
def run_with_datasets():
    if os.path.isfile(MZML_FILE_PATH) and os.path.isfile(MZID_FILE_PATH):
        return

    current_working_dir = os.getcwd()
    os.chdir(DEFAULT_PROJECT_DATASET_PATH)
    os.system("mmproteo -p PXD010000 -e mzid,mzml -n 2 download extract")
    os.remove(Config.default_log_file)
    os.chdir(current_working_dir)
