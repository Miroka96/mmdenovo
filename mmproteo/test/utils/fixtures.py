import tempfile
import shutil
import os

import pytest


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


@pytest.fixture(scope="session")
def run_with_datasets():
    current_working_dir = os.getcwd()
    os.chdir("../../datasets")
    os.system("mmproteo -p PXD010000 -t mzid,mzml -n 2 download")
    os.chdir(current_working_dir)
