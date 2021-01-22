#!/usr/bin/python3

from mmproteo.utils import pride
import pandas as pd

SAMPLE_PROJECT = "PXD010000"


def store_info():
    with open("resources/" + SAMPLE_PROJECT + "_info.txt", "w") as file:
        file.write(pride.info(project_name=SAMPLE_PROJECT, api_versions=["1"]))


def test_info():
    with open("resources/" + SAMPLE_PROJECT + "_info.txt", "r") as file:
        expected_info = file.read()

    received_info = pride.info(project_name=SAMPLE_PROJECT, api_versions=["1"])
    assert expected_info == received_info


def store_project_files():
    pride.get_project_files(project_name=SAMPLE_PROJECT, api_versions=["1"])\
        .to_parquet("resources/" + SAMPLE_PROJECT + "_files.parquet")


def test_get_project_files():
    expected_files = pd.read_parquet("resources/" + SAMPLE_PROJECT + "_files.parquet")
    received_files = pride.get_project_files(project_name=SAMPLE_PROJECT, api_versions=["1"])
    pd._testing.assert_frame_equal(expected_files, received_files, check_exact=True)


def store_raw_project_files():
    pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"], file_extensions={"raw"})\
        .to_parquet("resources/" + SAMPLE_PROJECT + "_files_raw.parquet")


def store_mgf_mzid_project_files():
    pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"], file_extensions={"mgf", "mzid"})\
        .to_parquet("resources/" + SAMPLE_PROJECT + "_files_mgf_mzid.parquet")


def test_list_files():
    expected_files = pd.read_parquet("resources/" + SAMPLE_PROJECT + "_files.parquet")
    received_files = pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"])
    pd._testing.assert_frame_equal(expected_files, received_files, check_exact=True)


def test_list_files_raw():
    expected_files = pd.read_parquet("resources/" + SAMPLE_PROJECT + "_files_raw.parquet")
    received_files = pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"], file_extensions={"raw"})
    pd._testing.assert_frame_equal(expected_files, received_files, check_exact=True)


def test_list_files_mgf_mzid():
    expected_files = pd.read_parquet("resources/" + SAMPLE_PROJECT + "_files_mgf_mzid.parquet")
    received_files = pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"], file_extensions={"mgf", "mzid"})
    pd._testing.assert_frame_equal(expected_files, received_files, check_exact=True)


def test_list_files_gz():
    #expected_files = pd.read_parquet("resources/" + SAMPLE_PROJECT + "_files_mgf_mzid.parquet")
    received_files = pride.list_files(project_name=SAMPLE_PROJECT, api_versions=["1"], file_extensions={"gz"})
    #pd._testing.assert_frame_equal(expected_files, received_files, check_exact=True)


if __name__ == '__main__':
    store_info()
    store_project_files()
    store_raw_project_files()
    store_mgf_mzid_project_files()
