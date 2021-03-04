import mmproteo.utils.filters
from mmproteo.utils import formats
from .utils.fixtures import *
from .utils.defaults import *


def test_read_mzid(run_with_datasets):
    formats.read(MZID_FILE_PATH)


def test_merge_mzml_mzid_to_parquet(run_with_datasets):
    if os.path.isfile(MZMLID_FILE_PATH):
        os.remove(MZMLID_FILE_PATH)
    parquet_files = formats.merge_mzml_and_mzid_files_to_parquet(filenames=[MZML_FILE_PATH, MZID_FILE_PATH])
    assert os.path.isfile(MZMLID_FILE_PATH)
    assert parquet_files == [MZMLID_FILE_PATH]
    os.remove(MZMLID_FILE_PATH)


def test_filter_files_list():
    filenames = [
        "abc.txt",
        "def.txt.gz",
        "ghi.gz"
    ]

    assert ["abc.txt"] == mmproteo.utils.filters.filter_files_list(filenames=filenames, max_num_files=1)
    assert ["abc.txt", "def.txt.gz"] == mmproteo.utils.filters.filter_files_list(filenames=filenames, file_extensions=["txt"])
    assert ["def.txt.gz", "ghi.gz"] == mmproteo.utils.filters.filter_files_list(filenames=filenames, file_extensions=["gz"])
    assert ["abc.txt", "def.txt.gz", "ghi.gz"] == mmproteo.utils.filters.filter_files_list(filenames=filenames,
                                                                                           file_extensions=["gz", "txt"])
