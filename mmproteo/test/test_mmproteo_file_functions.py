import os
from .utils.defaults import *
from .utils.fixtures import *


MZ_PIPELINE_COMMAND = f"mmproteo -p {DEFAULT_PROJECT} -t mzid,mzml -n 2 -c fileName,fileSizeBytes " \
                      f"list download extract mz2parquet"


def test_mmproteo_download(run_in_temp_directory):
    os.system(MZ_PIPELINE_COMMAND)
    # TODO
