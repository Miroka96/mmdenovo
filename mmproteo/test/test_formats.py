from mmproteo.utils import formats

MZML_FILE = "../../datasets/PXD010000/Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39.mzML"
MZID_FILE = "../../datasets/PXD010000/Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39_msgfplus.mzid"


def test_read_mzid():
    formats.read(MZID_FILE)


def test_merge_mzml_mzid_to_parquet():
    parquet_files = formats.merge_mzml_and_mzid_files_to_parquet(filenames=[MZML_FILE, MZID_FILE])
    assert len(parquet_files) > 0
