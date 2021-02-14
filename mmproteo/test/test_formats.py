from mmproteo.utils import formats

MZML_FILE = "../../datasets/PXD010000/Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39.mzML"
MZID_FILE = "../../datasets/PXD010000/Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39_msgfplus.mzid"


def test_read_mzid():
    formats.read(MZID_FILE)
