from mmproteo.utils import utils, commands, config, log
from .utils.defaults import *
from .utils.fixtures import *


def test_mz_pipeline(run_in_temp_directory):
    conf = config.Config()
    conf.pride_project = DEFAULT_PROJECT
    conf.valid_file_extensions = ["mzid", "mzml"]
    conf.max_num_files = 2
    conf.shown_columns = ["fileName", "fileSizeBytes"]
    conf.dummy_logger = True
    conf.verbose = True
    conf.commands = ["list", "download", "extract", "mz2parquet"]
    conf.check()
    logger = log.TestLogger(fail_early=False,
                            terminate_process=False,
                            verbose=True)

    commands.DISPATCHER.dispatch_commands(config=conf, logger=logger)

    assert os.path.isfile("Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39.mzML"), \
        "the mzml file should have been downloaded and extracted"
    assert os.path.isfile("Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39_msgfplus.mzid"), \
        "the mzid file should have been downloaded and extracted"
    assert os.path.isfile("Biodiversity_A_cryptum_FeTSB_anaerobic_1_01Jun16_Pippin_16-03-39_mzmlid.parquet"), \
        "the mzml file and the mzid file should have been merged"

    conf.clear_cache()
    conf.commands = ["download"]
    commands.DISPATCHER.dispatch_commands(config=conf, logger=logger)

    downloaded_files = utils.merge_column_values(conf.processed_files, [conf.default_downloaded_files_column])
    assert len(downloaded_files) == 0, "there should be no downloaded files in the second iteration"

    conf.clear_cache()
    conf.commands = ["extract"]
    commands.DISPATCHER.dispatch_commands(config=conf, logger=logger)

    extracted_files = utils.merge_column_values(conf.processed_files, [conf.default_extracted_files_column])
    assert len(extracted_files) == 0, "there should be no extracted files in the second iteration"

    conf.clear_cache()
    conf.commands = ["mz2parquet"]
    commands.DISPATCHER.dispatch_commands(config=conf, logger=logger)

    downloaded_and_extracted_files = utils.merge_column_values(conf.processed_files,
                                                               [conf.default_downloaded_files_column,
                                                                conf.default_extracted_files_column])
    assert len(downloaded_and_extracted_files) == 0, "there should be no converted files in the second iteration"
