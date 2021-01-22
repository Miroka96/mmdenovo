# MMProteo

Mirko meets Proteomics




## Getting started

### Requirements

* Python 3.8 (other versions haven't been tested yet)
* preferably Linux (other OSes haven't been tested yet)

If you want, set up a virtual environment to encapsulate this software and its dependencies:

```
virtualenv --python=/usr/bin/python3 venv
source venv/bin/activate
```

### Installation
#### ...from Pip

`pip install mmproteo`

#### ...from Source Code

```
cd mmproteo
python setup.py install
```

#### ...as editable Development Installation from Source Code

```
cd mmproteo
pip install -e .
```

### Running MMProteo

The installations will make `mmproteo` available on your command line (and in your PATH),
so preferably use this anywhere you want.

Alternatively, you can run the `mmproteo-runner.py` script in the mmproteo directory.

If you are crazy, you can also run the `mmproteo/mmproteo/mmproteo.py` script, but this one is 
already part of the installed mmproteo package and shouldn't really be used directly.

### Usage

Simply entering `mmproteo` will already show you the available parameters.
Using `mmproteo --help` you can get a better image of the functionality.
Currently, the following parameters are available:

```
$ mmproteo -h
usage: mmproteo [-h] [-p PRIDE_PROJECT] [-n MAX_NUM_FILES]
                [--count-failed-files] [-d STORAGE_DIR] [-l LOG_FILE]
                [--log-to-stdout] [-t VALID_FILE_EXTENSIONS] [-e] [-x] [-v]
                [--shown-columns SHOWN_COLUMNS]
                {download,info,ls} [{download,info,ls} ...]

positional arguments:
  {download,info,ls}    the list of actions to be performed on the repository.
                        Every action can only occur once. Duplicates are
                        dropped after the first occurrence.

optional arguments:
  -h, --help            show this help message and exit
  -p PRIDE_PROJECT, --pride-project PRIDE_PROJECT
                        the name of the PRIDE project, e.g. 'PXD010000' from '
                        https://www.ebi.ac.uk/pride/ws/archive/peptide/list/pr
                        oject/PXD010000'.For some commands, this parameter is
                        required. (default: None)
  -n MAX_NUM_FILES, --max-num-files MAX_NUM_FILES
                        the maximum number of files to be downloaded. Set it
                        to '0' to download all files. (default: 0)
  --count-failed-files  Count failed files and do not just skip them. This is
                        relevant for the max-num-files parameter. (default:
                        True)
  -d STORAGE_DIR, --storage-dir STORAGE_DIR
                        the name of the directory, in which the downloaded
                        files and the log file will be stored. (default:
                        ./pride)
  -l LOG_FILE, --log-file LOG_FILE
                        the name of the log file, relative to the download
                        directory. (default: downloader.log)
  --log-to-stdout       Log to stdout instead of stderr. (default: False)
  -t VALID_FILE_EXTENSIONS, --valid-file-extensions VALID_FILE_EXTENSIONS
                        a list of comma-separated allowed file extensions to
                        filter files for. An empty list deactivates filtering.
                        Capitalization does not matter. (default: )
  -e, --no-skip-existing
                        Do not skip existing files. (default: False)
  -x, --no-extract      Do not try to extract downloaded files. (default:
                        False)
  -v, --verbose         Increase output verbosity to debug level. (default:
                        False)
  --shown-columns SHOWN_COLUMNS
                        a list of comma-separated column names. Some commands
                        show their results as tables, so their output columns
                        will be limited to those in this list. An empty list
                        deactivates filtering. Capitalization matters.
                        (default: )
```

There are multiple commands (positional arguments) as well as several optional arguments available.
A single command doesn't require or use all the available arguments. However, one can provide multiple
commands (separated by spaces), which will all use their share of the given arguments.

### Examples

#### Request Project Information for the Project PXD010000

```
$ mmproteo -p PXD010000 info
2021-01-21 23:02:14,035 - MMProteo: Logging to <stderr> and to file ./pride/downloader.log
2021-01-21 23:02:14,035 - MMProteo: Requesting project summary from https://www.ebi.ac.uk:443/pride/ws/archive/project/PXD010000
2021-01-21 23:02:15,847 - MMProteo: Received project summary for project "PXD010000"
{
    "accession": "PXD010000",
    "title": "DeNovo Peptide Identification Deep Learning Training Set",
    "projectDescription": "A benchmark set of bottom-up proteomics data for training deep learning networks. It has data from 51 organisms and includes nearly 1 million peptides.",
    "publicationDate": "2018-06-13",
    "submissionType": "COMPLETE",
    "numAssays": 235,
    "species": [
        "Delftia acidovorans (strain DSM 14801 / SPH-1)",
        "Francisella tularensis subsp. novicida (strain U112)",
        "Cupriavidus necator (strain ATCC 43291 / DSM 13513 / N-1) (Ralstonia eutropha)",
        "Rhizobium radiobacter (Agrobacterium tumefaciens) (Agrobacterium radiobacter)",
        "Shewanella oneidensis (strain MR-1)",
        "Bacteroides thetaiotaomicron (strain ATCC 29148 / DSM 2079 / NCTC 10582 / E50 / VPI-5482)",
        "Synechococcus elongatus (strain PCC 7942) (Anacystis nidulans R2)",
        "bacteria",
        "Micrococcus luteus (Micrococcus lysodeikticus)",
        "Methylomicrobium alcaliphilum (strain DSM 19304 / NCIMB 14124 / VKM B-2133 / 20Z)",
        "Mycobacterium smegmatis",
        "Rhodopseudomonas palustris",
        "Listeria monocytogenes serotype 1/2a (strain 10403S)",
        "Alcaligenes faecalis",
        "Legionella pneumophila",
        "Lactobacillus casei subsp. casei ATCC 393",
        "Acidiphilium cryptum (strain JF-5)",
        "Bacillus cereus (strain ATCC 14579 / DSM 31)",
        "Citrobacter freundii",
        "Chryseobacterium indologenes",
        "Myxococcus xanthus DZ2",
        "Fibrobacter succinogenes subsp. succinogenes S85",
        "Bacteroides fragilis (strain 638R)",
        "Rhodococcus sp. (strain RHA1)",
        "Clostridium ljungdahlii (strain ATCC 55383 / DSM 13528 / PETC)",
        "Coprococcus comes ATCC 27758",
        "Bacillus subtilis subsp. subtilis str. NCIB 3610",
        "Bacillus subtilis subsp. subtilis str. 168",
        "Anaerococcus hydrogenalis DSM 7454",
        "Algoriphagus marincola HL-49",
        "Stigmatella aurantiaca (strain DW4/3-1)",
        "Streptococcus agalactiae",
        "Bifidobacterium longum subsp. infantis (strain ATCC 15697 / DSM 20088 / JCM 1222 / NCTC 11817 / S12)",
        "Sulfobacillus thermosulfidooxidans",
        "Paenibacillus polymyxa ATCC 842",
        "Faecalibacterium prausnitzii SL3/3",
        "Cyanobacterium stanieri",
        "Cellvibrio gilvus (strain ATCC 13127 / NRRL B-14078)",
        "Dorea formicigenerans",
        "Streptomyces griseorubens",
        "Streptomyces sp.",
        "Bifidobacterium bifidum DSM 20456 = JCM 1255",
        "Paracoccus denitrificans",
        "Prevotella ruminicola (strain ATCC 19189 / JCM 8958 / 23)",
        "Campylobacter jejuni",
        "Ruminococcus gnavus ATCC 29149",
        "Pseudomonas putida KT2440",
        "Cellulophaga baltica 18"
    ],
    "tissues": [],
    "ptmNames": [
        "Oxidation"
    ],
    "instrumentNames": [
        "Q Exactive"
    ],
    "projectTags": [],
    "doi": "10.6019/PXD010000",
    "submitter": {
        "title": "Dr",
        "firstName": "Matthew",
        "lastName": "Monroe",
        "email": "matthew.monroe@pnnl.gov",
        "affiliation": "Pacific Northwest National Laboratory"
    },
    "labHeads": [
        {
            "title": "Dr",
            "firstName": "Samuel",
            "lastName": "Payne",
            "email": "samuel.payne@pnnl.gov",
            "affiliation": "Pacific Northwest National Laboratory"
        }
    ],
    "submissionDate": "2018-06-12",
    "reanalysis": "PXD005851",
    "experimentTypes": [
        "Shotgun proteomics"
    ],
    "quantificationMethods": [],
    "keywords": "machine learning, deep learning, bacterial diversity",
    "sampleProcessingProtocol": "Samples were digested with trypsin then analyzed by LC-MS/MS",
    "dataProcessingProtocol": "Data was searched with MSGF+ using PNNL's DMS Processing pipeline",
    "otherOmicsLink": null,
    "numProteins": 1494431,
    "numPeptides": 10324593,
    "numSpectra": 11774613,
    "numUniquePeptides": 7496530,
    "numIdentifiedSpectra": 0,
    "references": []
}

```

#### List all Information about 2 Files of Project PXD010000

```
$ mmproteo -p PXD010000 -n 2 ls
2021-01-21 23:06:08,280 - MMProteo: Logging to <stderr> and to file ./pride/downloader.log
2021-01-21 23:06:08,280 - MMProteo: Requesting list of project files from https://www.ebi.ac.uk/pride/ws/archive/file/list/project/PXD010000
2021-01-21 23:06:08,852 - MMProteo: Received list of 1175 project files
2021-01-21 23:06:08,852 - MMProteo: Showing only the top 2 entries because of the max_num_files parameter
  projectAccession assayAccession fileType fileSource   fileSize                                                                             fileName                                                                                                                                        downloadLink                                                                                                                                asperaDownloadLink
0        PXD010000          93137   RESULT  SUBMITTED   98085358  Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39_msgfplus.mzid.gz  ftp://ftp.pride.ebi.ac.uk/pride/data/archive/2018/06/PXD010000/Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39_msgfplus.mzid.gz  prd_ascp@fasp.ebi.ac.uk:pride/data/archive/2018/06/PXD010000/Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39_msgfplus.mzid.gz
1        PXD010000          93137     PEAK  SUBMITTED  340204025           Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39.mzML.gz           ftp://ftp.pride.ebi.ac.uk/pride/data/archive/2018/06/PXD010000/Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39.mzML.gz           prd_ascp@fasp.ebi.ac.uk:pride/data/archive/2018/06/PXD010000/Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39.mzML.gz
```

#### Show selected Information about 5 RAW or mzML files of Project PXD010000

```
$ mmproteo -p PXD010000 -n 5 --shown-columns "projectAccession,fileName,fileSize" -t "raw,mzml" ls
2021-01-21 23:13:57,067 - MMProteo: Logging to <stderr> and to file ./pride/downloader.log
2021-01-21 23:13:57,067 - MMProteo: Requesting list of project files from https://www.ebi.ac.uk/pride/ws/archive/file/list/project/PXD010000
2021-01-21 23:13:57,679 - MMProteo: Received list of 1175 project files
2021-01-21 23:13:57,679 - MMProteo: Filtering repository files based on the following required file extensions ["mzml", "raw"] and the following optional file extensions ["zip", "gz"]
2021-01-21 23:13:57,680 - MMProteo: Showing only the top 5 entries because of the max_num_files parameter
2021-01-21 23:13:57,681 - MMProteo: Limiting the shown columns according to the shown_columns parameter
   projectAccession                                                                    fileName    fileSize
1         PXD010000  Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39.mzML.gz   340204025
3         PXD010000      Biodiversity_S_thermosulf_FeYE_anaerobic_2_01Jun16_Pippin_16-03-39.raw   479591415
6         PXD010000                               Cj_media_MH_R5_23Feb15_Arwen_14-12-03.mzML.gz   578988435
8         PXD010000                                   Cj_media_MH_R5_23Feb15_Arwen_14-12-03.raw  1842140687
11        PXD010000                               Cj_media_MH_R4_23Feb15_Arwen_14-12-03.mzML.gz   698723177
```

Recognize, that the archive extension `.gz` of the mzML files as well as the capitalization of `mzML` 
is ignored while filtering the files. The same would state for the `.zip` extension. Other archive formats
are not supported yet.

## Tests

