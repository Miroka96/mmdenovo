import json

from mmproteo.utils import log
from typing import Optional, List
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)


def print_df(df: pd.DataFrame,
             max_num_files: Optional[int] = None,
             shown_columns: Optional[List[str]] = None,
             logger: log.Logger = log.DUMMY_LOGGER):
    if max_num_files is not None and max_num_files != 0:
        df = df[:max_num_files]
        logger.info("Showing only the top %d entries because of the max_num_files parameter" % max_num_files)
    if shown_columns is not None and shown_columns != []:
        columns = [col for col in shown_columns if col in df.columns]
        df = df[columns]
        logger.info("Limiting the shown columns according to the shown_columns parameter")
    print(df)


def pretty_print_json(dic: dict) -> str:
    return json.dumps(dic, indent=4)