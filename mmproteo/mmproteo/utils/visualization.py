import json
from urllib.parse import quote

from mmproteo.utils import log
from typing import Optional, List
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)


def urlencode_df_columns(files_df: pd.DataFrame, columns: List[str], inplace: bool = True) -> pd.DataFrame:
    if not inplace:
        files_df = files_df.copy()

    for col in columns:
        if col in files_df.columns:
            files_df[col] = files_df[col].map(lambda s: quote(s, safe='/:'))

    return files_df


def print_df(df: pd.DataFrame,
             max_num_files: Optional[int] = None,
             shown_columns: Optional[List[str]] = None,
             urlencode_columns: List[str] = None,
             logger: log.Logger = log.DUMMY_LOGGER):
    df = df.copy()

    if max_num_files is not None and max_num_files != 0:
        df = df[:max_num_files]
        logger.info("Showing only the top %d entries because of the max_num_files parameter" % max_num_files)
    if shown_columns is not None and shown_columns != []:
        columns = [col for col in shown_columns if col in df.columns]
        df = df[columns]
        logger.info("Limiting the shown columns according to the shown_columns parameter")

    if urlencode_columns is not None:
        df = urlencode_df_columns(df, urlencode_columns, inplace=False)

    try:
        print(df)
    except BrokenPipeError:
        pass


def pretty_print_json(dic: Optional[dict]) -> str:
    if dic is None:
        return ""
    return json.dumps(dic, indent=4)
