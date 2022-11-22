##loading padnas dataframe from csv file using asyncio 

import pandas as pd
import os
import asyncio
from aio import srun
from csv import read_csv_asyn

__all__ = ['load_df']

async def load_df_async(file_path, show_progress=False, context_vars={}):
    """
    Load pandas dataframe from csv file
    """
    df_filepath = file_path.lower()
    df = await read_csv_asyn(
            df_filepath,
            *args,
            show_progress=show_progress,
            context_vars=context_vars,
            **kwargs
        )

    return df


def load_df(file_path, show_progress=False) -> pd.DataFrame:
    """
    Load pandas dataframe from csv file using asyncio
    Input: 
        file_path (csv): path to csv file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    return srun(load_df_async,file_path, show_progress=show_progress)