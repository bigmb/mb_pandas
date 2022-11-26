##loading padnas dataframe from csv file using asyncio 

import pandas as pd
import os
import asyncio
from .aio import srun
from .csv import read_csv_asyn
#import tqdm

__all__ = ['load_df','load_df_async','load_any_df']

async def load_df_async(file_path, *args,show_progress=False,context_vars: dict = {}, **kwargs):
    """
    Load pandas dataframe from csv file
    """
    df_filepath = file_path.lower()
    df = await read_csv_asyn(df_filepath,*args,show_progress=show_progress,context_vars=context_vars, **kwargs)

    return df


def load_df(file_path, *args,show_progress=False,**kwargs):
    """
    Load pandas dataframe from csv file using asyncio
    Input: 
        file_path (csv): path to csv file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    return srun(load_df_async,file_path, *args, show_progress=show_progress,**kwargs)

def load_any_df(file_path, *args,show_progress=True,**kwargs):
    """
    Loading any pandas dfload function
    Input: 
        file_path (csv): path to csv file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    try:
        df = load_df(file_path, *args,show_progress=show_progress,**kwargs)
    except:
        df = pd.read_csv(file_path, *args,index_col=0,**kwargs)
        #df = pd.concat([chunk for chunk in tqdm(pd.read_csv(file_path), desc='Loading data')])

    return df