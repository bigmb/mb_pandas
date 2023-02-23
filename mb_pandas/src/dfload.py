##loading padnas dataframe from csv file using asyncio 

import pandas as pd
import os
import asyncio
import io
from tqdm import tqdm

__all__ = ['load_df_new','load_any_df']


async def read_txt(filepath,size :int =None):     
    with open(filepath, mode="rt") as f: 
        return f.read(size)

async def load_df_new(fp,show_progress=False): 
    """
    load pandas dataframe from csv file using asyncio
    Input:
        fp (str): path to csv file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    def process(fp,data1: io.StringIO):
        if show_progress:
            bar = tqdm(unit='row') 
        dfs=[] 
        for df in pd.read_csv(data1,chunksize=1024): 
            dfs.append(df) 
            if show_progress:
                bar.update(len(df))
        df = pd.concat(dfs,sort=False) 
        if show_progress:
            bar.close()
        return df 
    data1 = await read_txt(fp) 
    return process(fp , io.StringIO(data1)) 

async def load_df_new_parquet(fp,show_progress=False): 
    """
    load pandas dataframe from csv file using asyncio
    Input:
        fp (str): path to csv file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    def process(fp,data1: io.StringIO):
        if show_progress:
            bar = tqdm(unit='row') 
        dfs=[] 
        for df in pd.read_parquet(data1,chunksize=1024): 
            dfs.append(df) 
            if show_progress:
                bar.update(len(df))
        df = pd.concat(dfs,sort=False) 
        if show_progress:
            bar.close()
        return df 
    data1 = await read_txt(fp) 
    return process(fp , io.StringIO(data1)) 

def load_any_df(file_path,show_progress=True,logger = None):
    """
    Loading any pandas dfload function
    Input: 
        file_path (csv): path to csv file/parquet file
        show_progress (bool): show progress bar
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    try:
        if file_path.endswith('.csv'):
            df = asyncio.run(load_df_new(file_path,show_progress=show_progress))
        elif file_path.endswith('.parquet'):
            df = asyncio.run(load_df_new_parquet(file_path,show_progress=show_progress))
        if logger:
            logger.info("Loaded dataframe from {} using asyncio".format(file_path))
    except:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, index_col=0)
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        else:
            raise ValueError("File type not supported")
        if logger:
            logger.info("Loaded dataframe from {} using pandas".format(file_path))
    return df

