##loading padnas dataframe from csv file using asyncio 

import pandas as pd
import os
import asyncio
import io
from tqdm import tqdm
from ast import literal_eval

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

def load_any_df(file_path,show_progress=True,literal_ast_columns=None ,logger = None):
    """
    Loading any pandas dfload function
    Input: 
        file_path (csv): path to csv file/parquet file
        show_progress (bool): show progress bar
        literal_ast_columns (list): columns to be converted to literal ast
        logger (logger): logger object
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
        raise ValueError("File type not supported")
    
    #df = df.reset_index(drop=True)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    if literal_ast_columns:
        for i in range(len(literal_ast_columns)):
            assert literal_ast_columns[i] in df.columns, f'{literal_ast_columns[i]} not in dataframe columns'
            if logger:
                logger.info("Converting {} to literal ast".format(literal_ast_columns[i]))
            df[literal_ast_columns[i]] = df[literal_ast_columns[i]].apply(lambda x: literal_eval(x))
    return df

