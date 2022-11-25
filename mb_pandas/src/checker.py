##Pandas file checker

import pandas as pd
import os
from .dfload import load_any_df
from mb_utils.src.logging import logger

__all__ = ['check_file']

def check_file(file_path,fillna=False,logger=logger) -> pd.DataFrame:
    """
    Pandas file checker. Checks Null values
    Input: 
        file_path (csv): path to csv file
        fillna (bool): fillna with False
    Output:
        df (pd.DataFrame): pandas dataframe
    """
    logger.info('Loading file: {}'.format(file_path))
    df = load_any_df(file_path)
    logger.info(f'Checking file: {file_path}')
    logger.info(f'File shape: {df.shape}')
    logger.info(f'File columns: {df.columns}')
    logger.info(f'File head: {df.head()}')
    logger.info('Checking Null values')
    for df_c in df.columns:
        if df_c.isnull().values.any():
            logger.warning(f'Column {df_c} has null values')
            logger.info(len(df[df[df_c].isnull()]))
            if fillna:
                if df[df_c].dtype == 'int':    
                    logger.info('Filling null values with 0')
                    df[df_c] = df[df_c].fillna(0)
                elif df[df_c].dtype == 'float':    
                    logger.info('Filling null values with 0.0')
                    df[df_c] = df[df_c].fillna(0.0)
                else:
                    logger.info('Going to next column')
    