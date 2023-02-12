import ast
from mb_utils.src.logging import logger
import pandas as pd


__all__ = ['convert_string_to_list']

def convert_string_to_list(df,column,logger =None):
    """
    Input
    df (pd.DataFrame): pandas dataframe
    column (str): column name
    logger (logger): logger
    
    OutPut:
        df (pd.DataFrame): pandas dataframe
    """

    df_l = list(df['columns'])
    df_l2 = [ast.literal_eval(i) for i in range(len(df_l))]