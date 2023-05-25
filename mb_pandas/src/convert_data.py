import ast
import pandas as pd


__all__ = ['convert_string_to_list']

def convert_string_to_list(df,logger =None):
    """
    Convert string to list data of a column
    
    Input:
        df (pd.DataFrame): pandas dataframe
        logger (logger): logger
    
    Output:
        df (pd.DataFrame): pandas dataframe
    """

    df_l = list(df['columns'])
    df_l2 = [ast.literal_eval(i) for i in range(len(df_l))]
    df2 = df.copy()
    df2['columns'] = df_l2
    return df2