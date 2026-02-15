"""
Transform module for pandas DataFrame operations.

This module provides utilities for DataFrame transformations, merging, and data validation.
"""

from typing import Union, List, Optional, Any
import pandas as pd
import numpy as np
import cv2
from .dfload import load_any_df
from mb.utils.logging import logg

__all__ = ['check_null', 'remove_unnamed', 'rename_columns', 'check_drop_duplicates', 'get_dftype', 'merge_chunk', 'merge_dask']

def merge_chunk(df1: pd.DataFrame, 
                df2: pd.DataFrame, 
                chunksize: int = 10000, 
                logger: Optional[Any] = None,
                **kwargs) -> pd.DataFrame:
    """
    Merge two DataFrames in chunks to handle large datasets efficiently.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        chunksize: Number of rows per chunk for processing
        logger: Optional logger instance for logging operations
        **kwargs: Additional arguments passed to pd.merge
    
    Returns:
        pd.DataFrame: Merged DataFrame
        
    """
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise TypeError("Both inputs must be pandas DataFrames")
        
    # Optimize by using smaller DataFrame as base
    if df1.shape[0] > df2.shape[0]:
        df1, df2 = df2, df1

    merge_on = set(df1.columns) & set(df2.columns)
    if not merge_on:
        raise ValueError("No common columns to merge on")
    
    # Create chunks
    list_df = [df2[i:i+chunksize] for i in range(0, df2.shape[0], chunksize)]
    
    logg.info(f'Size of chunk: {chunksize}',logger=logger)
    logg.info(f'Number of chunks: {len(list_df)}',logger=logger)
    
    # Process first chunk
    result = pd.merge(df1, list_df[0], **kwargs)
    
    # Process remaining chunks
    for chunk in list_df[1:]:
        merged_chunk = pd.merge(df1, chunk, **kwargs)
        result = pd.concat([result, merged_chunk], ignore_index=True)
    
    return result

def merge_dask(df1: pd.DataFrame, 
               df2: pd.DataFrame, 
               logger: Optional[Any] = None,
               **kwargs) -> pd.DataFrame:
    """
    Merge two DataFrames using Dask for improved performance with large datasets.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        logger: Optional logger instance for logging operations
        **kwargs: Additional arguments passed to dd.merge
        
    Returns:
        pd.DataFrame: Merged DataFrame

    """
    try:
        import dask.dataframe as dd
    except ImportError:
        raise ImportError("dask is required for this function. Install it with: pip install dask")
    
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise TypeError("Both inputs must be pandas DataFrames")
    
    logg.info('Converting pandas DataFrames to Dask DataFrames',logger=logger)
    
    # Optimize number of partitions based on DataFrame size
    npartitions = max(2, min(32, df1.shape[0] // 100000))
    
    ddf1 = dd.from_pandas(df1, npartitions=npartitions)
    ddf2 = dd.from_pandas(df2, npartitions=npartitions)

    merged_ddf = dd.merge(ddf1, ddf2, **kwargs)
    
    logg.info('Computing merge operation',logger=logger)
    
    merged_df = merged_ddf.compute()
    
    logg.info('Merged DataFrame and converted back to pandas DataFrame',logger=logger)
    
    return merged_df

def check_null(file_path: str, 
               fillna: bool = False,
               logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Check and optionally handle null values in a DataFrame.
    
    Args:
        file_path: Path to the input file
        fillna: If True, fills null values based on column type
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: Processed DataFrame
        
    """
    logg.info(f'Loading file: {file_path}',logger=logger)
    
    df = load_any_df(file_path)
    
    logg.info(f'File shape: {df.shape}',logger=logger)
    logg.info(f'File columns: {list(df.columns)}',logger=logger)
    logg.info('Checking Null values',logger=logger)
    
    for column in df.columns:
        null_mask = df[column].isnull()
        null_count = null_mask.sum()
        
        if null_count > 0:
            logg.warning(f'Column {column} has {null_count} null values',logger=logger)
            
            if fillna:
                if pd.api.types.is_numeric_dtype(df[column]):
                    fill_value = 0 if pd.api.types.is_integer_dtype(df[column]) else 0.0
                    logg.info(f'Filling null values with {fill_value}',logger=logger)
                    df[column].fillna(fill_value, inplace=True)
                else:
                    logg.info(f'Skipping non-numeric column {column}',logger=logger)
    
    return df

def remove_unnamed(df: pd.DataFrame,
                  logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Remove unnamed columns from DataFrame.
    
    Args:
        df: Input DataFrame
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with unnamed columns removed
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
        
    logg.info('Removing unnamed columns',logger=logger)
    
    unnamed_pattern = '^Unnamed'
    unnamed_cols = df.columns[df.columns.str.contains(unnamed_pattern)].tolist()
    
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
        logg.info(f'Removed columns: {unnamed_cols}',logger=logger)
    
    return df

def rename_columns(df: pd.DataFrame,
                  new_column: str,
                  old_column: str,
                  logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Rename DataFrame columns.
    
    Args:
        df: Input DataFrame
        new_column: New column name
        old_column: Old column name to be renamed
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with renamed columns
        
    Raises:
        KeyError: If old_column doesn't exist in DataFrame
        TypeError: If input is not a pandas DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
        
    if old_column not in df.columns:
        logg.error(f'Column {old_column} not in DataFrame',logger=logger)
        raise KeyError(f'Column {old_column} not in DataFrame')
    
    df = df.rename(columns={old_column: new_column})
    
    logg.info(f'Column {old_column} renamed to {new_column}',logger=logger)
    
    return df

def check_drop_duplicates(df: pd.DataFrame,
                         columns: Union[str, List[str]],
                         drop: bool = False,
                         logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Check and optionally remove duplicate rows based on specified columns.
    
    Args:
        df: Input DataFrame
        columns: Column or list of columns to check for duplicates
        drop: If True, removes duplicate rows
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with duplicates optionally removed
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if isinstance(columns, str):
        columns = [columns]
    
    # Validate columns exist
    missing_cols = set(columns) - set(df.columns)
    if missing_cols:
        raise KeyError(f"Columns not found in DataFrame: {missing_cols}")
    
    logg.info(f'Checking duplicates for columns: {columns}',logger=logger)
    
    duplicates = df[df.duplicated(subset=columns, keep=False)]
    duplicate_count = len(duplicates)
    
    if duplicate_count > 0:
        logg.warning(f'Found {duplicate_count} duplicate rows',logger=logger)
        
        if drop:
            logg.info('Removing duplicates',logger=logger)
            df = df.drop_duplicates(subset=columns)
            logg.info(f'Removed {duplicate_count} duplicate rows',logger=logger)
        else:
            logg.info('Duplicate removal not requested (drop=False)',logger=logger)
    else:
        logg.info('No duplicates found',logger=logger)
    
    return df

def get_dftype(s: pd.Series) -> str:
    """
    Detect the data type of a pandas Series.
    
    This function determines whether a series contains special types like ndarrays,
    sparse arrays, images, or standard types like strings and timestamps.
    
    Args:
        s: Input pandas Series
        
    Returns:
        str: Detected type as string. Possible values include:
            - 'json': For lists and dictionaries
            - 'ndarray': For numpy arrays
            - 'SparseNdarray': For sparse numpy arrays
            - 'Image': For OpenCV images
            - 'str': For string data
            - 'Timestamp': For datetime data
            - 'Timedelta': For time duration data
            - 'object': For mixed or unknown types
            - 'none': For empty series
            - Other pandas dtypes as strings
    """
    if not isinstance(s, pd.Series):
        raise TypeError("Input must be a pandas Series")
        
    if len(s) == 0:
        return 'object'

    # Fast path for simple types
    if pd.api.types.is_numeric_dtype(s.dtype):
        return str(s.dtype)
    
    # Check first non-null value for type inference
    first_valid = s.first_valid_index()
    if first_valid is not None:
        val = s[first_valid]
        
        if isinstance(val, str):
            return 'str' if s.apply(lambda x: isinstance(x, str)).all() else 'object'
        elif isinstance(val, (list, dict)):
            return 'json' if s.apply(lambda x: isinstance(x, (list, dict))).all() else 'object'
        elif isinstance(val, np.ndarray):
            return 'ndarray' if s.apply(lambda x: isinstance(x, np.ndarray)).all() else 'object'
        elif isinstance(val, np.SparseNdarray):
            return 'SparseNdarray' if s.apply(lambda x: isinstance(x, np.SparseNdarray)).all() else 'object'
        elif isinstance(val, cv2.Image):
            return 'Image' if s.apply(lambda x: isinstance(x, cv2.Image)).all() else 'object'
        elif isinstance(val, pd.Timestamp):
            return 'Timestamp' if s.apply(lambda x: isinstance(x, pd.Timestamp)).all() else 'object'
        elif isinstance(val, pd.Timedelta):
            return 'Timedelta' if s.apply(lambda x: isinstance(x, pd.Timedelta)).all() else 'object'
    
    # Check if all values are numeric
    try:
        pd.to_numeric(s, errors='raise')
        return 'float64'
    except (ValueError, TypeError):
        pass
    
    return 'object'
