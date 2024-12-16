"""
DataFrame loading module with async support.

This module provides utilities for loading pandas DataFrames from various file formats
using asynchronous I/O operations for improved performance.
"""

from typing import Optional, List, Union, Any
import pandas as pd
import asyncio
import io
from tqdm import tqdm
from ast import literal_eval
from pyarrow.parquet import ParquetFile

__all__ = ['load_any_df']

async def read_txt(filepath: str, size: Optional[int] = None) -> str:
    """
    Asynchronously read text from a file.

    Args:
        filepath: Path to the file to read
        size: Optional number of bytes to read

    Returns:
        str: Content of the file
    """
    try:
        with open(filepath, mode="rt") as f:
            if size:
                return f.read(size)
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except IOError as e:
        raise IOError(f"Error reading file {filepath}: {str(e)}")

async def load_df_async(filepath: str, 
                       show_progress: bool = False,
                       chunk_size: int = 1024) -> pd.DataFrame:
    """
    Load a DataFrame asynchronously from CSV or Parquet file.
    
    Args:
        filepath: Path to the input file
        show_progress: Whether to show a progress bar
        chunk_size: Number of rows to read per chunk
        
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    def process_csv(data: io.StringIO, progress_bar: Optional[tqdm] = None) -> pd.DataFrame:
        dfs = []
        chunk_iter = pd.read_csv(data, chunksize=1024)
            
        for chunk in chunk_iter:
            dfs.append(chunk)
            if progress_bar:
                progress_bar.update(len(chunk))  
        if progress_bar:
            progress_bar.close()
        return pd.concat(dfs, sort=False)
    
    def process_parquet(data: str) -> pd.DataFrame:
        try:
            # Try standard parquet reading first
            return pd.read_parquet(data)
        except Exception:
            # Fallback to pyarrow for problematic files
            pf = ParquetFile(data)
            table = pf.read()
            return table.to_pandas()
    
    try:
        progress_bar = tqdm(unit='row', disable=not show_progress)
        
        if filepath.endswith('.csv'):
            data = await read_txt(filepath)
            df = process_csv(io.StringIO(data), progress_bar)
        elif filepath.endswith('.parquet'):
            df = process_parquet(filepath)
        else:
            raise ValueError(f"Unsupported file format: {filepath}")

        return df
    
    except Exception as e:
        if progress_bar:
            progress_bar.close()
        raise ValueError(f"Error loading file {filepath}: {str(e)}")

def load_any_df(file_path: Union[str, pd.DataFrame],
                show_progress: bool = True,
                literal_ast_columns: Optional[List[str]] = None,
                logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Load a DataFrame from various sources with support for type conversion.
    
    This function can load from CSV files, Parquet files, or accept an existing DataFrame.
    It supports asynchronous loading for better performance and can convert specified
    columns using ast.literal_eval.
    
    Args:
        file_path: Path to the file or an existing DataFrame
        show_progress: Whether to show a progress bar during loading
        literal_ast_columns: List of column names to convert using ast.literal_eval
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: Loaded and processed DataFrame
    """
    # Handle DataFrame input
    if isinstance(file_path, pd.DataFrame):
        return file_path
    
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string path or pandas DataFrame")
    
    try:
        # Load the DataFrame
        if logger:
            logger.info(f"Loading DataFrame from {file_path}")
        
        df = asyncio.run(load_df_async(file_path, show_progress=show_progress))
        
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Convert specified columns using literal_eval
        if literal_ast_columns:
            for col in literal_ast_columns:
                if col not in df.columns:
                    raise KeyError(f"Column '{col}' not found in DataFrame")
                
                if logger:
                    logger.info(f"Converting column '{col}' using literal_eval")
                
                try:
                    df[col] = df[col].apply(literal_eval)
                except (ValueError, SyntaxError) as e:
                    if logger:
                        logger.error(f"Error converting column '{col}': {str(e)}")
                    raise ValueError(f"Error converting column '{col}' using literal_eval: {str(e)}")
        
        if logger:
            logger.info(f"Successfully loaded DataFrame with shape {df.shape}")
        
        return df
    
    except Exception as e:
        if logger:
            logger.error(f"Error loading file: {str(e)}")
        raise ValueError(f"Error loading file: {str(e)}")
