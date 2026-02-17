"""
DataFrame loading module with async support.

This module provides utilities for loading pandas DataFrames from various file formats
using asynchronous I/O operations for improved performance.
"""

from typing import Optional, List, Union, Any
import pandas as pd
import asyncio
import concurrent.futures
import io
from ast import literal_eval
from mb.utils.logging import logg

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
                       chunk_size: int = 1024,
                       logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Load a DataFrame asynchronously from CSV or Parquet file.
    
    Args:
        filepath: Path to the input file
        chunk_size: Number of rows to read per chunk
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    def process_csv(data: io.StringIO) -> pd.DataFrame:
        dfs = []
        chunk_iter = pd.read_csv(data, chunksize=1024)
            
        for chunk in chunk_iter:
            dfs.append(chunk)
        return pd.concat(dfs, sort=False)
    
    def process_parquet(data: str) -> pd.DataFrame:
        try:
            # Try standard parquet reading first
            return pd.read_parquet(data)
        except Exception as e:
            # Fallback to pyarrow for problematic files
            logg.warning(f"Standard parquet reading failed for {data}, pyarrow fallback disabled",logger)
            raise ValueError(f"Failed to read parquet file {data}: {str(e)}")
    
    try:
        if filepath.endswith('.csv'):
            data = await read_txt(filepath)
            df = process_csv(io.StringIO(data))
        elif filepath.endswith('.parquet'):
            df = process_parquet(filepath)
        else:
            raise ValueError(f"Unsupported file format: {filepath}")

        return df
    
    except Exception as e:

        raise ValueError(f"Error loading file {filepath}: {str(e)}")

def load_any_df(file_path: Union[str, pd.DataFrame],
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
        logg.info(f"Loading DataFrame from {file_path}",logger=logger)
        
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Event loop already running (e.g. Jupyter notebook)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                df = executor.submit(asyncio.run, load_df_async(file_path)).result()
        else:
            df = asyncio.run(load_df_async(file_path))
        
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Convert specified columns using literal_eval
        if literal_ast_columns:
            for col in literal_ast_columns:
                if col not in df.columns:
                    raise KeyError(f"Column '{col}' not found in DataFrame")
                
                logg.info(f"Converting column '{col}' using literal_eval",logger=logger)
                
                try:
                    df[col] = df[col].apply(literal_eval)
                except (ValueError, SyntaxError) as e:
                    logg.error(f"Error converting column '{col}': {str(e)}",logger=logger)
                    raise ValueError(f"Error converting column '{col}' using literal_eval: {str(e)}")
        
        logg.info(f"Successfully loaded DataFrame with shape {df.shape}",logger=logger)
        
        return df
    
    except Exception as e:
        logg.error(f"Error loading file: {str(e)}",logger=logger)
        raise ValueError(f"Error loading file: {str(e)}")
