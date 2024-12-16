"""
Data conversion utilities for pandas DataFrames.

This module provides functions for converting data between different formats,
particularly focusing on string representations of complex data structures.
"""

from typing import Union, List, Optional, Any, Type
import ast
import pandas as pd
import json

__all__ = ['convert_string_to_list', 'convert_string_to_dict', 'convert_string_to_type']

def convert_string_to_list(df: pd.DataFrame,
                          column: str,
                          logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Convert string representations of lists in a DataFrame column to actual lists.
    
    Args:
        df: Input DataFrame
        column: Name of the column containing string representations of lists
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with the specified column converted to lists
        
    Example:
        >>> df = pd.DataFrame({'data': ['[1, 2, 3]', '[4, 5, 6]']})
        >>> result = convert_string_to_list(df, 'data')
        >>> print(result['data'].tolist())
        [[1, 2, 3], [4, 5, 6]]
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")
    
    # Create a copy to avoid modifying the original DataFrame
    result = df.copy()
    
    try:
        if logger:
            logger.info(f"Converting column '{column}' from string to list")
        
        # Convert strings to lists using ast.literal_eval
        result[column] = result[column].apply(lambda x: (
            ast.literal_eval(x) if isinstance(x, str) else
            x if isinstance(x, list) else
            [x] if pd.notnull(x) else []
        ))
        
        # Validate that all values are lists
        non_list_mask = ~result[column].apply(lambda x: isinstance(x, list))
        if non_list_mask.any():
            problematic_rows = result.index[non_list_mask].tolist()
            raise ValueError(f"Conversion failed for rows: {problematic_rows}")
        
        if logger:
            logger.info(f"Successfully converted {len(result)} rows")
        
        return result
    
    except Exception as e:
        if logger:
            logger.error(f"Error converting column '{column}': {str(e)}")
        raise

def convert_string_to_dict(df: pd.DataFrame,
                          column: str,
                          logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Convert string representations of dictionaries in a DataFrame column to actual dictionaries.
    
    Args:
        df: Input DataFrame
        column: Name of the column containing string representations of dictionaries
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with the specified column converted to dictionaries
        
    Example:
        >>> df = pd.DataFrame({'data': ['{"a": 1}', '{"b": 2}']})
        >>> result = convert_string_to_dict(df, 'data')
        >>> print(result['data'].tolist())
        [{'a': 1}, {'b': 2}]
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")
    
    result = df.copy()
    
    try:
        if logger:
            logger.info(f"Converting column '{column}' from string to dict")
        
        # Try both ast.literal_eval and json.loads for maximum compatibility
        def safe_convert(x):
            if isinstance(x, dict):
                return x
            if pd.isnull(x):
                return {}
            if not isinstance(x, str):
                return {'value': x}
            try:
                return ast.literal_eval(x)
            except (ValueError, SyntaxError):
                try:
                    return json.loads(x)
                except json.JSONDecodeError:
                    raise ValueError(f"Could not convert value: {x}")
        
        result[column] = result[column].apply(safe_convert)
        
        # Validate that all values are dictionaries
        non_dict_mask = ~result[column].apply(lambda x: isinstance(x, dict))
        if non_dict_mask.any():
            problematic_rows = result.index[non_dict_mask].tolist()
            raise ValueError(f"Conversion failed for rows: {problematic_rows}")
        
        if logger:
            logger.info(f"Successfully converted {len(result)} rows")
        
        return result
    
    except Exception as e:
        if logger:
            logger.error(f"Error converting column '{column}': {str(e)}")
        raise

def convert_string_to_type(df: pd.DataFrame,
                          column: str,
                          target_type: Type,
                          logger: Optional[Any] = None) -> pd.DataFrame:
    """
    Convert string values in a DataFrame column to a specified type.
    
    Args:
        df: Input DataFrame
        column: Name of the column to convert
        target_type: Type to convert values to (e.g., int, float, bool)
        logger: Optional logger instance for logging operations
        
    Returns:
        pd.DataFrame: DataFrame with the specified column converted to target type
        
    Example:
        >>> df = pd.DataFrame({'numbers': ['1', '2', '3']})
        >>> result = convert_string_to_type(df, 'numbers', int)
        >>> print(result['numbers'].tolist())
        [1, 2, 3]
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")
    
    result = df.copy()
    
    try:
        if logger:
            logger.info(f"Converting column '{column}' to {target_type.__name__}")
        
        def safe_convert(x):
            if pd.isnull(x):
                return None
            if isinstance(x, target_type):
                return x
            try:
                return target_type(x)
            except (ValueError, TypeError):
                raise ValueError(f"Could not convert value '{x}' to {target_type.__name__}")
        
        result[column] = result[column].apply(safe_convert)
        
        if logger:
            logger.info(f"Successfully converted {len(result)} rows")
        
        return result
    
    except Exception as e:
        if logger:
            logger.error(f"Error converting column '{column}': {str(e)}")
        raise Exception(f"Error converting column '{column}': {str(e)}")
