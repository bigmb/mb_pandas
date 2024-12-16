"""
DataFrame profiling utilities.

This module provides functions for generating detailed profiling reports and comparisons
of pandas DataFrames using pandas-profiling.
"""

from typing import Union, List, Optional, Any
import pandas as pd
from pathlib import Path

__all__ = ['create_profile', 'profile_compare']

def create_profile(df: pd.DataFrame,
                  profile_name: Union[str, Path] = './pandas_profiling_report.html',
                  minimal: bool = False,
                  target: List[str] = None,
                  sample_size: int = 100000,
                  logger: Optional[Any] = None) -> None:
    """
    Create a pandas profiling report for a DataFrame.
    
    Args:
        df: Input DataFrame to profile
        profile_name: Output path for the HTML report
        minimal: If True, creates a minimal report for better performance
        target: List of target columns for correlation analysis
        sample_size: Maximum number of rows to process
        logger: Optional logger instance for logging operations
        
    Example:
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        >>> create_profile(df, 'report.html', target=['A'])
    """
    try:
        from pandas_profiling import ProfileReport
    except ImportError:
        raise ImportError(
            "pandas-profiling is required for this function. "
            "Install it with: pip install pandas-profiling"
        )
    
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    # Validate and process target columns
    target = target or []
    if target:
        missing_cols = set(target) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Target columns not found in DataFrame: {missing_cols}")
    
    # Sample large DataFrames
    original_size = len(df)
    if original_size > sample_size:
        if logger:
            logger.warning(f'DataFrame size ({original_size}) exceeds limit ({sample_size})')
            logger.info(f'Sampling {sample_size} rows')
        df = df.sample(n=sample_size, random_state=42)
        minimal = True
    
    try:
        if logger:
            logger.info('Generating profile report')
            if minimal:
                logger.info('Using minimal configuration for better performance')
        
        # Configure profile report
        profile = ProfileReport(
            df,
            title='Pandas Profiling Report',
            minimal=minimal,
            html={'style': {'full_width': True}},
            progress_bar=bool(logger),  # Show progress bar if logger is provided
            explorative=not minimal
        )
        
        # Set target columns for correlation analysis
        if target:
            if logger:
                logger.info(f'Analyzing correlations for target columns: {target}')
            profile.config.interactions.targets = target
        
        # Save report
        profile_path = Path(profile_name)
        profile.to_file(output_file=profile_path)
        
        if logger:
            logger.info(f'Profile report saved to: {profile_path.absolute()}')
    
    except Exception as e:
        if logger:
            logger.error(f'Error generating profile report: {str(e)}')
        raise

def profile_compare(df1: pd.DataFrame,
                   df2: pd.DataFrame,
                   profile_name: Union[str, Path] = './pandas_compare_report.html',
                   sample_size: int = 100000,
                   logger: Optional[Any] = None) -> None:
    """
    Create a comparison report between two DataFrames.
    
    Args:
        df1: First DataFrame to compare
        df2: Second DataFrame to compare
        profile_name: Output path for the HTML comparison report
        sample_size: Maximum number of rows to process per DataFrame
        logger: Optional logger instance for logging operations
        
    Raises:
        ImportError: If pandas-profiling is not installed
        TypeError: If inputs are not pandas DataFrames
        
    Example:
        >>> df1 = pd.DataFrame({'A': [1, 2, 3]})
        >>> df2 = pd.DataFrame({'A': [1, 2, 4]})
        >>> profile_compare(df1, df2, 'comparison.html')
    """
    try:
        from pandas_profiling import ProfileReport
    except ImportError:
        raise ImportError(
            "pandas-profiling is required for this function. "
            "Install it with: pip install pandas-profiling"
        )
    
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise TypeError("Both inputs must be pandas DataFrames")
    
    try:
        if logger:
            logger.info('Generating comparison report')
            logger.info(f'DataFrame 1 shape: {df1.shape}')
            logger.info(f'DataFrame 2 shape: {df2.shape}')
        
        # Generate profiles for both DataFrames
        profile1 = ProfileReport(
            df1.sample(n=min(len(df1), sample_size), random_state=42) if len(df1) > sample_size else df1,
            title="DataFrame 1",
            minimal=True,
            progress_bar=bool(logger)
        )
        
        profile2 = ProfileReport(
            df2.sample(n=min(len(df2), sample_size), random_state=42) if len(df2) > sample_size else df2,
            title="DataFrame 2",
            minimal=True,
            progress_bar=bool(logger)
        )
        
        # Generate comparison report
        if logger:
            logger.info('Comparing profiles')
        
        comparison = profile1.compare(profile2)
        comparison_path = Path(profile_name)
        comparison.to_file(comparison_path)
        
        if logger:
            logger.info(f'Comparison report saved to: {comparison_path.absolute()}')
    
    except Exception as e:
        if logger:
            logger.error(f'Error generating comparison report: {str(e)}')
        raise
