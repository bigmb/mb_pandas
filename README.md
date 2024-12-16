# mb_pandas

A Python package providing enhanced pandas functionality with async support and optimized operations.

## Features

- **Asynchronous DataFrame Loading**: Load large CSV and Parquet files efficiently using async I/O
- **Optimized DataFrame Merging**: Merge large DataFrames using chunking or Dask
- **Data Type Conversions**: Convert between string representations and Python objects
- **DataFrame Profiling**: Generate detailed profiling reports and comparisons
- **Data Transformation**: Various utilities for DataFrame transformations

## Installation

```bash
pip install mb_pandas
```

## Dependencies

- Python >= 3.8
- numpy
- pandas
- colorama

<!-- - tqdm (for progress bars)
- pyarrow (for parquet support)
- dask (for distributed computing)
- opencv-python (for image support)
- pandas-profiling (for profiling features)
- mb_utils (for logging utilities) -->

## Modules

### transform.py

Functions for DataFrame transformations and merging operations.

```python
from mb_pandas.transform import merge_chunk, merge_dask, check_null, remove_unnamed,rename_columns

# Merge large DataFrames in chunks
result = merge_chunk(df1, df2, chunksize=10000)

# Merge using Dask for distributed computing
result = merge_dask(df1, df2)

# Check and handle null values
df = check_null('data.csv', fillna=True)

# Remove unnamed columns
df = remove_unnamed(df)

# Rename column
df = rename_columns(data,'labels2','labels')
```

### dfload.py

Asynchronous DataFrame loading utilities.

```python
from mb_pandas import load_any_df

# Load any supported file format
df = load_any_df('data.csv')
df = load_any_df('data.parquet')

# Convert string columns to Python objects
df = load_any_df('data.csv', literal_ast_columns=['json_col'])
```

### aio.py

Asynchronous I/O utilities.

```python
from mb_pandas.aio import read_text, srun

# Read file asynchronously
content = await read_text('file.txt', context_vars={'async': True})

# Run async function synchronously
result = srun(async_function, *args)
```

### convert_data.py

Data type conversion utilities.

```python
from mb_pandas.convert_data import convert_string_to_list, convert_string_to_dict, convert_string_to_type

# Convert string representations to lists
df = convert_string_to_list(df, 'list_column')

# Convert string representations to dictionaries
df = convert_string_to_dict(df, 'dict_column')

# Convert strings to specific types
df = convert_string_to_type(df, 'number_column', int)
```

### profiler.py

DataFrame profiling and comparison utilities.

```python
from mb_pandas.profiler import create_profile, profile_compare

# Generate profiling report
create_profile(df, 'report.html', target=['target_column'])

# Compare two DataFrames
profile_compare(df1, df2, 'comparison.html')
```

## Key Functions

### merge_chunk(df1, df2, chunksize=10000)
Merge two DataFrames in chunks to handle large datasets efficiently.

### merge_dask(df1, df2)
Merge two DataFrames using Dask for improved performance with large datasets.

### load_any_df(file_path, show_progress=True)
Load DataFrames from various file formats with progress tracking.

### convert_string_to_list(df, column)
Convert string representations of lists in a DataFrame column to actual lists.

### create_profile(df, profile_name='report.html')
Generate a detailed profiling report for a DataFrame.

## Error Handling

All functions include comprehensive error handling with descriptive messages:

```python
try:
    df = load_any_df('data.csv')
except ValueError as e:
    print(f"Error loading file: {e}")
```

## Logging

Most functions accept an optional logger parameter for operation tracking:

```python
import logging
logger = logging.getLogger()
df = load_any_df('data.csv', logger=logger)
```

## Performance Tips

1. Use `merge_chunk` for large DataFrame merges that fit in memory
2. Use `merge_dask` for very large datasets that benefit from distributed computing
3. Enable `show_progress=True` to monitor long-running operations
4. Use `minimal=True` in profiling for large datasets
5. Consider sampling large datasets before profiling
