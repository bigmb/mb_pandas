# mb_pandas

[![forthebadge](https://forthebadge.com/images/badges/made-with-python.svg)](https://www.python.org/)
[![forthebadge](https://forthebadge.com/images/badges/built-by-neckbeards.svg)](https://github.com/bigmb)

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/bigmb/mb_pandas/graphs/commit-activity)

Personal addition to pandas package for faster and better performance of data wrangling. 

Main functions:

    1) pandas profile (from mb_pandas.src.profiler import create_profile)

    2) df load using async for larger datasets (from mb_pandas.src.dfload import load_any_df)

    3) df compare (from mb_pandas.src.profiler import profile_compare)

    4) df transformation for basic function (from mb_pandas.src.tranform import *)
    ['check_null','remove_unnamed','rename_columns','check_drop_duplicates','get_dftype'])

Scripts:

    1) df_profile - to create profile for any df. (default folder : /home/malav/pandas_profiles/)