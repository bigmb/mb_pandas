mb_pandas
=========

|forthebadge| |image1|

|Maintenance|

|Hits|

Personal addition to pandas package for faster and better performance of
data wrangling.

Main functions:

::

   1) pandas profile (from mb_pandas.src.profiler import create_profile)

   2) df load using async for larger datasets (from mb_pandas.src.dfload import load_any_df)

   3) df compare (from mb_pandas.src.profiler import profile_compare)

   4) df transformation for basic function (from mb_pandas.src.tranform import *)
   ['check_null','remove_unnamed','rename_columns','check_drop_duplicates','get_dftype'])

Scripts:

::

   1) df_profile - to create profile for any df. (default folder : /home/malav/pandas_profiles/)

Pip install :

::

   pip install mb-pandas==1.0.202211280400

.. |forthebadge| image:: https://forthebadge.com/images/badges/made-with-python.svg
   :target: https://www.python.org/
.. |image1| image:: https://forthebadge.com/images/badges/built-by-neckbeards.svg
   :target: https://github.com/bigmb
.. |Maintenance| image:: https://img.shields.io/badge/Maintained%3F-yes-green.svg
   :target: https://github.com/bigmb/mb_pandas/graphs/commit-activity
.. |Hits| image:: https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Fbigmb%2Fmb_pandas&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false
   :target: https://hits.seeyoufarm.com