#!/usr/bin/env python3

from setuptools import setup,find_packages,find_namespace_packages
from mb_pandas.src.version import version

setup(
    name="mb_pandas",
    version=version,
    description="Basic Pandas functions package",
    author=["Malav Bateriwala"],
    packages=find_namespace_packages(include=["mb_pandas.*"]),
    #packages=find_packages(),
    scripts=['scripts/df_profile','scripts/df_view'],
    install_requires=[
        "numpy",
        "pandas",
        "colorama",],
    python_requires='>=3.8',)
