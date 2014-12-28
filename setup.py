#!/usr/bin/env python

from __future__ import with_statement
from setuptools import setup, find_packages
import re

version = "0.0.1.dev.0"
with open("tdclient/version.py") as fp:
    m = re.search(r"^__version__ *= *\"([^\"]*)\" *$", fp.read(), re.MULTILINE)
    if m is not None:
        version = m.group(1)

setup(
    name="td-client",
    version=version,
    description="Treasure Data API library for Python",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=[
        "msgpack-python>=0.4,<0.5",
        "pytest>=2.6,<2.7",
        "pytest-cov>=1.8,<1.9",
        "tox>=1.8,<1.9",
    ],
    packages=find_packages(),
    test_suite="tdclient.test",
    license="Apache Software License",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Internet",
    ],
)
