#!/usr/bin/env python

from __future__ import with_statement
import contextlib
import re

try:
  from setuptools import setup, find_packages
except ImportError:
  from distutils.core import setup

from pip.req import parse_requirements

install_requires = parse_requirements("requirements.txt")

setup(
  name="td-client",
  version="0.0.1",
  description="Treasure Data API library for Python",
  author="Treasure Data, Inc.",
  author_email="support@treasure-data.com",
  url="http://treasuredata.com/",
  install_requires=install_requires,
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
    "Topic :: Internet"
  ],
)
