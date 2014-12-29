#!/usr/bin/env python

from __future__ import with_statement
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import re
import sys

version = "0.0.1.dev.0"
with open("tdclient/version.py") as fp:
    m = re.search(r"^__version__ *= *\"([^\"]*)\" *$", fp.read(), re.MULTILINE)
    if m is not None:
        version = m.group(1)

class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name="td-client",
    version=version,
    description="Treasure Data API library for Python",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=[
        "msgpack-python>=0.4,<0.5",
    ],
    tests_require=[
        "coveralls>=0.5,<0.6",
        "pytest>=2.6,<2.7",
        "pytest-cov>=1.8,<1.9",
        "tox>=1.8,<1.9",
    ],
    packages=find_packages(),
    cmdclass = {"test": PyTest},
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
