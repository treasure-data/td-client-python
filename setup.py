#!/usr/bin/env python

import os
import re
import sys

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as fp:
        return fp.read()


m = re.search(
    r"^__version__ *= *\"([^\"]*)\" *$", read("tdclient/version.py"), re.MULTILINE
)

if m is None:
    raise (RuntimeError("could not read tdclient/version.py"))
else:
    version = m.group(1)


class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name="td-client",
    version=version,
    description="Treasure Data API library for Python",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst; charset=UTF-8;",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    python_requires=">=3.5",
    install_requires=["msgpack>=0.6.2", "python-dateutil", "urllib3"],
    tests_require=["coveralls", "mock", "pytest", "pytest-cov", "tox"],
    extras_require={
        "dev": ["black==19.3b0", "isort", "flake8"],
        "docs": ["sphinx", "sphinx_rtd_theme"],
    },
    packages=find_packages(),
    cmdclass={"test": PyTest},
    license="Apache Software License",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Internet",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
