#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import os
import re
import sys

def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as fp:
        return fp.read()

m = re.search(r"^__version__ *= *\"([^\"]*)\" *$", read("tdclient/version.py"), re.MULTILINE)

if m is None:
    raise(RuntimeError("could not read tdclient/version.py"))
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

install_requires = []
with open("requirements.txt") as fp:
    for s in fp:
        install_requires.append(s.strip())

tests_require = []
with open("test-requirements.txt") as fp:
    for s in fp:
        tests_require.append(s.strip())

setup(
    name="td-client",
    version=version,
    description="Treasure Data API library for Python",
    long_description=read("README.md"),
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=install_requires,
    tests_require=tests_require,
    packages=find_packages(),
    cmdclass = {"test": PyTest},
    license="Apache Software License",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Internet",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
