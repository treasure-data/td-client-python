#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import io
import re
import sys
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

version = None

def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ).read()

with open("src/tdclient/version.py") as fp:
    m = re.search(r"^__version__ *= *\"([^\"]*)\" *$", fp.read(), re.MULTILINE)
    if m is not None:
        version = m.group(1)

if version is None:
    raise(RuntimeError("could not read tdclient/version.py"))

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
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass = {"test": PyTest},
    license="Apache Software License",
    platforms="Posix; MacOS X; Windows",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
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
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
)
