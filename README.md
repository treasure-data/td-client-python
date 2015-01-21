# Treasure Data API library for Python

[![Build Status](https://travis-ci.org/yyuu/td-client-python.svg)](https://travis-ci.org/yyuu/td-client-python)
[![Coverage Status](https://coveralls.io/repos/yyuu/td-client-python/badge.svg)](https://coveralls.io/r/yyuu/td-client-python)
[![PyPI version](https://badge.fury.io/py/td-client.svg)](http://badge.fury.io/py/td-client)

Treasure Data API library for Python

## Requirements

`td-client` supports the following versions of Python.

* Python 2.7+
* Python 3.3+
* PyPy

## Install

You can install the releases from [PyPI](https://pypi.python.org/).

```sh
$ pip install td-client
```

It'd be better to install [certifi](https://pypi.python.org/pypi/certifi) to enable SSL certificate verification.

```sh
$ pip install certifi
```

## Examples

Please see also the examples at [Treasure Data Documentation](http://docs.treasuredata.com/articles/rest-api-python-client).

### Listing jobs

TreasureData API key will be read from environment variable `TD_API_KEY`, if none is given via arguments to `tdclient.Client`.

```python
#!/usr/bin/env python

from contextlib import closing
import os
import sys
import tdclient

with closing(tdclient.Client()) as td:
    for job in td.jobs():
        print(job.job_id)
```

## Development

### Running tests

Run tests.

```sh
$ python setup.py test
```

### Running tests (tox)

You can run tests against all supported Python versions. I'd recommend you to install [pyenv](https://github.com/yyuu/pyenv) to manage Pythons.

```sh
$ pyenv shell system
$ for version in $(cat .python-version); do [ -d "$(pyenv root)/versions/${version}" ] || pyenv install "${version}"; done
$ pyenv shell --unset
```

Install [tox](https://pypi.python.org/pypi/tox).

```sh
$ pip install tox
```

Then, run `tox`.

```sh
$ tox
```

### Release

Release to PyPI.

```sh
$ python setup.py sdist upload
```

## License

Apache Software License, Version 2.0
