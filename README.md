# Treasure Data API library for Python

[![Build Status](https://travis-ci.org/yyuu/td-client-python.svg)](https://travis-ci.org/yyuu/td-client-python)
[![Coverage Status](https://coveralls.io/repos/yyuu/td-client-python/badge.png)](https://coveralls.io/r/yyuu/td-client-python)
[![PyPI version](https://badge.fury.io/py/td-client.svg)](http://badge.fury.io/py/td-client)

*EXPERIMENTAL* Treasure Data API library for Python

## Install

You can install the releases from [PyPI](https://pypi.python.org/).

```sh
$ pip install td-client
```

## Examples

### Listing jobs

```python
#!/usr/bin/env python

import os
import sys
import tdclient

td = tdclient.Client(os.getenv("TD_API_KEY"))
# list jobs
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

## License

Apache Software License, Version 2.0
