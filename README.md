# Treasure Data API library for Python

[![Build Status](https://travis-ci.org/yyuu/td-client-python.svg)](https://travis-ci.org/yyuu/td-client-python)
[![Coverage Status](https://coveralls.io/repos/yyuu/td-client-python/badge.png)](https://coveralls.io/r/yyuu/td-client-python)

*EXPERIMENTAL* Treasure Data API library for Python

## Install

TODO: register to PyPI.

```sh
$ python setup.py install
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
$ for version in $(cat .python-version); do [ -d "$(pyenv root)/versions/${version}" ] || pyenv install "${version}"; done
```

Then, run `tox`.

```sh
$ tox
```

## License

Apache Software License, Version 2.0
