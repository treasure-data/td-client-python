# Treasure Data API library for Python

[![Build Status](https://travis-ci.org/treasure-data/td-client-python.svg)](https://travis-ci.org/treasure-data/td-client-python)
[![Build status](https://ci.appveyor.com/api/projects/status/eol91l1ag50xee9m/branch/master?svg=true)](https://ci.appveyor.com/project/nahi/td-client-python/branch/master)
[![Coverage Status](https://coveralls.io/repos/treasure-data/td-client-python/badge.svg)](https://coveralls.io/r/treasure-data/td-client-python)
[![Code Health](https://landscape.io/github/treasure-data/td-client-python/master/landscape.svg?style=flat)](https://landscape.io/github/treasure-data/td-client-python/master)
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
import tdclient

with tdclient.Client() as td:
    for job in td.jobs():
        print(job.job_id)
```

### Running jobs

Running jobs on Treasure Data.

```python
import tdclient

with tdclient.Client() as td:
    job = td.query("sample_datasets", "SELECT COUNT(1) FROM www_access", type="hive")
    job.wait()
    for row in job.result():
        print(repr(row))
```

### Running jobs via DBAPI2

td-client-python implements [PEP 0249](https://www.python.org/dev/peps/pep-0249/) Python Database API v2.0.
You can use td-client-python with external libraries which supports Database API such like [pandas](http://pandas.pydata.org/).

```python
import pandas
import tdclient

def on_waiting(cursor):
    print(cursor.job_status())

with tdclient.connect(db="sample_datasets", type="presto", wait_callback=on_waiting) as td:
    data = pandas.read_sql("SELECT symbol, COUNT(1) AS c FROM nasdaq GROUP BY symbol", td)
    print(repr(data))
```

We offer another package for pandas named [pandas-td](https://github.com/treasure-data/pandas-td) with some advanced features.
You may prefer it if you need to do complicated things, such like exporting result data to Treasure Data, printing job's
progress during long execution, etc.

### Importing data

Importing data into Treasure Data in streaming manner, as similar as [fluentd](http://www.fluentd.org/) is doing.

```python
import sys
import tdclient

with tdclient.Client() as td:
    for file_name in sys.argv[:1]:
        td.import_file("mydb", "mytbl", "csv", file_name)
```

### Bulk import

Importing data into Treasure Data in batch manner.

```python
from __future__ import print_function
import sys
import tdclient
import time
import warnings

if len(sys.argv) <= 1:
    sys.exit(0)

with tdclient.Client() as td:
    session_name = "session-%d" % (int(time.time()),)
    bulk_import = td.create_bulk_import(session_name, "mydb", "mytbl")
    try:
        for file_name in sys.argv[1:]:
            part_name = "part-%s" % (file_name,)
            bulk_import.upload_file(part_name, "json", file_name)
        bulk_import.freeze()
    except:
        bulk_import.delete()
        raise
    bulk_import.perform(wait=True)
    if 0 < bulk_import.error_records:
        warnings.warn("detected %d error records." % (bulk_import.error_records,))
    if 0 < bulk_import.valid_records:
        print("imported %d records." % (bulk_import.valid_records,))
    else:
        raise(RuntimeError("no records have been imported: %s" % (repr(bulk_import.name),)))
    bulk_import.commit(wait=True)
    bulk_import.delete()
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
$ python setup.py bdist_wheel --universal sdist upload
```

## Version History

See [CHANGELOG.md](CHANGELOG.md).

## License

Apache Software License, Version 2.0
