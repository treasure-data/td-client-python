# Treasure Data API library for Python

*experimental*

## Install

```sh
$ python setup.py install
```

## Examples

Listing jobs.

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

I'd recommend you to install [pyenv](https://github.com/yyuu/pyenv) first.

Then, run tests.

```sh
$ for version in $(cat .python-version); do [ -d "$(pyenv root)/versions/${version}" ] || pyenv install "${version}"; done
$ python setup.py develop
$ tox
```
