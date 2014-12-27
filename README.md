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

td = tdclient.Client(os.getenv("TREASUREDATA_API_KEY"))
# list jobs
for job in td.jobs():
    print(job.job_id)
```

## Development

Runnning tests.

```sh
$ python setup.py develop
$ tox
```
