#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import contextlib
try:
    from unittest import mock
except ImportError:
    import mock
import os

def unset_environ():
    try:
        del os.environ["TD_API_KEY"]
    except KeyError:
        pass
    try:
        del os.environ["TD_API_SERVER"]
    except KeyError:
        pass
    try:
        del os.environ["HTTP_PROXY"]
    except KeyError:
        pass

def make_raw_response(status, body, headers={}):
    response = mock.MagicMock()
    response.status = status
    response.read = mock.MagicMock(return_value=body)
    return response

def make_response(*args, **kwargs):
    response = make_raw_response(*args, **kwargs)
    return contextlib.closing(response)
