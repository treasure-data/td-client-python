#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

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
    response.pos = 0
    response.body = body
    def read(size=None):
        if response.pos < len(response.body):
            if size is None:
                s = body[response.pos:]
                response.pos = len(response.body)
            else:
                s = response.body[response.pos:response.pos+size]
                response.pos += size
            return s
        else:
            return b""
    response.read.side_effect = read
    return response

def make_response(*args, **kwargs):
    response = make_raw_response(*args, **kwargs)
    return contextlib.closing(response)
