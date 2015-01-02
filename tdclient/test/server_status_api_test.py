#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    import mock
except ImportError:
    from unittest import mock
import pytest

from tdclient import api
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_server_status_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "status": "ok"
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    assert td.server_status() == "ok"
    td.get.assert_called_with("/v3/system/server_status")

def test_server_status_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    assert td.server_status() == "Server is down (500)"
    td.get.assert_called_with("/v3/system/server_status")
