#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from tdclient import api
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_show_account_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "account":{
                "id":1,
                "plan":0,
                "storage_size":1024,
                "guaranteed_cores":0,
                "maximum_cores":4,
                "created_at":"1970-01-01"
            }
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    access_controls = td.show_account()
    td.get.assert_called_with("/v3/account/show")

def test_show_account_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.show_account()
    assert error.value.args == ("500: Show account failed: error",)
