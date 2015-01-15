#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

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
    td.get = mock.MagicMock(return_value=make_response(200, body))
    access_controls = td.show_account()
    td.get.assert_called_with("/v3/account/show")

def test_show_account_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.show_account()
    assert error.value.args == ("500: Show account failed: error",)

def test_core_utilization_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "from": "2015-01-03 10:29:34 UTC",
            "to": "2015-01-03 10:30:29 UTC",
            "interval": "1",
            "history": ""
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    access_controls = td.account_core_utilization(0, 3)
    td.get.assert_called_with("/v3/account/core_utilization", {"from": "0", "to": "3"})
