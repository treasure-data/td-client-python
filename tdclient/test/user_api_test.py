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

def test_list_users_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "users":[
                {"name":"foo","email":"foo@example.com"},
                {"name":"bar","email":"foo@example.com"},
                {"name":"baz","email":"foo@example.com"}
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    users = td.list_users()
    td.get.assert_called_with("/v3/user/list")
    assert len(users) == 3

def test_list_users_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_users()
    assert error.value.args == ("500: List users failed: error",)
