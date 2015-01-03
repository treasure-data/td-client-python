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

def test_authenticate_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "apikey": "foobarbaz"
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, body, res))
    apikey = td.authenticate("foo", "bar")
    td.post.assert_called_with("/v3/user/authenticate", {"user": "foo", "password": "bar"})
    assert apikey == "foobarbaz"

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

def test_add_user_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikey = td.add_user("name", "org", "email@example.com", "password")
    td.post.assert_called_with("/v3/user/add/name", {"organization": "org", "email": "email@example.com", "password": "password"})

def test_remove_user_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikey = td.remove_user("user")
    td.post.assert_called_with("/v3/user/remove/user")

def test_change_email_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikey = td.change_email("user", "email@example.com")
    td.post.assert_called_with("/v3/user/email/change/user", {"email": "email@example.com"})

def test_list_apikeys_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "apikeys": []
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    apikeys = td.list_apikeys("foo")
    td.get.assert_called_with("/v3/user/apikey/list/foo")

def test_add_apikey_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikeys = td.add_apikey("foo")
    td.post.assert_called_with("/v3/user/apikey/add/foo")

def test_remove_apikey_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikeys = td.remove_apikey("foo", "bar")
    td.post.assert_called_with("/v3/user/apikey/remove/foo", {"apikey": "bar"})

def test_change_password_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikeys = td.change_password("foo", "bar")
    td.post.assert_called_with("/v3/user/password/change/foo", {"password": "bar"})

def test_change_my_password_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    apikeys = td.change_my_password("foo", "bar")
    td.post.assert_called_with("/v3/user/password/change", {"old_password": "foo", "password": "bar"})
