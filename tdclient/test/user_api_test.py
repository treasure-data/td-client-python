#!/usr/bin/env python

from unittest import mock

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
    td.post = mock.MagicMock(return_value=make_response(200, body))
    apikey = td.authenticate("foo", "bar")
    td.post.assert_called_with(
        "/v3/user/authenticate", {"user": "foo", "password": "bar"}
    )
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
    td.get = mock.MagicMock(return_value=make_response(200, body))
    users = td.list_users()
    td.get.assert_called_with("/v3/user/list")
    assert len(users) == 3


def test_list_users_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_users()
    assert error.value.args == ("500: List users failed: error",)


def test_add_user_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    apikey = td.add_user("name", "org", "email@example.com", "password")
    td.post.assert_called_with(
        "/v3/user/add/name",
        {"organization": "org", "email": "email@example.com", "password": "password"},
    )


def test_remove_user_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    apikey = td.remove_user("user")
    td.post.assert_called_with("/v3/user/remove/user")


def test_list_apikeys_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "apikeys": []
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    apikeys = td.list_apikeys("foo")
    td.get.assert_called_with("/v3/user/apikey/list/foo")


def test_add_apikey_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    apikeys = td.add_apikey("foo")
    td.post.assert_called_with("/v3/user/apikey/add/foo")


def test_remove_apikey_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    apikeys = td.remove_apikey("foo", "bar")
    td.post.assert_called_with("/v3/user/apikey/remove/foo", {"apikey": "bar"})
