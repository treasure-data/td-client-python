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

def test_grant_access_control_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    access_controls = td.grant_access_control("foo", "bar", "baz", "hoge")
    td.post.assert_called_with("/v3/acl/grant", {"subject": "foo", "action": "bar", "scope": "baz", "grant_option": "hoge"})

def test_revoke_access_control_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
    access_controls = td.revoke_access_control("foo", "bar", "baz")
    td.post.assert_called_with("/v3/acl/revoke", {"subject": "foo", "action": "bar", "scope": "baz"})

def test_test_access_control_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "permission": "",
            "access_controls": [
                {"subject":"foo","action":"bar","scope":"baz"},
                {"subject":"bar","action":"bar","scope":"baz"},
                {"subject":"baz","action":"bar","scope":"baz"}
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    access_controls = td.test_access_control("foo", "bar", "baz")
    td.get.assert_called_with("/v3/acl/test", {"user": "foo", "action": "bar", "scope": "baz"})

def test_list_access_controls_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "access_controls":[
              {"subject":"foo","action":"","scope":"","grant_option":""},
              {"subject":"bar","action":"","scope":"","grant_option":""},
              {"subject":"baz","action":"","scope":"","grant_option":""}
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    access_controls = td.list_access_controls()
    td.get.assert_called_with("/v3/acl/list")
    assert len(access_controls) == 3

def test_list_access_controls_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_access_controls()
    assert error.value.args == ("500: Listing access control failed: error",)
