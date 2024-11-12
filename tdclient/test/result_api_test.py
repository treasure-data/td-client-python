#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_list_result_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "results": [
                {
                    "name": "foo",
                    "organization": null,
                    "url": "mysql://example.com/db/foo"
                },
                {
                    "name": "bar",
                    "organization": null,
                    "url": "postgresql://example.com/db/bar"
                },
                {
                    "name": "baz",
                    "organization": null,
                    "url": "s3://s3.example.com/baz.csv"
                }
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    results = td.list_result()
    td.get.assert_called_with("/v3/result/list")
    assert len(results) == 3
    assert results[0] == ("foo", "mysql://example.com/db/foo", None)
    assert results[1] == ("bar", "postgresql://example.com/db/bar", None)
    assert results[2] == ("baz", "s3://s3.example.com/baz.csv", None)


def test_list_result_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_result()
    assert error.value.args == ("500: List result table failed: error",)


def test_create_result_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    results = td.create_result("foo", "http://example.com")
    td.post.assert_called_with("/v3/result/create/foo", {"url": "http://example.com"})


def test_delete_result_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    results = td.delete_result("foo")
    td.post.assert_called_with("/v3/result/delete/foo")
