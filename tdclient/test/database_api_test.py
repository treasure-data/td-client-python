#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_list_databases_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "databases":[
                {"name":"sample_datasets","count":8812278,"created_at":"2014-10-04 01:13:11 UTC","updated_at":"2014-10-08 18:42:12 UTC","organization":null,"permission":"administrator"},
                {"name":"jma_weather","count":29545,"created_at":"2014-10-12 06:33:01 UTC","updated_at":"2014-10-12 06:33:01 UTC","organization":null,"permission":"administrator"},
                {"name":"test_db","count":15,"created_at":"2014-10-16 09:48:44 UTC","updated_at":"2014-10-16 09:48:44 UTC","organization":null,"permission":"administrator"}
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    databases = td.list_databases()
    td.get.assert_called_with("/v3/database/list")
    assert len(databases) == 3
    assert sorted(databases.keys()) == ["jma_weather", "sample_datasets", "test_db"]
    assert sorted([v.get("count") for v in databases.values()]) == [15, 29545, 8812278]


def test_list_databases_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_databases()
    assert error.value.args == ("500: List databases failed: error",)


def test_delete_database_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.delete_database("sample_datasets")
    td.post.assert_called_with("/v3/database/delete/sample_datasets")


def test_create_database_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.create_database("sample_datasets")
    td.post.assert_called_with("/v3/database/create/sample_datasets", {})
