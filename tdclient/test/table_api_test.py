#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_list_tables_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "tables":[
                {"id":210906,"name":"nasdaq","estimated_storage_size":168205061,"counter_updated_at":null,"last_log_timestamp":null,"type":"log","count":8807278,"expire_days":null,"created_at":"2014-10-08 02:57:38 UTC","updated_at":"2014-10-08 03:16:59 UTC","schema":"[[\\"symbol\\",\\"string\\"],[\\"open\\",\\"double\\"],[\\"volume\\",\\"long\\"],[\\"high\\",\\"double\\"],[\\"low\\",\\"double\\"],[\\"close\\",\\"double\\"]]"},
                {"id":208715,"name":"www_access","estimated_storage_size":0,"counter_updated_at":"2014-10-04T01:13:20Z","last_log_timestamp":"2014-10-04T01:13:15Z","type":"log","count":5000,"expire_days":null,"created_at":"2014-10-04 01:13:12 UTC","updated_at":"2014-10-22 07:02:19 UTC","schema":"[[\\"user\\",\\"int\\"],[\\"host\\",\\"string\\"],[\\"path\\",\\"string\\"],[\\"referer\\",\\"string\\"],[\\"code\\",\\"long\\"],[\\"agent\\",\\"string\\"],[\\"size\\",\\"long\\"],[\\"method\\",\\"string\\"]]"}
            ],
            "database":"sample_datasets"
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    tables = td.list_tables("sample_datasets")
    td.get.assert_called_with("/v3/table/list/sample_datasets")
    assert len(tables) == 2
    assert sorted(tables.keys()) == ["nasdaq", "www_access"]
    assert sorted([v.get("type") for v in tables.values()]) == ["log", "log"]


def test_list_tables_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_tables("sample_datasets")
    assert error.value.args == ("500: List tables failed: error",)


def test_create_log_table_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.create_log_table("sample_datasets", "nasdaq")
    td.post.assert_called_with("/v3/table/create/sample_datasets/nasdaq/log", {})


def test_swap_table_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.swap_table("sample_datasets", "foo", "bar")
    td.post.assert_called_with("/v3/table/swap/sample_datasets/foo/bar")


def test_update_schema_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.update_schema("sample_datasets", "foo", "{}")
    td.post.assert_called_with(
        "/v3/table/update-schema/sample_datasets/foo", {"schema": "{}"}
    )


def test_update_expire_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.update_expire("sample_datasets", "foo", 7)
    td.post.assert_called_with(
        "/v3/table/update/sample_datasets/foo", {"expire_days": 7}
    )


def test_delete_table_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "type": "item"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    td.delete_table("sample_datasets", "foo")
    td.post.assert_called_with("/v3/table/delete/sample_datasets/foo")


def test_tail_success():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(200, b""))
    records = td.tail("sample_datasets", "nasdaq", 3)
    td.get.assert_called_with(
        "/v3/table/tail/sample_datasets/nasdaq", {"count": 3, "format": "msgpack"}
    )


def test_change_database_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.change_database("sample_datasets", "foo", "new_database")
    td.post.assert_called_with(
        "/v3/table/change_database/sample_datasets/foo",
        {"dest_database_name": "new_database"},
    )
