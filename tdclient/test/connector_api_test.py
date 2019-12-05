#!/usr/bin/env python

import io
from unittest import mock

import msgpack
import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


seed = {
    "config": {
        "in": {
            "type": "s3",
            "access_key_id": "abcdefg",
            "secret_access_key": "hijklmn",
            "bucket": "bucket",
            "path_prefix": "prefix",
        },
        "out": {"mode": "append"},
    }
}

config = {
    "config": {
        "in": {
            "type": "s3",
            "access_key_id": "abcdefg",
            "secret_access_key": "hijklmn",
            "bucket": "bucket",
            "path_prefix": "prefix",
            "decoders": [{"type": "gzip"}],
            "parser": {
                "charset": "UTF-8",
                "newline": "CRLF",
                "delimiter": ",",
                "quote": "",
                "escape": "",
                "skip_header_lines": 1,
                "allow_extra_columns": False,
                "allow_optional_columns": False,
                "columns": [
                    {"name": "member_id", "type": "long"},
                    {"name": "goods_id", "type": "long"},
                    {"name": "category", "type": "string"},
                    {"name": "sub_category", "type": "string"},
                    {
                        "name": "order_date",
                        "type": "timestamp",
                        "format": "%Y-%m-%d %H:%M:%S",
                    },
                    {"name": "ship_date", "type": "timestamp", "format": "%Y-%m-%d"},
                    {"name": "amount", "type": "long"},
                    {"name": "price", "type": "long"},
                ],
            },
        },
        "out": {"mode": "append"},
    }
}


def dumps(x):
    return json.dumps(x).encode("utf-8")


def test_connector_guess_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, dumps(config)))
    res = td.connector_guess(seed)
    seed["config"]["exec"] = {}
    seed["config"]["filters"] = []
    assert res == config
    td.post.assert_called_with(
        "/v3/bulk_loads/guess",
        dumps(seed),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_guess_without_config_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, dumps(config)))
    res = td.connector_guess(seed["config"])
    seed["config"]["exec"] = {}
    seed["config"]["filters"] = []
    assert res == config
    td.post.assert_called_with(
        "/v3/bulk_loads/guess",
        dumps(seed),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_preview_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.post = mock.MagicMock(return_value=make_response(200, body))
    td.connector_preview(config)
    td.post.assert_called_with(
        "/v3/bulk_loads/preview",
        dumps(config),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_issue_success():
    td = api.API("APIKEY")
    body = b"""
      {"job_id": 12345}
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    res = td.connector_issue("database", "table", config)
    assert res == "12345"
    req = dict(config)
    req["database"] = "database"
    req["table"] = "table"
    td.post.assert_called_with(
        "/v3/job/issue/bulkload/database",
        dumps(req),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_list_success():
    td = api.API("APIKEY")
    body = b"[]"
    td.get = mock.MagicMock(return_value=make_response(200, body))
    td.connector_list()
    td.get.assert_called_with("/v3/bulk_loads")


def test_connector_create_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.post = mock.MagicMock(return_value=make_response(200, body))
    td.connector_create("name", "database", "table", config)
    req = dict(config)
    req["name"] = "name"
    req["database"] = "database"
    req["table"] = "table"
    td.post.assert_called_with(
        "/v3/bulk_loads",
        dumps(req),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_show_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.get = mock.MagicMock(return_value=make_response(200, body))
    td.connector_show("name")
    td.get.assert_called_with("/v3/bulk_loads/name")


def test_connector_update_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.put = mock.MagicMock(return_value=make_response(200, body))
    td.connector_update("name", config)
    td.put.assert_called_with(
        "/v3/bulk_loads/name",
        dumps(config),
        len(dumps(config)),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def test_connector_delete_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.delete = mock.MagicMock(return_value=make_response(200, body))
    td.connector_delete("name")
    td.delete.assert_called_with("/v3/bulk_loads/name")


def test_connector_history_success():
    td = api.API("APIKEY")
    body = b"[]"
    td.get = mock.MagicMock(return_value=make_response(200, body))
    td.connector_history("name")
    td.get.assert_called_with("/v3/bulk_loads/name/jobs")


def test_connector_run_success():
    td = api.API("APIKEY")
    body = b"{}"
    td.post = mock.MagicMock(return_value=make_response(200, body))
    td.connector_run("name", scheduled_time="scheduled_time")
    td.post.assert_called_with(
        "/v3/bulk_loads/name/jobs",
        dumps({"scheduled_time": "scheduled_time"}),
        headers={"content-type": "application/json; charset=utf-8"},
    )
