#!/usr/bin/env python

import io
import time
from unittest import mock

import msgpack
import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_create_bulk_import_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.create_bulk_import("name", "db", "table")
    td.post.assert_called_with("/v3/bulk_import/create/name/db/table", {})


def test_delete_bulk_import_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.delete_bulk_import("name")
    td.post.assert_called_with("/v3/bulk_import/delete/name", {})


def test_show_bulk_import_success():
    td = api.API("APIKEY")
    body = b"""
        {"name":"foo","status":"uploading","job_id":null,"valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"upload_frozen":false,"database":"db_name","table":"tbl_name"}
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    bulk_import = td.show_bulk_import("foo")
    td.get.assert_called_with("/v3/bulk_import/show/foo")
    assert bulk_import["name"] == "foo"
    assert bulk_import["status"] == "uploading"


def test_list_bulk_imports_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "bulk_imports":[
                {"name":"foo","valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"status":"uploading","upload_frozen":false,"database":"yuudb","table":"yuutbl1","job_id":null},
                {"name":"bar","valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"status":"uploading","upload_frozen":false,"database":"yuudb","table":"yuutbl1","job_id":null}
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    bulk_imports = td.list_bulk_imports()
    td.get.assert_called_with("/v3/bulk_import/list", {})
    assert len(bulk_imports) == 2
    assert sorted([bulk_import["name"] for bulk_import in bulk_imports]) == [
        "bar",
        "foo",
    ]


def test_list_bulk_imports_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_bulk_imports()
    assert error.value.args == ("500: List bulk imports failed: error",)


def test_list_bulk_import_parts_success():
    td = api.API("APIKEY")
    body = b"""
        {"parts":["part1", "part2"],"name":"foo","bulk_import":"foo"}
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    parts = td.list_bulk_import_parts("foo")
    td.get.assert_called_with("/v3/bulk_import/list_parts/foo", {})
    assert len(parts) == 2
    assert sorted(parts) == ["part1", "part2"]


def test_list_bulk_import_upload_part_success():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(200, b""))
    td.bulk_import_upload_part("name", "part_name", "stream", 1024)
    td.put.assert_called_with(
        "/v3/bulk_import/upload_part/name/part_name", "stream", 1024
    )


def test_list_bulk_import_upload_part_fail_periods():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(200, b""))
    with pytest.raises(ValueError):
        td.bulk_import_upload_part("name", "bad.part.name", "stream", 1024)


def test_list_bulk_import_upload_part_fail_slashes():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(200, b""))
    with pytest.raises(ValueError):
        td.bulk_import_upload_part("name", "bad/part.name", "stream", 1024)


def test_list_bulk_import_upload_file_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def bulk_import_upload_part(name, part_name, stream, size):
        assert name == "name"
        assert part_name == "part_name"
        assert msgunpackb(gunzipb(stream.read(size))) == data

    td.bulk_import_upload_part = bulk_import_upload_part
    stream = io.BytesIO(jsonb(data))
    td.bulk_import_upload_file("name", "part_name", "json", stream)


def test_list_bulk_import_delete_part_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.bulk_import_delete_part("name", "part_name")
    td.post.assert_called_with("/v3/bulk_import/delete_part/name/part_name", {})


def test_freeze_bulk_import_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.freeze_bulk_import("name")
    td.post.assert_called_with("/v3/bulk_import/freeze/name", {})


def test_unfreeze_bulk_import_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.unfreeze_bulk_import("name")
    td.post.assert_called_with("/v3/bulk_import/unfreeze/name", {})


def test_perform_bulk_import_success():
    td = api.API("APIKEY")
    body = b"""
        {"name":"foo","bulk_import":"foo","job_id":12345}
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    job_id = td.perform_bulk_import("foo")
    td.post.assert_called_with("/v3/bulk_import/perform/foo", {})
    assert job_id == "12345"


def test_commit_bulk_import_success():
    td = api.API("APIKEY")
    body = b"""
        {"name":"foo","bulk_import":"foo"}
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    td.commit_bulk_import("foo")
    td.post.assert_called_with("/v3/bulk_import/commit/foo", {})


def test_bulk_import_error_records_success():
    td = api.API("APIKEY")
    data = [
        {"str": "value1", "int": 1, "float": 2.3},
        {"str": "value4", "int": 5, "float": 6.7},
    ]
    body = gzipb(msgpackb(data))
    td.get = mock.MagicMock(return_value=make_response(200, body))
    rows = []
    for row in td.bulk_import_error_records("name"):
        rows.append(row)
    td.get.assert_called_with("/v3/bulk_import/error_records/name", {})


def test_bulk_import_error_records_false():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        for row in td.bulk_import_error_records("name"):
            pass
    td.get.assert_called_with("/v3/bulk_import/error_records/name", {})
