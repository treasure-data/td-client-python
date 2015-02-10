#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import io
import msgpack
try:
    from unittest import mock
except ImportError:
    import mock
import pytest
import zlib

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
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "status": "SUCCESS"
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    bulk_import = td.show_bulk_import("name")
    td.get.assert_called_with("/v3/bulk_import/show/name")
    assert bulk_import == {"status": "SUCCESS"}

def test_list_bulk_imports_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "bulk_imports":[
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    bulk_imports = td.list_bulk_imports()
    td.get.assert_called_with("/v3/bulk_import/list", {})
    assert len(bulk_imports) == 0

def test_list_bulk_imports_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_bulk_imports()
    assert error.value.args == ("500: List bulk imports failed: error",)

def test_list_bulk_import_parts_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "parts":[
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    parts = td.list_bulk_import_parts("foo")
    td.get.assert_called_with("/v3/bulk_import/list_parts/foo", {})
    assert len(parts) == 0

def test_list_bulk_import_upload_part_success():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(200, b""))
    td.bulk_import_upload_part("name", "part_name", "stream", 1024)
    td.put.assert_called_with("/v3/bulk_import/upload_part/name/part_name", "stream", 1024)

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
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "job_id": "12345"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    job_id = td.perform_bulk_import("name")
    td.post.assert_called_with("/v3/bulk_import/perform/name", {})
    assert job_id == "12345"

def test_commit_bulk_import_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.commit_bulk_import("name")
    td.post.assert_called_with("/v3/bulk_import/commit/name", {})

def msgpackb(list):
    """list -> bytes"""
    stream = io.BytesIO()
    packer = msgpack.Packer()
    for item in list:
        stream.write(packer.pack(item))
    return stream.getvalue()

def gzipb(bytes):
    """bytes -> bytes"""
    compress = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compress.compress(bytes) + compress.flush()

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
