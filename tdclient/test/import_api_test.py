#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import io
import json
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

def test_import_data_with_id_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "elapsed_time": "3.14"
        }
    """
    td.put = mock.MagicMock(return_value=make_response(200, body))
    elapsed_time = td.import_data("db", "table", "format", b"stream", 6, unique_id="unique_id")
    td.put.assert_called_with("/v3/table/import_with_id/db/table/unique_id/format", b"stream", 6)
    assert elapsed_time == 3.14

def test_import_data_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "elapsed_time": 2.71
        }
    """
    td.put = mock.MagicMock(return_value=make_response(200, body))
    elapsed_time = td.import_data("db", "table", "format", b"stream", 6)
    td.put.assert_called_with("/v3/table/import/db/table/format", b"stream", 6)
    assert elapsed_time == 2.71

def test_import_data_failure():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.import_data("db", "table", "format", b"stream", 6)
    assert error.value.args == ("500: Import failed: error",)

def msgpackb(list):
    """list -> bytes"""
    stream = io.BytesIO()
    packer = msgpack.Packer()
    for item in list:
        stream.write(packer.pack(item))
    return stream.getvalue()

def msgunpackb(bytes):
    """bytes -> list"""
    unpacker = msgpack.Unpacker(io.BytesIO(bytes), encoding=str("utf-8"))
    return list(unpacker)

def jsonb(list):
    """list -> bytes"""
    stream = io.BytesIO()
    for item in list:
        stream.write(json.dumps(item).encode("utf-8"))
        stream.write(b"\n")
    return stream.getvalue()

def unjsonb(bytes):
    """bytes -> list"""
    return [ json.loads(s.decode("utf-8")) for s in bytes.splitlines() ]
    return list(unpacker)

def gzipb(bytes):
    """bytes -> bytes"""
    compress = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compress.compress(bytes) + compress.flush()

def gunzipb(bytes):
    """bytes -> bytes"""
    decompress = zlib.decompressobj(zlib.MAX_WBITS | 16)
    return decompress.decompress(bytes) + decompress.flush()

def test_import_file_msgpack_success():
    td = api.API("APIKEY")
    data = [
        {"str": "value1", "int": 1, "float": 2.3},
        {"str": "value4", "int": 5, "float": 6.7},
    ]
    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None
    td.import_data = import_data
    stream = io.BytesIO(msgpackb(data))
    td.import_file("db", "table", "msgpack", stream)

def test_import_file_msgpack_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"\xc1 malformed msgpack")
    with pytest.raises(ValueError) as error:
        td.import_file("db", "table", "msgpack", stream)

def test_import_file_msgpack_gz_success():
    td = api.API("APIKEY")
    data = [
        {"str": "value1", "int": 1, "float": 2.3},
        {"str": "value4", "int": 5, "float": 6.7},
    ]
    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None
    td.import_data = import_data
    stream = io.BytesIO(gzipb(msgpackb(data)))
    td.import_file("db", "table", "msgpack.gz", stream)

def test_import_file_msgpack_gz_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"\xc1 malformed msgpack.gz")
    with pytest.raises(IOError) as error:
        td.import_file("db", "table", "msgpack.gz", stream)

def test_import_file_json_success():
    td = api.API("APIKEY")
    data = [
        {"str": "value1", "int": 1, "float": 2.3},
        {"str": "value4", "int": 5, "float": 6.7},
    ]
    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None
    td.import_data = import_data
    stream = io.BytesIO(jsonb(data))
    td.import_file("db", "table", "json", stream)

def test_import_file_json_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed json")
    with pytest.raises(ValueError) as error:
        td.import_file("db", "table", "json", stream)

def test_import_file_json_gz_success():
    td = api.API("APIKEY")
    data = [
        {"str": "value1", "int": 1, "float": 2.3},
        {"str": "value4", "int": 5, "float": 6.7},
    ]
    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None
    td.import_data = import_data
    stream = io.BytesIO(gzipb(jsonb(data)))
    td.import_file("db", "table", "json.gz", stream)

def test_import_file_json_gz_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed json.gz")
    with pytest.raises(IOError) as error:
        td.import_file("db", "table", "json.gz", stream)

def test_import_file_unknown_format():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed json.gz")
    with pytest.raises(TypeError) as error:
        td.import_file("db", "table", "UNKNOWN", stream)
