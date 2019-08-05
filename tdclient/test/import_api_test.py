#!/usr/bin/env python

import io
import json
import tempfile
import time
import zlib
from unittest import mock

import msgpack
import pytest

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
    elapsed_time = td.import_data(
        "db", "table", "format", b"stream", 6, unique_id="unique_id"
    )
    td.put.assert_called_with(
        "/v3/table/import_with_id/db/table/unique_id/format", b"stream", 6
    )
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


def test_import_file_msgpack_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
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


def test_import_file_msgpack_bigint_as_string():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "int": 64, "bigint": -(1 << 64)},
        {"time": int(time.time()), "int": 128, "bigint": 1 << 128},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == msgunpackb(msgpackb(data))
        assert unique_id is None

    td.import_data = import_data
    stream = io.BytesIO(msgpackb(data))
    td.import_file("db", "table", "msgpack", stream)


def test_import_file_msgpack_file_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    # should not use `tempfile.NamedTemporaryFile` to fix tests working on Windows
    # http://bugs.python.org/issue14243
    name = None
    try:
        fd, name = tempfile.mkstemp("wb")
        fp = os.fdopen(fd, "wb")
        fp.write(msgpackb(data))
        fp.close()
        td.import_file("db", "table", "msgpack", name)
    finally:
        if name is not None:
            os.unlink(name)


def test_import_file_msgpack_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"\xc1 malformed msgpack")
    with pytest.raises(ValueError) as error:
        td.import_file("db", "table", "msgpack", stream)


def test_import_file_msgpack_gz_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
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


def test_import_file_msgpack_gz_file_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    # should not use `tempfile.NamedTemporaryFile` to fix tests working on Windows
    # http://bugs.python.org/issue14243
    name = None
    try:
        fd, name = tempfile.mkstemp("wb")
        fp = os.fdopen(fd, "wb")
        fp.write(gzipb(msgpackb(data)))
        fp.close()
        td.import_file("db", "table", "msgpack.gz", name)
    finally:
        if name is not None:
            os.unlink(name)


def test_import_file_msgpack_gz_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"\xc1 malformed msgpack.gz")
    with pytest.raises(IOError) as error:
        td.import_file("db", "table", "msgpack.gz", stream)


def test_import_file_json_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
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
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
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


def test_import_file_csv_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    columns = data[0].keys()
    stream = io.BytesIO(csvb(data, columns=columns))
    td.import_file("db", "table", "csv", stream, columns=columns)


def test_import_file_tsv_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    columns = data[0].keys()
    stream = io.BytesIO(tsvb(data, columns=columns))
    td.import_file("db", "table", "tsv", stream, columns=columns)


def test_import_file_csv_dict_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    stream = io.BytesIO(dcsvb(data))
    td.import_file("db", "table", "csv", stream)


def test_import_file_csv_dict_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed\0csv")
    with pytest.raises(Exception) as error:
        td.import_file("db", "table", "csv", stream)


def test_import_file_tsv_dict_success():
    td = api.API("APIKEY")
    data = [
        {"time": int(time.time()), "str": "value1", "int": 1, "float": 2.3},
        {"time": int(time.time()), "str": "value4", "int": 5, "float": 6.7},
    ]

    def import_data(db, table, format, stream, size, unique_id=None):
        assert db == "db"
        assert table == "table"
        assert format == "msgpack.gz"
        assert msgunpackb(gunzipb(stream.read(size))) == data
        assert unique_id is None

    td.import_data = import_data
    stream = io.BytesIO(dtsvb(data))
    td.import_file("db", "table", "tsv", stream)


def test_import_file_tsv_dict_failure():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed\0tsv")
    with pytest.raises(Exception) as error:
        td.import_file("db", "table", "tsv", stream)


def test_import_file_unknown_format():
    td = api.API("APIKEY")
    td.import_data = mock.MagicMock()
    stream = io.BytesIO(b"malformed json.gz")
    with pytest.raises(TypeError) as error:
        td.import_file("db", "table", "UNKNOWN", stream)
