#!/usr/bin/env python

"""Tests for the dtypes and converters arguments to CSV import.
"""

import pytest

from io import BytesIO
from unittest import mock

from tdclient import api, Client
from tdclient.util import read_csv_records

from tdclient.test.test_helper import gunzipb
from tdclient.test.test_helper import make_response
from tdclient.test.test_helper import msgunpackb


DEFAULT_DATA = [
    {
        "time": "100",
        "col1": "0001",
        "col2": "10",
        "col3": "1.0",
        "col4": "abcd",
        "col5": "true",
        "col6": "none",
    },
    {
        "time": "200",
        "col1": "0002",
        "col2": "20",
        "col3": "2.0",
        "col4": "efgh",
        "col5": "false",
        "col6": "",
    },
]


def sample_reader(data=DEFAULT_DATA):
    """A very simple emulation of the actual CSV readers.
    """
    for item in data:
        yield item


def test_basic_read_csv_records():
    """The base test of read_csv_records - no customisation.
    """
    reader = sample_reader()

    result = list(read_csv_records(reader))

    assert result == [
        {
            "time": 100,
            "col1": 1,
            "col2": 10,
            "col3": 1.0,
            "col4": "abcd",
            "col5": True,
            "col6": None,
        },
        {
            "time": 200,
            "col1": 2,
            "col2": 20,
            "col3": 2.0,
            "col4": "efgh",
            "col5": False,
            "col6": None,
        },
    ]


def test_unsupported_dtype_gives_error():
    reader = sample_reader()

    with pytest.raises(ValueError) as excinfo:
        # Remember, it won't yield anything if we don't "next" it
        next(read_csv_records(reader, dtypes={"something": "no-such-dtype"}))
    assert "Unrecognized dtype 'no-such-dtype'" in str(excinfo.value)


def test_guess_dtype_gives_default_result():
    reader = sample_reader()

    result = list(
        read_csv_records(
            reader,
            dtypes={
                "time": "guess",
                "col1": "guess",
                "col2": "guess",
                "col3": "guess",
                "col4": "guess",
                "col5": "guess",
                "col6": "guess",
            },
        )
    )

    assert result == [
        {
            "time": 100,
            "col1": 1,
            "col2": 10,
            "col3": 1.0,
            "col4": "abcd",
            "col5": True,
            "col6": None,
        },
        {
            "time": 200,
            "col1": 2,
            "col2": 20,
            "col3": 2.0,
            "col4": "efgh",
            "col5": False,
            "col6": None,
        },
    ]


def test_dtypes_change_parsing():
    reader = sample_reader()

    result = list(
        read_csv_records(
            reader, dtypes={"col1": "str", "col2": "float", "col6": "str",}
        )
    )

    assert result == [
        {
            "time": 100,
            "col1": "0001",
            "col2": 10.0,
            "col3": 1.0,
            "col4": "abcd",
            "col5": True,
            "col6": "none",
        },
        {
            "time": 200,
            "col1": "0002",
            "col2": 20.0,
            "col3": 2.0,
            "col4": "efgh",
            "col5": False,
            "col6": "",
        },
    ]


def test_converters_change_parsing():
    reader = sample_reader()

    result = list(
        read_csv_records(reader, converters={"col1": str, "col2": float, "col6": str,})
    )

    assert result == [
        {
            "time": 100,
            "col1": "0001",
            "col2": 10.0,
            "col3": 1.0,
            "col4": "abcd",
            "col5": True,
            "col6": "none",
        },
        {
            "time": 200,
            "col1": "0002",
            "col2": 20.0,
            "col3": 2.0,
            "col4": "efgh",
            "col5": False,
            "col6": "",
        },
    ]


def test_dtypes_plus_converters_change_parsing():
    reader = sample_reader()

    result = list(
        read_csv_records(
            reader, dtypes={"col1": "str", "col6": "str",}, converters={"col2": float,}
        )
    )

    assert result == [
        {
            "time": 100,
            "col1": "0001",
            "col2": 10.0,
            "col3": 1.0,
            "col4": "abcd",
            "col5": True,
            "col6": "none",
        },
        {
            "time": 200,
            "col1": "0002",
            "col2": 20.0,
            "col3": 2.0,
            "col4": "efgh",
            "col5": False,
            "col6": "",
        },
    ]


def test_dtypes_overridden_by_converters():
    reader = sample_reader()

    result = list(
        read_csv_records(
            reader,
            dtypes={
                "time": "bool",  # overridden by converters
                "col1": "str",
                "col2": "int",  # overridden by converters
                "col6": "str",
            },
            converters={"time": int, "col2": float, "col5": str,},
        )
    )

    assert result == [
        {
            "time": 100,
            "col1": "0001",
            "col2": 10.0,
            "col3": 1.0,
            "col4": "abcd",
            "col5": "true",
            "col6": "none",
        },
        {
            "time": 200,
            "col1": "0002",
            "col2": 20.0,
            "col3": 2.0,
            "col4": "efgh",
            "col5": "false",
            "col6": "",
        },
    ]


DEFAULT_HEADER_BYTE_CSV = (
    b"time,col1,col2,col3,col4\n" b"100,0001,10,1.0,abcd\n" b"200,0002,20,2.0,efgh\n"
)


def test_import_file_supports_dtypes_and_converters():
    def import_data(db, table, format, stream, size, unique_id=None):
        data = stream.read(size)
        assert msgunpackb(gunzipb(data)) == [
            {"time": 100, "col1": "0001", "col2": 10.0, "col3": 1.0, "col4": "abcd"},
            {"time": 200, "col1": "0002", "col2": 20.0, "col3": 2.0, "col4": "efgh"},
        ]

    td = api.API("APIKEY")
    td.import_data = import_data
    td.import_file(
        "db",
        "table",
        "csv",
        BytesIO(DEFAULT_HEADER_BYTE_CSV),
        dtypes={"col1": "str", "col6": "str"},
        converters={"col2": float},
    )


def test_bulk_import_upload_file_supports_dtypes_and_converters():
    def bulk_import_upload_part(name, part_name, stream, size):
        data = stream.read(size)
        assert msgunpackb(gunzipb(data)) == [
            {"time": 100, "col1": "0001", "col2": 10.0, "col3": 1.0, "col4": "abcd"},
            {"time": 200, "col1": "0002", "col2": 20.0, "col3": 2.0, "col4": "efgh"},
        ]

    td = api.API("APIKEY")
    td.bulk_import_upload_part = bulk_import_upload_part
    td.bulk_import_upload_file(
        "name",
        "part-name",
        "csv",
        BytesIO(DEFAULT_HEADER_BYTE_CSV),
        dtypes={"col1": "str", "col6": "str"},
        converters={"col2": float},
    )


def test_bulk_import_dot_upload_file_supports_dtypes_and_converters():
    def bulk_import_upload_part(name, part_name, stream, size):
        data = stream.read(size)
        assert msgunpackb(gunzipb(data)) == [
            {"time": 100, "col1": "0001", "col2": 10.0, "col3": 1.0, "col4": "abcd"},
            {"time": 200, "col1": "0002", "col2": 20.0, "col3": 2.0, "col4": "efgh"},
        ]

    with Client("APIKEY") as td:
        td.api.post = mock.MagicMock(return_value=make_response(200, b""))
        td.api.bulk_import_upload_part = bulk_import_upload_part
        bulk_import = td.create_bulk_import("session-name", "mydb", "mytbl")
        bulk_import.update = mock.MagicMock()
        bulk_import.upload_file(
            "part-name",
            "csv",
            BytesIO(DEFAULT_HEADER_BYTE_CSV),
            dtypes={"col1": "str", "col6": "str"},
            converters={"col2": float},
        )
