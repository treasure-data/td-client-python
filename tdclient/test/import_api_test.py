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

def test_import_with_id_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "elapsed_time": "3.14"
        }
    """
    td.put = mock.MagicMock(return_value=make_response(200, body))
    elapsed_time = td.import_data("db", "table", "format", b"stream", 6, unique_id="unique_id")
    td.put.assert_called_with("/v3/table/import_with_id/db/table/unique_id/format", b"stream", 6, endpoint="https://api-import.treasuredata.com/")
    assert elapsed_time == 3.14

def test_import_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "elapsed_time": 2.71
        }
    """
    td.put = mock.MagicMock(return_value=make_response(200, body))
    elapsed_time = td.import_data("db", "table", "format", b"stream", 6)
    td.put.assert_called_with("/v3/table/import/db/table/format", b"stream", 6, endpoint="https://api-import.treasuredata.com/")
    assert elapsed_time == 2.71

def test_import_failure():
    td = api.API("APIKEY")
    td.put = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.import_data("db", "table", "format", b"stream", 6)
    assert error.value.args == ("500: Import failed: error",)
