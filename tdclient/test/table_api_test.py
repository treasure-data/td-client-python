#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    import mock
except ImportError:
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
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    tables = td.list_tables("sample_datasets")
    td.get.assert_called_with("/v3/table/list/sample_datasets")
    assert len(tables) == 2
    assert sorted(tables.keys()) == ["nasdaq", "www_access"]
    assert sorted([ v[0] for v in tables.values() ]) == ["log", "log"]

def test_list_tables_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_tables("sample_datasets")
    assert error.value.args == ("500: List tables failed: error",)
