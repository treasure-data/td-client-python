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

def test_list_databases_success():
    td = api.API("APIKEY")
    body = """
        {
            "databases":[
                {"name":"sample_datasets","count":8812278,"created_at":"2014-10-04 01:13:11 UTC","updated_at":"2014-10-08 18:42:12 UTC","organization":null,"permission":"administrator"},
                {"name":"jma_weather","count":29545,"created_at":"2014-10-12 06:33:01 UTC","updated_at":"2014-10-12 06:33:01 UTC","organization":null,"permission":"administrator"},
                {"name":"test_db","count":15,"created_at":"2014-10-16 09:48:44 UTC","updated_at":"2014-10-16 09:48:44 UTC","organization":null,"permission":"administrator"}
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    databases = td.list_databases()
    td.get.assert_called_with("/v3/database/list")
    assert len(databases) == 3
    assert sorted(databases.keys()) == ["jma_weather", "sample_datasets", "test_db"]
    assert sorted([ v[0] for v in databases.values() ]) == [15, 29545, 8812278]

def test_list_databases_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, "error", res))
    with pytest.raises(api.APIError) as error:
        td.list_databases()
    assert error.value.message == "500: List databases failed: error"
