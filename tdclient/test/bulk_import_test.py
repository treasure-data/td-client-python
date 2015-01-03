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

def test_list_bulk_imports_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "bulk_imports":[
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    access_controls = td.list_bulk_imports()
    td.get.assert_called_with("/v3/bulk_import/list", {})
    assert len(access_controls) == 0

def test_list_bulk_imports_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_bulk_imports()
    assert error.value.args == ("500: List bulk imports failed: error",)
