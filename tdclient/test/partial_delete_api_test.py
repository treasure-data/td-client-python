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

def test_partial_delete_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "job_id":"12345"
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, body, res))
    partial_delete = td.partial_delete("sample_datasets", "nasdaq", 0, 10)
    td.post.assert_called_with("/v3/table/partialdelete/sample_datasets/nasdaq", {"from": "10", "to": "0"})

def test_partial_delete_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.post = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.partial_delete("sample_datasets", "nasdaq", 0, 10)
    assert error.value.args == ("500: Partial delete failed: error",)
