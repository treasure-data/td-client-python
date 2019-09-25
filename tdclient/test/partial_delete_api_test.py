#!/usr/bin/env python

from unittest import mock

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
    td.post = mock.MagicMock(return_value=make_response(200, body))
    partial_delete = td.partial_delete("sample_datasets", "nasdaq", 0, 10)
    td.post.assert_called_with(
        "/v3/table/partialdelete/sample_datasets/nasdaq", {"from": "10", "to": "0"}
    )


def test_partial_delete_failure():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.partial_delete("sample_datasets", "nasdaq", 0, 10)
    assert error.value.args == ("500: Partial delete failed: error",)
