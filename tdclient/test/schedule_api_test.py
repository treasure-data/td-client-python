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

def test_list_schedules_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "schedules":[
                {"name":"foo","cron":"* * * * *","query":"SELECT COUNT(1) FROM nasdaq;","database":"sample_datasets","result":"","timezone":"UTC","delay":"","next_time":"","priority":"","retry_limit":""},
                {"name":"bar","cron":"* * * * *","query":"SELECT COUNT(1) FROM nasdaq;","database":"sample_datasets","result":"","timezone":"UTC","delay":"","next_time":"","priority":"","retry_limit":""},
                {"name":"baz","cron":"* * * * *","query":"SELECT COUNT(1) FROM nasdaq;","database":"sample_datasets","result":"","timezone":"UTC","delay":"","next_time":"","priority":"","retry_limit":""}
            ]
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    schedules = td.list_schedules()
    td.get.assert_called_with("/v3/schedule/list")
    assert len(schedules) == 3

def test_list_schedules_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_schedules()
    assert error.value.args == ("500: List schedules failed: error",)
