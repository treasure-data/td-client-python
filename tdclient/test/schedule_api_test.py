#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from tdclient import api
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_create_schedule_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "start": "2015-01-24 04:34:51 UTC"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    start = td.create_schedule("bar", {"type": "presto"})
    td.post.assert_called_with("/v3/schedule/create/bar", {"type": "presto"})
    assert start.year == 2015
    assert start.month == 1
    assert start.day == 24
    assert start.hour == 4
    assert start.minute == 34
    assert start.second == 51
    assert start.utcoffset().seconds == 0 # UTC

def test_delete_schedule_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "cron": "foo",
            "query": "SELECT 1 FROM nasdaq"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    cron, query = td.delete_schedule("bar")
    td.post.assert_called_with("/v3/schedule/delete/bar")
    assert cron == "foo"
    assert query == "SELECT 1 FROM nasdaq"

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
    td.get = mock.MagicMock(return_value=make_response(200, body))
    schedules = td.list_schedules()
    td.get.assert_called_with("/v3/schedule/list")
    assert len(schedules) == 3

def test_list_schedules_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_schedules()
    assert error.value.args == ("500: List schedules failed: error",)

def test_update_schedule_success():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    td.update_schedule("foo")
    td.post.assert_called_with("/v3/schedule/update/foo", {})

def test_history_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "history": [
                {"job_id":"12345"},
                {"job_id":"67890"}
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    history = td.history("foo", 0, 3)
    td.get.assert_called_with("/v3/schedule/history/foo", {"from": "0", "to": "3"})

def test_run_schedule_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "jobs": [
                {"job_id":"12345","type":"hive"},
                {"job_id":"67890","type":"hive"}
            ]
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.run_schedule("name", "time", 1)
    td.post.assert_called_with("/v3/schedule/run/name/time", {"num": 1})
