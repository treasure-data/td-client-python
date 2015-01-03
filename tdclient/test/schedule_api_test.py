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

def test_create_schedule_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "start": "foo"
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, body, res))
    start = td.create_schedule("bar", {"type": "presto"})
    td.post.assert_called_with("/v3/schedule/create/bar", {"type": "presto"})
    assert start == "foo"

def test_delete_schedule_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "cron": "foo",
            "query": "SELECT 1 FROM nasdaq"
        }
    """
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, body, res))
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

def test_update_schedule_success():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, b"", res))
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
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
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
    res = mock.MagicMock()
    res.status = 200
    td.post = mock.MagicMock(return_value=(res.status, body, res))
    jobs = td.run_schedule("name", "time", 1)
    td.post.assert_called_with("/v3/schedule/run/name/time", {"num": 1})
