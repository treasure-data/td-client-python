#!/usr/bin/env python

from unittest import mock

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
    assert start.utcoffset().seconds == 0  # UTC


def test_create_schedule_without_cron_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "start": null
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    start = td.create_schedule("bar", {"type": "presto", "cron": ""})
    td.post.assert_called_with(
        "/v3/schedule/create/bar", {"type": "presto", "cron": ""}
    )
    assert start.year == 1970
    assert start.month == 1
    assert start.day == 1
    assert start.hour == 0
    assert start.minute == 0
    assert start.second == 0
    assert start.utcoffset().seconds == 0  # UTC


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
    body = b"""
        {
            "schedules":[
                {
                    "name": "foo",
                    "cron": null,
                    "timezone": "UTC",
                    "delay": 0,
                    "created_at": "2016-08-02T17:58:40Z",
                    "type": "presto",
                    "query": "SELECT COUNT(1) FROM nasdaq;",
                    "database": "sample_datasets",
                    "user_name": "Yuu Yamashita",
                    "priority": 0,
                    "retry_limit": 0,
                    "result": "",
                    "next_time": null
                },
                {
                    "name": "bar",
                    "cron": "0 0 * * *",
                    "timezone": "UTC",
                    "delay": 0,
                    "created_at": "2016-08-02T18:01:04Z",
                    "type": "presto",
                    "query": "SELECT COUNT(1) FROM nasdaq;",
                    "database": "sample_datasets",
                    "user_name": "Kazuki Ota",
                    "priority": 0,
                    "retry_limit": 0,
                    "result": "",
                    "next_time": "2016-09-24T00:00:00Z"
                },
                {
                    "name": "baz",
                    "cron": "* * * * *",
                    "timezone": "UTC",
                    "delay": 0,
                    "created_at": "2016-03-02T23:01:59Z",
                    "type": "hive",
                    "query": "SELECT COUNT(1) FROM nasdaq;",
                    "database": "sample_datasets",
                    "user_name": "Yuu Yamashita",
                    "priority": 0,
                    "retry_limit": 0,
                    "result": "",
                    "next_time": "2016-07-06T00:00:00Z"
                }
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    schedules = td.list_schedules()
    td.get.assert_called_with("/v3/schedule/list")
    assert len(schedules) == 3
    next_time = sorted(
        [schedule.get("next_time") for schedule in schedules if "next_time" in schedule]
    )
    assert len(next_time) == 3
    assert next_time[2].year == 2016
    assert next_time[2].month == 9
    assert next_time[2].day == 24
    assert next_time[2].hour == 0
    assert next_time[2].minute == 0
    assert next_time[2].second == 0
    created_at = sorted(
        [
            schedule.get("created_at")
            for schedule in schedules
            if "created_at" in schedule
        ]
    )
    assert len(created_at) == 3
    assert created_at[2].year == 2016
    assert created_at[2].month == 8
    assert created_at[2].day == 2
    assert created_at[2].hour == 18
    assert created_at[2].minute == 1
    assert created_at[2].second == 4


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
    body = b"""
        {
            "history": [
                {
                    "query": "SELECT COUNT(1) FROM nasdaq;",
                    "type": "presto",
                    "priority": 0,
                    "retry_limit": 0,
                    "duration": 1,
                    "status": "success",
                    "cpu_time": null,
                    "result_size": 30,
                    "job_id": "12345",
                    "created_at": "2016-04-13 05:24:59 UTC",
                    "updated_at": "2016-04-13 05:25:02 UTC",
                    "start_at": "2016-04-13 05:25:00 UTC",
                    "end_at": "2016-04-13 05:25:01 UTC",
                    "num_records": 1,
                    "database": "sample_datasets",
                    "user_name": "Ryuta Kamizono",
                    "result": "",
                    "url": "https://console.treasuredata.com/jobs/12345",
                    "hive_result_schema": "[[\\"_col0\\", \\"bigint\\"]]",
                    "organization": null,
                    "scheduled_at": ""
                },
                {
                    "query": "SELECT COUNT(1) FROM nasdaq;",
                    "type": "presto",
                    "priority": 0,
                    "retry_limit": 0,
                    "duration": 1,
                    "status": "success",
                    "cpu_time": null,
                    "result_size": 30,
                    "job_id": "67890",
                    "created_at": "2016-04-13 05:24:59 UTC",
                    "updated_at": "2016-04-13 05:25:02 UTC",
                    "start_at": "2016-04-13 05:25:00 UTC",
                    "end_at": "2016-04-13 05:25:01 UTC",
                    "num_records": 1,
                    "database": "sample_datasets",
                    "user_name": "Ryuta Kamizono",
                    "result": "",
                    "url": "https://console.treasuredata.com/jobs/67890",
                    "hive_result_schema": "[[\\"_col0\\", \\"bigint\\"]]",
                    "organization": null,
                    "scheduled_at": ""
                }
            ],
            "count": 2,
            "from": 0,
            "to": 20
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
