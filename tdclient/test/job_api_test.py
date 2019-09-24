#!/usr/bin/env python

import datetime
import json
from unittest import mock

import dateutil.tz
import msgpack
import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_list_jobs_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "count":11,"from":0,"to":10,"jobs":[
                {"status":"success","cpu_time":496570,"result_size":24,"duration":262,"job_id":"18882028","created_at":"2014-12-23 12:00:18 UTC","updated_at":"2014-12-23 12:04:42 UTC","start_at":"2014-12-23 12:00:19 UTC","end_at":"2014-12-23 12:04:41 UTC","query":"select count(1) from import_test","type":"hive","priority":0,"retry_limit":0,"result":"","url":"http://console.treasuredata.com/jobs/18882028","user_name":"owner","hive_result_schema":"[[\\"_c0\\", \\"bigint\\"]]","organization":null,"database":"jma_weather"},
                {"status":"success","cpu_time":489540,"result_size":24,"duration":272,"job_id":"18880612","created_at":"2014-12-23 11:00:16 UTC","updated_at":"2014-12-23 11:04:48 UTC","start_at":"2014-12-23 11:00:16 UTC","end_at":"2014-12-23 11:04:48 UTC","query":"select count(1) from import_test","type":"hive","priority":0,"retry_limit":0,"result":"","url":"http://console.treasuredata.com/jobs/18880612","user_name":"owner","hive_result_schema":"[[\\"_c0\\", \\"bigint\\"]]","organization":null,"database":"jma_weather"},
                {"status":"success","cpu_time":486630,"result_size":24,"duration":263,"job_id":"18879199","created_at":"2014-12-23 10:00:21 UTC","updated_at":"2014-12-23 10:04:44 UTC","start_at":"2014-12-23 10:00:21 UTC","end_at":"2014-12-23 10:04:44 UTC","query":"select count(1) from import_test","type":"hive","priority":0,"retry_limit":0,"result":"","url":"http://console.treasuredata.com/jobs/18879199","user_name":"owner","hive_result_schema":"[[\\"_c0\\", \\"bigint\\"]]","organization":null,"database":"jma_weather"}
            ]
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.list_jobs(0, 2)
    td.get.assert_called_with("/v3/job/list", {"from": "0", "to": "2"})
    assert len(jobs) == 3
    assert sorted([job["job_id"] for job in jobs]) == [
        "18879199",
        "18880612",
        "18882028",
    ]
    assert sorted([job["url"] for job in jobs]) == [
        "http://console.treasuredata.com/jobs/18879199",
        "http://console.treasuredata.com/jobs/18880612",
        "http://console.treasuredata.com/jobs/18882028",
    ]
    assert [job["debug"] for job in jobs] == [None, None, None]
    assert sorted([job["hive_result_schema"] for job in jobs]) == [
        [["_c0", "bigint"]],
        [["_c0", "bigint"]],
        [["_c0", "bigint"]],
    ]


def test_list_jobs_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_jobs(0, 2)
    assert error.value.args == ("500: List jobs failed: error",)


def test_show_job_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "cpu_time": null,
            "created_at": "2015-02-09 11:44:25 UTC",
            "database": "sample_datasets",
            "debug": {
                "cmdout": "started at 2015-02-09T11:44:27Z\\nexecuting query: SELECT COUNT(1) FROM nasdaq\\n",
                "stderr": null
            },
            "duration": 1,
            "end_at": "2015-02-09 11:44:28 UTC",
            "hive_result_schema": "[[\\"cnt\\", \\"bigint\\"]]",
            "job_id": "12345",
            "organization": null,
            "priority": 1,
            "query": "SELECT COUNT(1) FROM nasdaq",
            "result": "",
            "result_size": 22,
            "retry_limit": 0,
            "start_at": "2015-02-09 11:44:27 UTC",
            "status": "success",
            "type": "presto",
            "updated_at": "2015-02-09 11:44:28 UTC",
            "url": "http://console.example.com/jobs/12345",
            "user_name": "nobody@example.com",
            "linked_result_export_job_id": null,
            "result_export_target_job_id": null,
            "num_records": 4
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    job = td.show_job(12345)
    td.get.assert_called_with("/v3/job/show/12345")
    assert job["job_id"] == 12345
    assert job["type"] == "presto"
    assert job["url"] == "http://console.example.com/jobs/12345"
    assert job["query"] == "SELECT COUNT(1) FROM nasdaq"
    assert job["status"] == "success"
    assert job["debug"] == {
        "cmdout": "started at 2015-02-09T11:44:27Z\nexecuting query: SELECT COUNT(1) FROM nasdaq\n",
        "stderr": None,
    }
    assert job["start_at"] == datetime.datetime(
        2015, 2, 9, 11, 44, 27, tzinfo=dateutil.tz.tzutc()
    )
    assert job["end_at"] == datetime.datetime(
        2015, 2, 9, 11, 44, 28, tzinfo=dateutil.tz.tzutc()
    )
    assert job["cpu_time"] is None
    assert job["result_size"] == 22
    assert job["result"] is None
    assert job["result_url"] is None
    assert job["hive_result_schema"] == [["cnt", "bigint"]]
    assert job["priority"] == 1
    assert job["retry_limit"] == 0
    assert job["org_name"] is None
    assert job["database"] == "sample_datasets"


def test_job_status_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "status": "RUNNING"
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.job_status(12345)
    td.get.assert_called_with("/v3/job/status/12345")


def test_job_result_success():
    td = api.API("APIKEY")
    packer = msgpack.Packer()
    rows = [["foo", 123], ["bar", 456], ["baz", 789]]
    body = b""
    for row in rows:
        body += packer.pack(row)
    td.get = mock.MagicMock(return_value=make_response(200, body))
    result = td.job_result(12345)
    td.get.assert_called_with("/v3/job/result/12345", {"format": "msgpack"})
    assert result == rows


def test_job_result_each_success():
    td = api.API("APIKEY")
    packer = msgpack.Packer()
    rows = [["foo", 123], ["bar", 456], ["baz", 789]]
    body = b""
    for row in rows:
        body += packer.pack(row)
    td.get = mock.MagicMock(return_value=make_response(200, body))
    result = []
    for row in td.job_result_each(12345):
        result.append(row)
    td.get.assert_called_with("/v3/job/result/12345", {"format": "msgpack"})
    assert result == rows


def test_job_result_json_success():
    td = api.API("APIKEY")
    rows = [["foo", 123], ["bar", 456], ["baz", 789]]
    # result will be a JSON record per line (#4)
    body = "\n".join([json.dumps(row) for row in rows]).encode("utf-8")
    td.get = mock.MagicMock(return_value=make_response(200, body))
    result = td.job_result_format(12345, "json")
    td.get.assert_called_with("/v3/job/result/12345", {"format": "json"})
    assert result == rows


def test_job_result_json_each_success():
    td = api.API("APIKEY")
    rows = [["foo", 123], ["bar", 456], ["baz", 789]]
    # result will be a JSON record per line (#4)
    body = "\n".join([json.dumps(row) for row in rows]).encode("utf-8")
    td.get = mock.MagicMock(return_value=make_response(200, body))
    result = []
    for row in td.job_result_format_each(12345, "json"):
        result.append(row)
    td.get.assert_called_with("/v3/job/result/12345", {"format": "json"})
    assert result == rows


def test_kill_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "former_status": "foo"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.kill(12345)
    td.post.assert_called_with("/v3/job/kill/12345")


def test_query_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "job": "12345",
            "database": "sample_datasets",
            "job_id": "12345"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    job_id = td.query(
        "SELECT COUNT(1) FROM nasdaq", db="sample_datasets", priority="HIGH"
    )
    td.post.assert_called_with(
        "/v3/job/issue/hive/sample_datasets",
        {"query": "SELECT COUNT(1) FROM nasdaq", "priority": 1},
    )
    assert job_id == "12345"


def test_query_priority_unknown():
    td = api.API("APIKEY")
    td.post = mock.MagicMock(return_value=make_response(200, b""))
    with pytest.raises(ValueError) as error:
        td.query(
            "SELECT COUNT(1) FROM nasdaq", db="sample_datasets", priority="unknown"
        )
