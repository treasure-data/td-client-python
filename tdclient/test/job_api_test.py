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
    assert sorted([ job[0] for job in jobs ]) == ["18879199", "18880612", "18882028"]

def test_list_jobs_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    with pytest.raises(api.APIError) as error:
        td.list_jobs(0, 2)
    assert error.value.args == ("500: List jobs failed: error",)

def test_show_job_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "status": {
                "type": "presto",
                "database": "sample_datasets",
                "query": "SELECT COUNT(1) FROM nasdaq;",
                "status": "finished",
                "debug": 1,
                "url": "http://example.com",
                "start_at": "",
                "end_at": "",
                "cpu_time": "",
                "result_size": "",
                "result": "",
                "priority": "HIGH",
                "retry_liit": 3
            }
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.show_job(12345)
    td.get.assert_called_with("/v3/job/show/12345")

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

#def test_job_result_success():
#    td = api.API("APIKEY")
#    # TODO: should be replaced by wire dump
#    body = b"""
#        {
#            "status": "RUNNING"
#        }
#    """
#    td.get = mock.MagicMock(return_value=make_response(200, body))
#    jobs = td.job_result(12345)
#    td.get.assert_called_with("/v3/job/result/12345")

def test_job_result_raw_success():
    td = api.API("APIKEY")
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "foo": "bar"
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    jobs = td.job_result_raw(12345, "json")
    td.get.assert_called_with("/v3/job/result/12345", {"format": "json"})

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
    # TODO: should be replaced by wire dump
    body = b"""
        {
            "job_id": "12345"
        }
    """
    td.post = mock.MagicMock(return_value=make_response(200, body))
    job_id = td.query("SELECT COUNT(1) FROM nasdaq", db="sample_datasets", priority="HIGH")
    td.post.assert_called_with("/v3/job/issue/hive/sample_datasets", {"query": "SELECT COUNT(1) FROM nasdaq", "priority": "HIGH"})
    assert job_id == "12345"
