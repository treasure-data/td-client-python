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
    res = mock.MagicMock()
    res.status = 200
    td.get = mock.MagicMock(return_value=(res.status, body, res))
    jobs = td.list_jobs(0, 2)
    td.get.assert_called_with("/v3/job/list", {"from": "0", "to": "2"})
    assert len(jobs) == 3
    assert sorted([ job[0] for job in jobs ]) == ["18879199", "18880612", "18882028"]

def test_list_jobs_failure():
    td = api.API("APIKEY")
    res = mock.MagicMock()
    res.status = 500
    td.get = mock.MagicMock(return_value=(res.status, b"error", res))
    with pytest.raises(api.APIError) as error:
        td.list_jobs(0, 2)
    assert error.value.args == ("500: List jobs failed: error",)
