#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import functools
import os

from tdclient import api
from tdclient import version

def setup_function(function):
    try:
        del os.environ["TD_API_SERVER"]
    except KeyError:
        pass
    try:
        del os.environ["HTTP_PROXY"]
    except KeyError:
        pass

class Response(object):
    def __init__(self, status, body, headers):
        self.status = status
        self.body = body.encode("utf-8")
        self.headers = headers
        self.request_method = None
        self.request_path = None
        self.request_headers = None

def get(response, url, params={}):
    response.request_method = "GET"
    response.request_path = url
    response.request_headers = params
    return (response.status, response.body, response)

def test_list_tables():
    client = api.API("apikey")
    body = """
        {
            "tables":[
                {"id":210906,"name":"nasdaq","estimated_storage_size":168205061,"counter_updated_at":null,"last_log_timestamp":null,"type":"log","count":8807278,"expire_days":null,"created_at":"2014-10-08 02:57:38 UTC","updated_at":"2014-10-08 03:16:59 UTC","schema":"[[\\"symbol\\",\\"string\\"],[\\"open\\",\\"double\\"],[\\"volume\\",\\"long\\"],[\\"high\\",\\"double\\"],[\\"low\\",\\"double\\"],[\\"close\\",\\"double\\"]]"},
                {"id":208715,"name":"www_access","estimated_storage_size":0,"counter_updated_at":"2014-10-04T01:13:20Z","last_log_timestamp":"2014-10-04T01:13:15Z","type":"log","count":5000,"expire_days":null,"created_at":"2014-10-04 01:13:12 UTC","updated_at":"2014-10-22 07:02:19 UTC","schema":"[[\\"user\\",\\"int\\"],[\\"host\\",\\"string\\"],[\\"path\\",\\"string\\"],[\\"referer\\",\\"string\\"],[\\"code\\",\\"long\\"],[\\"agent\\",\\"string\\"],[\\"size\\",\\"long\\"],[\\"method\\",\\"string\\"]]"}
            ],
            "database":"sample_datasets"
        }
    """
    response = Response(200, body, {})
    client.get = functools.partial(get, response)
    tables = client.list_tables("sample_datasets")
    assert response.request_method == "GET"
    assert response.request_path == "/v3/table/list/sample_datasets"
    assert len(tables) == 2
    assert sorted(tables.keys()) == ["nasdaq", "www_access"]
    assert sorted([ v[0] for v in tables.values() ]) == ["log", "log"]
