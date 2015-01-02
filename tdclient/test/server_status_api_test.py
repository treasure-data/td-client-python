#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import functools

from tdclient import api
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_server_status_success():
    client = api.API("apikey")
    body = """
        {
            "status": "ok"
        }
    """
    response = Response(200, body, {})
    client.get = functools.partial(get, response)
    server_status = client.server_status()
    assert response.request_method == "GET"
    assert response.request_path == "/v3/system/server_status"
    assert server_status == "ok"

def test_server_status_failure():
    client = api.API("apikey")
    response = Response(500, "", {})
    client.get = functools.partial(get, response)
    server_status = client.server_status()
    assert response.request_method == "GET"
    assert response.request_path == "/v3/system/server_status"
    assert server_status == "Server is down (500)"
