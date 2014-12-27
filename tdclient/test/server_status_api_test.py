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

def test_server_status_success():
    client = api.API("apikey")
    body = """
      {
          "status": "ok"
      }
    """
    response = Response(200, body, {})
    client.get = functools.partial(get, response)
    assert client.server_status() == "ok"
    assert response.request_method == "GET"
    assert response.request_path == "/v3/system/server_status"

def test_server_status_failure():
    client = api.API("apikey")
    response = Response(500, "", {})
    client.get = functools.partial(get, response)
    assert client.server_status() == "Server is down (500)"
    assert response.request_method == "GET"
    assert response.request_path == "/v3/system/server_status"
