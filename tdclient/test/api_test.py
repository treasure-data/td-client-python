#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import os

from tdclient import api
from tdclient import version

def setup_function(function):
    try:
        del os.environ["TD_API_SERVER"]
    except KeyError:
        pass

def test_apikey():
    client = api.API("apikey")
    assert client.apikey == "apikey"

def test_default_user_agent():
    client = api.API("apikey")
    assert client._user_agent.startswith("TD-Client-Python: %s" % version.__version__)

def test_default_endpoint():
    client = api.API("apikey")
    assert client._ssl == True
    assert client._host == "api.treasuredata.com"
    assert client._port == 443
    assert client._base_path == "/"

def test_custom_user_agent():
    client = api.API("apikey", user_agent="user_agent")
    assert client._user_agent == "user_agent"

def test_custom_http_endpoint():
    client = api.API("apikey", endpoint="http://api.example.com")
    assert client._ssl == False
    assert client._host == "api.example.com"
    assert client._port == 80
    assert client._base_path == ""

def test_custom_http_endpoint_with_custom_port():
    client = api.API("apikey", endpoint="http://api.example.com:8080/")
    assert client._ssl == False
    assert client._host == "api.example.com"
    assert client._port == 8080
    assert client._base_path == "/"

def test_custom_https_endpoint():
    client = api.API("apikey", endpoint="https://api.example.com/")
    assert client._ssl == True
    assert client._host == "api.example.com"
    assert client._port == 443
    assert client._base_path == "/"

def test_custom_https_endpoint_with_custom_path():
    client = api.API("apikey", endpoint="https://api.example.com/v1/")
    assert client._ssl == True
    assert client._host == "api.example.com"
    assert client._port == 443
    assert client._base_path == "/v1/"
