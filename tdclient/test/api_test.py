#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from unittest import mock
except ImportError:
    import mock
import os
import pytest

from tdclient import api
from tdclient import version
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_apikey_success():
    td = api.API("apikey")
    assert td.apikey == "apikey"

def test_apikey_from_environ():
    os.environ["TD_API_KEY"] = "APIKEY"
    td = api.API()
    assert td.apikey == "APIKEY"

def test_apikey_failure():
    with pytest.raises(RuntimeError) as error:
        api.API()
    assert error.value.args == ("no API key given",)

def test_default_user_agent():
    td = api.API("apikey")
    assert td._user_agent.startswith("TD-Client-Python: %s" % version.__version__)

def test_user_agent_from_keyword():
    td = api.API("apikey", user_agent="user_agent")
    assert td._user_agent == "user_agent"

def test_default_endpoint():
    td = api.API("apikey")
    assert td._ssl == True
    assert td._host == "api.treasuredata.com"
    assert td._port == 443
    assert td._base_path == "/"

def test_endpoint_from_environ():
    os.environ["TD_API_SERVER"] = "http://api1.example.com"
    td = api.API("apikey")
    assert td._ssl == False
    assert td._host == "api1.example.com"
    assert td._port == 80
    assert td._base_path == ""

def test_endpoint_from_keyword():
    td = api.API("apikey", endpoint="http://api2.example.com")
    assert td._ssl == False
    assert td._host == "api2.example.com"
    assert td._port == 80
    assert td._base_path == ""

def test_endpoint_prefer_keyword():
    os.environ["TD_API_SERVER"] = "http://api1.example.com"
    td = api.API("apikey", endpoint="http://api2.example.com")
    assert td._ssl == False
    assert td._host == "api2.example.com"
    assert td._port == 80
    assert td._base_path == ""

def test_http_endpoint_with_custom_port():
    td = api.API("apikey", endpoint="http://api.example.com:8080/")
    assert td._ssl == False
    assert td._host == "api.example.com"
    assert td._port == 8080
    assert td._base_path == "/"

def test_https_endpoint():
    td = api.API("apikey", endpoint="https://api.example.com/")
    assert td._ssl == True
    assert td._host == "api.example.com"
    assert td._port == 443
    assert td._base_path == "/"

def test_https_endpoint_with_custom_path():
    td = api.API("apikey", endpoint="https://api.example.com/v1/")
    assert td._ssl == True
    assert td._host == "api.example.com"
    assert td._port == 443
    assert td._base_path == "/v1/"

def test_http_proxy_from_environ():
    os.environ["HTTP_PROXY"] = "proxy1.example.com:8080"
    td = api.API("apikey")
    assert td._http_proxy == "proxy1.example.com:8080"

def test_http_proxy_from_keyword():
    td = api.API("apikey", http_proxy="proxy2.example.com:8080")
    assert td._http_proxy == "proxy2.example.com:8080"

def test_http_proxy_prefer_keyword():
    os.environ["HTTP_PROXY"] = "proxy1.example.com:8080"
    td = api.API("apikey", http_proxy="proxy2.example.com:8080")
    assert td._http_proxy == "proxy2.example.com:8080"

def test_http_proxy_with_scheme():
    os.environ["HTTP_PROXY"] = "http://proxy1.example.com:8080/"
    td = api.API("apikey")
    assert td._http_proxy == "proxy1.example.com:8080"

def test_get_success():
    td = api.API("APIKEY")
    http = mock.MagicMock()
    http.getresponse = mock.MagicMock(return_value=response(200, b"body"))
    td.new_http = mock.MagicMock(return_value=(http, {}))
    with td.get("/foo", {"bar": "baz"}) as res:
        status, body = res.status, res.read()
        http.request.assert_called_with("GET", "/foo?bar=baz", headers={"Accept-Encoding": "deflate, gzip"})
        assert status == 200
        assert body == b"body"

def test_post_success():
    td = api.API("APIKEY")
    http = mock.MagicMock()
    http.getresponse = mock.MagicMock(return_value=response(200, b"body"))
    td.new_http = mock.MagicMock(return_value=(http, {}))
    with td.post("/foo", {"bar": "baz"}) as res:
        status, body = res.status, res.read()
        http.request.assert_called_with("POST", "/foo", "bar=baz", headers={})
        assert status == 200
        assert body == b"body"

def test_put_success():
    td = api.API("APIKEY")
    http = mock.MagicMock()
    http.getresponse = mock.MagicMock(return_value=response(200, b"body"))
    td.new_http = mock.MagicMock(return_value=(http, {}))
    with td.put("/foo", b"body", 7) as res:
        status, body = res.status, res.read()
        http.request.assert_called_with("PUT", "/foo", b"body", headers={"Content-Length": "7", "Content-Type": "application/octet-stream"})
        assert status == 200
        assert body == b"body"
