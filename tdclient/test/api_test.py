#!/usr/bin/env python
# coding=utf-8

import io
import os
import tempfile
import time
import urllib.parse as urlparse
from array import array
from unittest import mock

import pytest
import urllib3

from tdclient import api, version
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
    with pytest.raises(ValueError) as error:
        api.API()
    assert error.value.args == ("no API key given",)


def test_default_user_agent():
    td = api.API("apikey")
    assert td._user_agent.startswith("TD-Client-Python/%s" % version.__version__)


def test_user_agent_from_keyword():
    td = api.API("apikey", user_agent="user_agent")
    assert td._user_agent == "user_agent"


def test_default_endpoint():
    td = api.API("apikey")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "https://api.treasuredata.com/" == url


def test_endpoint_from_environ():
    os.environ["TD_API_SERVER"] = "http://api1.example.com"
    td = api.API("apikey")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "http://api1.example.com" == url


def test_endpoint_from_keyword():
    td = api.API("apikey", endpoint="http://api2.example.com")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "http://api2.example.com" == url


def test_endpoint_prefer_keyword():
    os.environ["TD_API_SERVER"] = "http://api1.example.com"
    td = api.API("apikey", endpoint="http://api2.example.com")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "http://api2.example.com" == url


def test_http_endpoint_with_custom_port():
    td = api.API("apikey", endpoint="http://api.example.com:8080/")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "http://api.example.com:8080/" == url


def test_https_endpoint():
    td = api.API("apikey", endpoint="https://api.example.com/")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "https://api.example.com/" == url


def test_https_endpoint_with_custom_path():
    td = api.API("apikey", endpoint="https://api.example.com/v1/")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "https://api.example.com/v1/" == url


def test_https_endpoint():
    td = api.API("apikey", endpoint="api.example.com")
    assert isinstance(td.http, urllib3.PoolManager)
    url, headers = td.build_request()
    assert "https://api.example.com" == url


def test_http_proxy_from_environ():
    os.environ["HTTP_PROXY"] = "proxy1.example.com:8080"
    td = api.API("apikey")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy1.example.com:8080"
    assert td.http.proxy_headers == {}


def test_http_proxy_from_keyword():
    td = api.API("apikey", http_proxy="proxy2.example.com:8080")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy2.example.com:8080"
    assert td.http.proxy_headers == {}


def test_http_proxy_prefer_keyword():
    os.environ["HTTP_PROXY"] = "proxy1.example.com:8080"
    td = api.API("apikey", http_proxy="proxy2.example.com:8080")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy2.example.com:8080"
    assert td.http.proxy_headers == {}


def test_http_proxy_with_scheme():
    td = api.API("apikey", http_proxy="http://proxy1.example.com:8080/")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy1.example.com:8080"
    assert td.http.proxy_headers == {}


def test_http_proxy_with_credentials():
    td = api.API("apikey", http_proxy="john:doe@proxy1.example.com:8080")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy1.example.com:8080"
    assert td.http.proxy_headers == {"proxy-authorization": "Basic am9objpkb2U="}


def test_http_proxy_with_scheme_and_credentials():
    td = api.API("apikey", http_proxy="http://john:doe@proxy1.example.com:8080/")
    assert isinstance(td.http, urllib3.ProxyManager)
    assert td.http.proxy.url == "http://proxy1.example.com:8080"
    assert td.http.proxy_headers == {"proxy-authorization": "Basic am9objpkb2U="}


def test_no_timeout():
    with mock.patch("tdclient.api.urllib3") as urllib3:
        td = api.API("apikey")
        assert urllib3.PoolManager.called
        args, kwargs = urllib3.PoolManager.call_args
        assert kwargs["timeout"] == 60


def test_timeout():
    with mock.patch("tdclient.api.urllib3") as urllib3:
        td = api.API("apikey", timeout=12345)
        assert urllib3.PoolManager.called
        args, kwargs = urllib3.PoolManager.call_args
        assert kwargs["timeout"] == 12345


def test_get_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.get("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("GET", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "accept-encoding",
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_get_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.get("/hoge", {"fuga": "ふが"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("GET", "https://api.treasuredata.com/hoge")
            assert kwargs["fields"] == {"fuga": "ふが"}
            assert sorted(kwargs["headers"].keys()) == [
                "accept-encoding",
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_get_error():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(404, b"not found")]
        td.http.request.side_effect = responses
        with td.get("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("GET", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "accept-encoding",
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 404
            assert body == b"not found"
        assert not t_sleep.called


def test_get_retry_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [
            make_raw_response(500, b"failure1"),
            make_raw_response(503, b"failure2"),
            make_raw_response(200, b"success1"),
        ]
        td.http.request.side_effect = responses
        with td.get("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("GET", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "accept-encoding",
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"success1"
            assert t_sleep.called
            sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
            assert len(sleeps) == len(responses) - 1
            assert sum(sleeps) < td._max_cumul_retry_delay


def test_get_failure():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        td.http.request.return_value = make_raw_response(500, b"failure")
        with pytest.raises(api.APIError) as error:
            with td.get("/foo", {"bar": "baz"}) as response:
                pass
        assert t_sleep.called
        sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
        assert td._max_cumul_retry_delay < sum(sleeps)


def test_post_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.post("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("POST", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_post_bytearray_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.urlopen.side_effect = responses
        bytes_or_stream = bytearray(b"abcd")
        with td.post("/foo", bytes_or_stream) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("POST", "https://api.treasuredata.com/foo")
            assert kwargs["body"] == bytes_or_stream
            if urllib3.util.IS_PYOPENSSL:
                assert isinstance(kwargs["body"], memoryview)
            else:
                assert isinstance(kwargs["body"], bytearray)
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_post_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.post("/hoge", {"fuga": "ふが"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("POST", "https://api.treasuredata.com/hoge")
            assert kwargs["fields"] == {"fuga": "ふが"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_post_retry_success():
    td = api.API("APIKEY", retry_post_requests=True)
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [
            make_raw_response(500, b"failure1"),
            make_raw_response(503, b"failure2"),
            make_raw_response(200, b"success1"),
        ]
        td.http.request.side_effect = responses
        with td.post("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("POST", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"success1"
            assert t_sleep.called
            sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
            assert len(sleeps) == len(responses) - 1
            assert sum(sleeps) < td._max_cumul_retry_delay


def test_post_never_retry():
    td = api.API("APIKEY", retry_post_requests=False)
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(500, b"failure")]
        td.http.request.side_effect = responses
        with pytest.raises(api.APIError) as error:
            with td.post("/foo", {"bar": "baz"}) as response:
                pass
        assert not t_sleep.called


def test_post_failure():
    td = api.API("APIKEY", retry_post_requests=True)
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        td.http.request.return_value = make_raw_response(500, b"failure")
        with pytest.raises(api.APIError) as error:
            with td.post("/foo", {"bar": "baz"}) as response:
                pass
        assert t_sleep.called
        sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
        assert td._max_cumul_retry_delay < sum(sleeps)


def test_put_bytes_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        bytes_or_stream = b"request body"
        td.http.urlopen.side_effect = responses
        with td.put("/foo", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/foo")
            if urllib3.util.IS_PYOPENSSL:
                assert kwargs["body"] == b"request body"
            else:
                assert kwargs["body"] == array(str("b"), bytes_or_stream)
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_bytes_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        bytes_or_stream = "リクエストボディー".encode("utf-8")
        td.http.urlopen.side_effect = responses
        with td.put("/hoge", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/hoge")
            if urllib3.util.IS_PYOPENSSL:
                assert kwargs["body"] == "リクエストボディー".encode("utf-8")
            else:
                assert kwargs["body"] == array(str("b"), bytes_or_stream)
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_file_with_fileno_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        td.http.urlopen.side_effect = responses
        bytes_or_stream = tempfile.TemporaryFile()
        bytes_or_stream.write(b"request body")
        bytes_or_stream.seek(0)
        with td.put("/foo", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/foo")
            assert kwargs["body"] == bytes_or_stream
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_file_with_fileno_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        td.http.urlopen.side_effect = responses
        bytes_or_stream = tempfile.TemporaryFile()
        bytes_or_stream.write("リクエストボディー".encode("utf-8"))
        bytes_or_stream.seek(0)
        with td.put("/hoge", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/hoge")
            assert kwargs["body"] == bytes_or_stream
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_file_without_fileno_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        td.http.urlopen.side_effect = responses
        bytes_or_stream = io.BytesIO(b"request body")
        with td.put("/foo", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/foo")
            if urllib3.util.IS_PYOPENSSL:
                assert kwargs["body"] == b"request body"
            else:
                assert kwargs["body"] == array(str("b"), bytes_or_stream.getvalue())
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_file_without_fileno_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.urlopen = mock.MagicMock()
        responses = [make_raw_response(200, b"response body")]
        td.http.urlopen.side_effect = responses
        bytes_or_stream = io.BytesIO("リクエストボディー".encode("utf-8"))
        with td.put("/hoge", bytes_or_stream, 12) as response:
            args, kwargs = td.http.urlopen.call_args
            assert args == ("PUT", "https://api.treasuredata.com/hoge")
            if urllib3.util.IS_PYOPENSSL:
                assert kwargs["body"] == "リクエストボディー".encode("utf-8")
            else:
                assert kwargs["body"] == array(str("b"), bytes_or_stream.getvalue())
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "content-length",
                "content-type",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"response body"
        assert not t_sleep.called


def test_put_failure():
    td = api.API("APIKEY")
    td.http.urlopen = mock.MagicMock()
    td.http.urlopen.return_value = make_raw_response(500, b"error")
    with pytest.raises(api.APIError) as error:
        with td.put("/foo", b"body", 7) as response:
            pass


def test_delete_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.delete("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("DELETE", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_delete_unicode_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(200, b"body")]
        td.http.request.side_effect = responses
        with td.delete("/hoge", {"fuga": "ふが"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("DELETE", "https://api.treasuredata.com/hoge")
            assert kwargs["fields"] == {"fuga": "ふが"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"body"
        assert not t_sleep.called


def test_delete_error():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [make_raw_response(404, b"not found")]
        td.http.request.side_effect = responses
        with td.delete("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("DELETE", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 404
            assert body == b"not found"
        assert not t_sleep.called


def test_delete_retry_success():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        responses = [
            make_raw_response(500, b"failure1"),
            make_raw_response(503, b"failure2"),
            make_raw_response(200, b"success1"),
        ]
        td.http.request.side_effect = responses
        with td.delete("/foo", {"bar": "baz"}) as response:
            args, kwargs = td.http.request.call_args
            assert args == ("DELETE", "https://api.treasuredata.com/foo")
            assert kwargs["fields"] == {"bar": "baz"}
            assert sorted(kwargs["headers"].keys()) == [
                "authorization",
                "date",
                "user-agent",
            ]
            status, body = response.status, response.read()
            assert status == 200
            assert body == b"success1"
            assert t_sleep.called
            sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
            assert len(sleeps) == len(responses) - 1
            assert sum(sleeps) < td._max_cumul_retry_delay


def test_delete_failure():
    td = api.API("APIKEY")
    with mock.patch("time.sleep") as t_sleep:
        td.http.request = mock.MagicMock()
        td.http.request.return_value = make_raw_response(500, b"failure")
        with pytest.raises(api.APIError) as error:
            with td.delete("/foo", {"bar": "baz"}) as response:
                pass
        assert t_sleep.called
        sleeps = [args[0] for (args, kwargs) in t_sleep.call_args_list]
        assert td._max_cumul_retry_delay < sum(sleeps)


def test_raise_error_401():
    td = api.API("APIKEY")
    with pytest.raises(api.AuthError) as error:
        td.raise_error("msg", make_raw_response(401, b"unauthorized"), b"body")


def test_raise_error_403():
    td = api.API("APIKEY")
    with pytest.raises(api.ForbiddenError) as error:
        td.raise_error("msg", make_raw_response(403, b"forbidden"), b"body")


def test_raise_error_404():
    td = api.API("APIKEY")
    with pytest.raises(api.NotFoundError) as error:
        td.raise_error("msg", make_raw_response(404, b"not found"), b"body")


def test_raise_error_409():
    td = api.API("APIKEY")
    with pytest.raises(api.AlreadyExistsError) as error:
        td.raise_error("msg", make_raw_response(409, b"conflict"), b"body")


def test_raise_error_4xx():
    td = api.API("APIKEY")
    with pytest.raises(api.APIError) as error:
        td.raise_error("msg", make_raw_response(402, b"payment required"), b"body")


def test_checked_json_success():
    td = api.API("APIKEY")
    assert td.checked_json(b'{"foo": "FOO"}', ["foo"])
    assert td.checked_json(b'{"foo": "FOO", "bar": "BAR", "baz": "BAZ"}', ["foo"])
    assert td.checked_json(
        b'{"foo": "FOO", "bar": "BAR", "baz": "BAZ"}', ["foo", "bar"]
    )


def test_checked_json_load_error():
    td = api.API("APIKEY")
    with pytest.raises(api.APIError) as error:
        td.checked_json(b"{invalid}", [])


def test_checked_json_field_error():
    td = api.API("APIKEY")
    with pytest.raises(api.APIError) as error:
        td.checked_json(b"{}", ["foo"])
