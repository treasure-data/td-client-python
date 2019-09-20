#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import connection, cursor, errors
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_connection():
    td = connection.Connection(apikey="foo")
    assert td.api.apikey == "foo"


def test_connection_with_options():
    td = connection.Connection(apikey="APIKEY", endpoint="http://api.example.com/")
    assert td.api.apikey == "APIKEY"
    assert td.api._endpoint == "http://api.example.com/"


def test_connection_with_cursor_options():
    td = connection.Connection(
        apikey="APIKEY",
        endpoint="http://api.example.com/",
        type="presto",
        db="sample_datasets",
        result_url="mysql://db.example.com",
        priority="HIGH",
        retry_limit=3,
        wait_interval=5,
        wait_callback=repr,
    )
    with mock.patch("tdclient.connection.cursor.Cursor") as Cursor:
        td.cursor()
        assert Cursor.called
        args, kwargs = Cursor.call_args
        assert kwargs.get("type") == "presto"
        assert kwargs.get("db") == "sample_datasets"
        assert kwargs.get("result_url") == "mysql://db.example.com"
        assert kwargs.get("priority") == "HIGH"
        assert kwargs.get("retry_limit") == 3
        assert kwargs.get("wait_interval") == 5
        assert kwargs.get("wait_callback") == repr


def test_connection_close():
    td = connection.Connection(apikey="APIKEY")
    td._api = mock.MagicMock()
    td.close()
    assert td._api.close.called


def test_connection_commit():
    td = connection.Connection(apikey="APIKEY")
    with pytest.raises(errors.NotSupportedError) as error:
        td.commit()


def test_connection_rollback():
    td = connection.Connection(apikey="APIKEY")
    with pytest.raises(errors.NotSupportedError) as error:
        td.rollback()
