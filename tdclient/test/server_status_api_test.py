#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import api
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_server_status_success():
    td = api.API("APIKEY")
    body = b"""
        {
            "status": "ok"
        }
    """
    td.get = mock.MagicMock(return_value=make_response(200, body))
    assert td.server_status() == "ok"
    td.get.assert_called_with("/v3/system/server_status")


def test_server_status_failure():
    td = api.API("APIKEY")
    td.get = mock.MagicMock(return_value=make_response(500, b"error"))
    assert td.server_status() == "Server is down (500)"
    td.get.assert_called_with("/v3/system/server_status")
