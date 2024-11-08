#!/usr/bin/env python

from unittest import mock

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_result():
    client = mock.MagicMock()
    result = models.Result(client, "name", "url", "org_name")
    assert result.name == "name"
    assert result.url == "url"
    assert result.org_name == "org_name"
