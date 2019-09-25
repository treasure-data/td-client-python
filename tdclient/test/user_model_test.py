#!/usr/bin/env python

from unittest import mock

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_user():
    client = mock.MagicMock()
    user = models.User(client, "name", "org_name", ["role1", "role2"], "email")
    assert user.name == "name"
    assert user.org_name == "org_name"
    assert user.role_names == ["role1", "role2"]
    assert user.email == "email"
