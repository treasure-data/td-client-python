#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from unittest import mock
except ImportError:
    import mock

from tdclient import models
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_access_control():
    client = mock.MagicMock()
    access_control = models.AccessControl(client, "subject", "action", "scope", "grant_option")
    assert access_control.subject == "subject"
    assert access_control.action == "action"
    assert access_control.scope == "scope"
    assert access_control.grant_option == "grant_option"
