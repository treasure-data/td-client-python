#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from tdclient import api

def test_apikey():
    client = api.API("apikey")
    assert client.apikey == "apikey"
    assert client._user_agent.startswith("TD-Client-Python:")

def test_custom_user_agent():
    client = api.API("apikey", user_agent="user_agent")
    assert client._user_agent == "user_agent"
