#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from tdclient import api

def test_apikey():
    apikey = "foo"
    client = api.API(apikey)
    assert apikey == apikey
