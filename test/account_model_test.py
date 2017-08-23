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

def test_account():
    client = mock.MagicMock()
    account = models.Account(client, 1, 2, storage_size=3456, guaranteed_cores=7, maximum_cores=8, created_at="created_at")
    assert account.account_id == 1
    assert account.plan == 2
    assert account.storage_size == 3456
    assert account.guaranteed_cores == 7
    assert account.maximum_cores == 8
    assert account.created_at == "created_at"

def test_account_storage_size_string():
    client = mock.MagicMock()
    account1 = models.Account(client, 1, 1, storage_size=0)
    assert account1.storage_size_string == "0.0 GB"
    account2 = models.Account(client, 1, 1, storage_size=50*1024*1024)
    assert account2.storage_size_string == "0.01 GB"
    account3 = models.Account(client, 1, 1, storage_size=50*1024*1024*1024)
    assert account3.storage_size_string == "50.0 GB"
    account4 = models.Account(client, 1, 1, storage_size=300*1024*1024*1024)
    assert account4.storage_size_string == "300 GB"
