#!/usr/bin/env python

from unittest import mock

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_table():
    client = mock.MagicMock()
    table = models.Table(
        client,
        "db_name",
        "table_name",
        "type",
        "schema",
        12345,
        created_at="created_at",
        updated_at="updated_at",
        estimated_storage_size=67890,
        last_import="last_import",
        last_log_timestamp="last_log_timestamp",
        expire_days="expire_days",
        primary_key="primary_key",
        primary_key_type="primary_key_type",
    )
    assert table.type == "type"
    assert table.db_name == "db_name"
    assert table.table_name == "table_name"
    assert table.schema == "schema"
    assert table.count == 12345
    assert table.estimated_storage_size == 67890
    assert table.primary_key == "primary_key"
    assert table.primary_key_type == "primary_key_type"
    assert table.database_name == "db_name"
    assert table.name == "table_name"
    assert table.created_at == "created_at"
    assert table.updated_at == "updated_at"
    assert table.last_import == "last_import"
    assert table.last_log_timestamp == "last_log_timestamp"
    assert table.expire_days == "expire_days"
    assert table.identifier == "db_name.table_name"


def test_table_permission():
    client = mock.MagicMock()
    table = models.Table(client, "sample_datasets", "nasdaq", "log", "schema", 12345)
    client.database().permission = "permission"
    assert table.permission == "permission"
    client.database.assert_called_with("sample_datasets")


def test_table_estimated_storage_size_string():
    client = mock.MagicMock()
    table1 = models.Table(
        client,
        "db_name",
        "table_name",
        "type",
        "schema",
        12345,
        estimated_storage_size=1,
    )
    assert table1.estimated_storage_size_string == "0.0 GB"
    table2 = models.Table(
        client,
        "db_name",
        "table_name",
        "type",
        "schema",
        12345,
        estimated_storage_size=50 * 1024 * 1024,
    )
    assert table2.estimated_storage_size_string == "0.01 GB"
    table3 = models.Table(
        client,
        "db_name",
        "table_name",
        "type",
        "schema",
        12345,
        estimated_storage_size=50 * 1024 * 1024 * 1024,
    )
    assert table3.estimated_storage_size_string == "50.0 GB"
    table4 = models.Table(
        client,
        "db_name",
        "table_name",
        "type",
        "schema",
        12345,
        estimated_storage_size=300 * 1024 * 1024 * 1024,
    )
    assert table4.estimated_storage_size_string == "300 GB"
