#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from tdclient import client
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_client_apikey():
    td = client.Client("foo")
    assert td.api.apikey == "foo"
    assert td.apikey == "foo"

def test_server_status():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.server_status = mock.MagicMock()
    td.server_status()
    td.api.server_status.assert_called_with()

def test_create_database():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_database = mock.MagicMock()
    td.create_database("db_name", param1="param1")
    td.api.create_database.assert_called_with("db_name", param1="param1")

def test_delete_database():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.delete_database = mock.MagicMock()
    td.delete_database("db_name")
    td.api.delete_database.assert_called_with("db_name")

def test_account():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.show_account = mock.MagicMock(return_value=(1, 0, 10, 0, 4, "2015-01-13 17:17:17 UTC"))
    account = td.account()
    td.api.show_account.assert_called_with()
    assert account.account_id == 1

def test_core_utilization():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.account_core_utilization = mock.MagicMock(return_value=(0, 1, 2, []))
    core_utilization = td.core_utilization(0, 1)
    td.api.account_core_utilization.assert_called_with(0, 1)

def test_databases():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_databases = mock.MagicMock(return_value=({"sample_datasets": [{"name":"nasdaq"}, {"name":"www_access"}]}))
    databases = td.databases()
    td.api.list_databases.assert_called_with()
    assert len(databases) == 1

def test_create_log_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_log_table = mock.MagicMock()
    td.create_log_table("db_name", "table_name")
    td.api.create_log_table.assert_called_with("db_name", "table_name")

def test_create_item_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_item_table = mock.MagicMock()
    td.create_item_table("db_name", "table_name", "primary_key", "primary_key_type")
    td.api.create_item_table.assert_called_with("db_name", "table_name", "primary_key", "primary_key_type")

def test_swap_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.swap_table = mock.MagicMock()
    td.swap_table("db_name", "table_name1", "table_name2")
    td.api.swap_table.assert_called_with("db_name", "table_name1", "table_name2")

def test_update_schema():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.update_schema = mock.MagicMock()
    td.update_schema("db_name", "table_name", {"schema":"v"})
    td.api.update_schema.assert_called_with("db_name", "table_name", "{\"schema\": \"v\"}")

def test_update_expire():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.update_expire = mock.MagicMock()
    td.update_expire("db_name", "table_name", 7)
    td.api.update_expire.assert_called_with("db_name", "table_name", 7)

def test_delete_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.delete_table = mock.MagicMock()
    td.delete_table("db_name", "table_name")
    td.api.delete_table.assert_called_with("db_name", "table_name")

def test_tables():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_tables = mock.MagicMock(return_value=({"nasdaq": ["item", "", 100], "www_access": ["item", "", 200]}))
    td.tables("sample_datasets")
    td.api.list_tables.assert_called_with("sample_datasets")

def test_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_tables = mock.MagicMock(return_value=({"nasdaq": ["item", "", 100], "www_access": ["item", "", 200]}))
    table = td.table("sample_datasets", "nasdaq")
    td.api.list_tables.assert_called_with("sample_datasets")
    assert table.table_name == "nasdaq"

def test_tail():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.tail = mock.MagicMock()
    td.tail("sample_datasets", "nasdaq", 3)
    td.api.tail.assert_called_with("sample_datasets", "nasdaq", 3, None, None, None)

def test_query():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.query = mock.MagicMock(return_value=("12345"))
    job = td.query("sample_datasets", "SELECT 1", type="presto")
    td.api.query.assert_called_with("SELECT 1", "presto", "sample_datasets", None, None, None)
    assert job.job_id == "12345"

def test_jobs():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_jobs = mock.MagicMock(return_value=([]))
    jobs = td.jobs(0, 3)
    td.api.list_jobs.assert_called_with(0, 3, None, None)
    assert len(jobs) == 0

def test_job():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.show_job = mock.MagicMock(return_value=(["type", "query", "status", "url", "debug", "start_at", "end_at", "cpu_time", "result_size", "result_url", "hive_result_schema", "priority", "retry_limit", "org", "db"]))
    job = td.job("12345")
    td.api.show_job.assert_called_with("12345")
    assert job.job_id == "12345"

def test_job_status():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.job_status = mock.MagicMock()
    job = td.job_status("12345")
    td.api.job_status.assert_called_with("12345")

def test_job_result():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.job_result = mock.MagicMock()
    job = td.job_result("12345")
    td.api.job_result.assert_called_with("12345")

def test_job_result_format():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.job_result_format = mock.MagicMock()
    job = td.job_result_format("12345", "json")
    td.api.job_result_format.assert_called_with("12345", "json", None, None)

def test_kill():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.kill = mock.MagicMock()
    job = td.kill("12345")
    td.api.kill.assert_called_with("12345")
