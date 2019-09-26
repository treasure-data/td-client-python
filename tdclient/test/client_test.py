#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import api, client
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


def test_databases():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_databases = mock.MagicMock(
        return_value=(
            {
                "sample_datasets": {
                    "nasdaq": {"name": "nasdaq"},
                    "www_access": {"name": "www_access"},
                }
            }
        )
    )
    databases = td.databases()
    td.api.list_databases.assert_called_with()
    assert len(databases) == 1


def test_database_success():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_databases = mock.MagicMock(
        return_value=(
            {
                "sample_datasets": {
                    "nasdaq": {"name": "nasdaq"},
                    "www_access": {"name": "www_access"},
                }
            }
        )
    )
    database = td.database("sample_datasets")
    td.api.list_databases.assert_called_with()
    assert database.name == "sample_datasets"


def test_database_failure():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_databases = mock.MagicMock(
        return_value=(
            {
                "sample_datasets": {
                    "nasdaq": {"name": "nasdaq"},
                    "www_access": {"name": "www_access"},
                }
            }
        )
    )
    with pytest.raises(api.NotFoundError) as error:
        td.database("not_exist")


def test_create_log_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_log_table = mock.MagicMock()
    td.create_log_table("db_name", "table_name")
    td.api.create_log_table.assert_called_with("db_name", "table_name")


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
    td.update_schema("db_name", "table_name", {"schema": "v"})
    td.api.update_schema.assert_called_with("db_name", "table_name", '{"schema": "v"}')


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
    td._api.list_tables = mock.MagicMock(
        return_value=(
            {
                "nasdaq": {"type": "item", "schema": [], "count": 100},
                "www_access": {"type": "item", "schema": [], "count": 200},
            }
        )
    )
    td.tables("sample_datasets")
    td.api.list_tables.assert_called_with("sample_datasets")


def test_table():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_tables = mock.MagicMock(
        return_value=(
            {
                "nasdaq": {"type": "item", "schema": [], "count": 100},
                "www_access": {"type ": "item", "schema": [], "count": 200},
            }
        )
    )
    table = td.table("sample_datasets", "nasdaq")
    td.api.list_tables.assert_called_with("sample_datasets")
    assert table.table_name == "nasdaq"


def test_tail():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.tail = mock.MagicMock()
    td.tail("sample_datasets", "nasdaq", 3)
    td.api.tail.assert_called_with("sample_datasets", "nasdaq", 3, None, None, None)


def test_change_database():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.change_database = mock.MagicMock()
    td.change_database("sample_datasets", "nasdaq", "new_db")
    td.api.change_database.assert_called_with("sample_datasets", "nasdaq", "new_db")


def test_query():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.query = mock.MagicMock(return_value=("12345"))
    job = td.query("sample_datasets", "SELECT 1", type="presto")
    td.api.query.assert_called_with(
        "SELECT 1",
        db="sample_datasets",
        type="presto",
        retry_limit=None,
        priority=None,
        result_url=None,
    )
    assert job.job_id == "12345"


def test_jobs():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_jobs = mock.MagicMock(return_value=[])
    jobs = td.jobs(0, 3)
    td.api.list_jobs.assert_called_with(0, 3, None, None)
    assert len(jobs) == 0


def test_job():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.show_job = mock.MagicMock(
        return_value={
            "job_id": "12345",
            "type": "presto",
            "url": "http://www.example.com/",
            "query": "SELECT COUNT(1) FROM nasdaq",
            "status": "success",
            "debug": "0",
            "start_at": "1970-01-01T00:00:00Z",
            "end_at": "1970-01-01T00:00:00Z",
            "cpu_time": "123.45",
            "result_size": 12345,
            "result": None,
            "result_url": None,
            "hive_result_schema": "",
            "priority": "NORMAL",
            "retry_limit": "retry_limit",
            "org_name": None,
            "database": "sample_datasets",
        }
    )
    job = td.job("12345")
    td.api.show_job.assert_called_with("12345")
    assert job.job_id == "12345"
    assert job.url == "http://www.example.com/"


def test_job_status():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.job_status = mock.MagicMock()
    job = td.job_status("12345")
    td.api.job_status.assert_called_with("12345")


def test_job_result():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    rows = [[123], [456]]
    td._api.job_result = mock.MagicMock(return_value=rows)
    result = td.job_result("12345")
    td.api.job_result.assert_called_with("12345")
    assert result == rows


def test_job_result_each():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    rows = [[123], [456]]
    td._api.job_result_each = mock.MagicMock(return_value=rows)
    result = []
    for row in td.job_result_each("12345"):
        result.append(row)
    td.api.job_result_each.assert_called_with("12345")
    assert result == rows


def test_job_result_format():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    rows = [[123], [456]]
    td._api.job_result_format = mock.MagicMock(return_value=rows)
    result = td.job_result_format("12345", "json")
    td.api.job_result_format.assert_called_with("12345", "json")
    assert result == rows


def test_job_result_format_each():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    rows = [[123], [456]]
    td._api.job_result_format_each = mock.MagicMock(return_value=rows)
    result = []
    for row in td.job_result_format_each("12345", "json"):
        result.append(row)
    td.api.job_result_format_each.assert_called_with("12345", "json")
    assert result == rows


def test_kill():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.kill = mock.MagicMock()
    job = td.kill("12345")
    td.api.kill.assert_called_with("12345")


def test_partial_delete():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.partial_delete = mock.MagicMock(return_value=("12345"))
    job = td.partial_delete("db_name", "table_name", 0, 2)
    td.api.partial_delete.assert_called_with("db_name", "table_name", 0, 2, {})
    assert job.job_id == "12345"


def test_create_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_bulk_import = mock.MagicMock()
    td.create_bulk_import("name", "database", "table")
    td.api.create_bulk_import.assert_called_with("name", "database", "table", {})


def test_delete_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.delete_bulk_import = mock.MagicMock()
    td.delete_bulk_import("name")
    td.api.delete_bulk_import.assert_called_with("name")


def test_freeze_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.freeze_bulk_import = mock.MagicMock()
    td.freeze_bulk_import("name")
    td.api.freeze_bulk_import.assert_called_with("name")


def test_unfreeze_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.unfreeze_bulk_import = mock.MagicMock()
    td.unfreeze_bulk_import("name")
    td.api.unfreeze_bulk_import.assert_called_with("name")


def test_perform_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.perform_bulk_import = mock.MagicMock(return_value=("12345"))
    job = td.perform_bulk_import("name")
    td.api.perform_bulk_import.assert_called_with("name")
    assert job.job_id == "12345"


def test_commit_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.commit_bulk_import = mock.MagicMock()
    td.commit_bulk_import("name")
    td.api.commit_bulk_import.assert_called_with("name")


def test_bulk_import_error_records():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.bulk_import_error_records = mock.MagicMock(return_value=[["foo"], ["bar"]])
    records = []
    for record in td.bulk_import_error_records("name"):
        records.append(record)
    td.api.bulk_import_error_records.assert_called_with("name")
    assert [["foo"], ["bar"]] == records


def test_bulk_import():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.show_bulk_import = mock.MagicMock(return_value=({"name": "foo"}))
    bulk_import = td.bulk_import("name")
    td.api.show_bulk_import.assert_called_with("name")
    assert bulk_import.name == "foo"


def test_bulk_imports():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_bulk_imports = mock.MagicMock(
        return_value=([{"name": "foo"}, {"name": "bar"}])
    )
    bulk_imports = td.bulk_imports()
    td.api.list_bulk_imports.assert_called_with()
    assert sorted([bulk_import.name for bulk_import in bulk_imports]) == ["bar", "foo"]


def test_bulk_import_upload_part():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.bulk_import_upload_part = mock.MagicMock()
    td.bulk_import_upload_part("name", "part_name", b"foo", 3)
    td.api.bulk_import_upload_part.assert_called_with("name", "part_name", b"foo", 3)


def test_bulk_import_delete_part():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.bulk_import_delete_part = mock.MagicMock()
    td.bulk_import_delete_part("name", "part_name")
    td.api.bulk_import_delete_part.assert_called_with("name", "part_name")


def test_list_bulk_import_parts():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_bulk_import_parts = mock.MagicMock()
    td.list_bulk_import_parts("name")
    td.api.list_bulk_import_parts.assert_called_with("name")


def test_create_schedule():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_schedule = mock.MagicMock()
    td.create_schedule("name", {"cron": "0 * * * *", "query": "SELECT 1"})
    td.api.create_schedule.assert_called_with(
        "name", {"cron": "0 * * * *", "query": "SELECT 1"}
    )


def test_delete_schedule():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.delete_schedule = mock.MagicMock()
    td.delete_schedule("name")
    td.api.delete_schedule("name")


def test_schedules():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_schedules = mock.MagicMock(return_value=[])
    schedules = td.schedules()
    td.api.list_schedules.assert_called_with()
    assert len(schedules) == 0


def test_update_schedule():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.update_schedule = mock.MagicMock()
    td.update_schedule("name", {"foo": "bar"})
    td.api.update_schedule("name", {"foo": "bar"})


def test_history():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.history = mock.MagicMock(
        return_value=[
            (
                "scheduled_at1",
                "job_id1",
                "type1",
                "status1",
                "query1",
                "start_at1",
                "end_at1",
                "result_url1",
                "priority1",
                "database1",
            ),
            (
                "scheduled_at2",
                "job_id2",
                "type2",
                "status2",
                "query2",
                "start_at2",
                "end_at2",
                "result_url2",
                "priority2",
                "database2",
            ),
            (
                "scheduled_at3",
                "job_id3",
                "type3",
                "status3",
                "query3",
                "start_at3",
                "end_at3",
                "result_url3",
                "priority3",
                "database3",
            ),
        ]
    )
    history = td.history("name", 0, 3)
    td.api.history.assert_called_with("name", 0, 3)
    assert len(history) == 3


def test_run_schedule():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.run_schedule = mock.MagicMock(return_value=[])
    scheduled_jobs = td.run_schedule("name", "time", "num")
    td.api.run_schedule.assert_called_with("name", "time", "num")
    assert len(scheduled_jobs) == 0


def test_import_data():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.import_data = mock.MagicMock()
    td.import_data(
        "db_name", "table_name", "format", "stream", 123, unique_id="unique_id"
    )
    td.api.import_data(
        "db_name", "table_name", "format", "stream", 123, unique_id="unique_id"
    )


def test_import_file():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.import_file = mock.MagicMock()
    td.import_file("db_name", "table_name", "format", "file", unique_id="unique_id")
    td.api.import_file("db_name", "table_name", "format", "file", unique_id="unique_id")


def test_results():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_result = mock.MagicMock()
    results = td.results()
    td.api.list_result.assert_called_with()
    assert len(results) == 0


def test_create_result():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.create_result = mock.MagicMock()
    td.create_result("name", "url", {"foo": "bar"})
    td.api.create_result("name", "url", {"foo": "bar"})


def test_delete_result():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.delete_result = mock.MagicMock()
    td.delete_result("name")
    td.api.delete_result("name")


def test_users():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_users = mock.MagicMock()
    users = td.users()
    td.api.list_users.assert_called_with()
    assert len(users) == 0


def test_add_user():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.add_user = mock.MagicMock()
    td.add_user("name", "org", "email", "password")
    td.api.add_user("name", "org", "email", "password")


def test_remove_user():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.remove_user = mock.MagicMock()
    td.remove_user("name")
    td.api.remove_user("name")


def test_list_apikeys():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.list_apikeys = mock.MagicMock()
    td.list_apikeys("user")
    td.api.list_apikeys("user")


def test_add_apikey():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.add_apikey = mock.MagicMock()
    td.add_apikey("user")
    td.api.add_apikey("user")


def test_remove_apikey():
    td = client.Client("APIKEY")
    td._api = mock.MagicMock()
    td._api.remove_apikey = mock.MagicMock()
    td.remove_apikey("user", "apikey")
    td.api.remove_apikey("user", "apikey")
