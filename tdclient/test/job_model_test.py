#!/usr/bin/env python

import datetime
from unittest import mock

import dateutil.tz
import pytest

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_schema():
    client = mock.MagicMock()
    job = models.Job(
        client,
        "job_id",
        "type",
        "query",
        status="status",
        url="url",
        debug="debug",
        start_at="start_at",
        end_at="end_at",
        cpu_time="cpu_time",
        result_size="result_size",
        result="result",
        result_url="result_url",
        hive_result_schema=[["_c1", "string"], ["_c2", "bigint"]],
        priority="UNKNOWN",
        retry_limit="retry_limit",
        org_name="org_name",
        database="database",
    )
    assert job.id == "job_id"
    assert job.job_id == "job_id"
    assert job.type == "type"
    assert job.result_url == "result_url"
    assert job.priority == "UNKNOWN"
    assert job.retry_limit == "retry_limit"
    assert job.org_name == "org_name"
    assert job.database == "database"
    assert job.result_schema == [["_c1", "string"], ["_c2", "bigint"]]


def test_job_priority():
    client = mock.MagicMock()
    assert (
        models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", priority=-2
        ).priority
        == "VERY LOW"
    )
    assert (
        models.Job(
            client, "2", "hive", "SELECT COUNT(1) FROM nasdaq", priority=-1
        ).priority
        == "LOW"
    )
    assert (
        models.Job(
            client, "3", "hive", "SELECT COUNT(1) FROM nasdaq", priority=0
        ).priority
        == "NORMAL"
    )
    assert (
        models.Job(
            client, "4", "hive", "SELECT COUNT(1) FROM nasdaq", priority=1
        ).priority
        == "HIGH"
    )
    assert (
        models.Job(
            client, "5", "hive", "SELECT COUNT(1) FROM nasdaq", priority=2
        ).priority
        == "VERY HIGH"
    )
    assert (
        models.Job(
            client, "42", "hive", "SELECT COUNT(1) FROM nasdaq", priority=42
        ).priority
        == "42"
    )


def test_job_wait_success():
    client = mock.MagicMock()
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.finished = mock.MagicMock(side_effect=[False, True])
    job.update = mock.MagicMock()
    with mock.patch("time.time") as t_time:
        t_time.side_effect = [1423570800.0, 1423570860.0, 1423570920.0, 1423570980.0]
        with mock.patch("time.sleep") as t_sleep:
            job.wait(timeout=120)
            assert t_sleep.called
        assert t_time.called
    assert job.finished.called
    assert job.update.called


def test_job_wait_failure():
    client = mock.MagicMock()
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.finished = mock.MagicMock(return_value=False)
    job.update = mock.MagicMock()
    with mock.patch("time.time") as t_time:
        t_time.side_effect = [1423570800.0, 1423570860.0, 1423570920.0, 1423570980.0]
        with mock.patch("time.sleep") as t_sleep:
            with pytest.raises(RuntimeError) as error:
                job.wait(timeout=120)
                assert t_sleep.called
        assert t_time.called
    assert job.finished.called
    assert not job.update.called


def test_job_kill():
    client = mock.MagicMock()
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.update = mock.MagicMock()
    job.kill()
    client.kill.assert_called_with("12345")
    assert job.update.called


def test_job_result_generator():
    client = mock.MagicMock()

    def job_result_each(job_id):
        assert job_id == "12345"
        yield ["foo", 123]
        yield ["bar", 456]
        yield ["baz", 789]

    client.job_result_each = job_result_each
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.success = mock.MagicMock(return_value=True)
    job.update = mock.MagicMock()
    rows = []
    for row in job.result():
        rows.append(row)
    assert rows == [["foo", 123], ["bar", 456], ["baz", 789]]
    assert job.update.called


def test_job_result_list():
    client = mock.MagicMock()
    result = [["foo", 123], ["bar", 456], ["baz", 789]]
    job = models.Job(
        client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq", result=result
    )
    job.success = mock.MagicMock(return_value=True)
    job.update = mock.MagicMock()
    rows = []
    for row in job.result():
        rows.append(row)
    assert rows == [["foo", 123], ["bar", 456], ["baz", 789]]
    assert job.update.called


def test_job_result_failure():
    client = mock.MagicMock()
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.success = mock.MagicMock(return_value=False)
    job.update = mock.MagicMock()
    with pytest.raises(ValueError) as error:
        for row in job.result():
            pass
    assert not job.update.called


def test_job_result_format_generator():
    client = mock.MagicMock()

    def job_result_format_each(job_id, format):
        assert job_id == "12345"
        assert format == "msgpack.gz"
        yield ["foo", 123]
        yield ["bar", 456]
        yield ["baz", 789]

    client.job_result_format_each = job_result_format_each
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.success = mock.MagicMock(return_value=True)
    job.update = mock.MagicMock()
    rows = []
    for row in job.result_format("msgpack.gz"):
        rows.append(row)
    assert rows == [["foo", 123], ["bar", 456], ["baz", 789]]
    assert job.update.called


def test_job_result_format_list():
    client = mock.MagicMock()
    result = [["foo", 123], ["bar", 456], ["baz", 789]]
    job = models.Job(
        client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq", result=result
    )
    job.success = mock.MagicMock(return_value=True)
    job.update = mock.MagicMock()
    rows = []
    for row in job.result_format("msgpack.gz"):
        rows.append(row)
    assert rows == [["foo", 123], ["bar", 456], ["baz", 789]]
    assert job.update.called


def test_job_result_format_failure():
    client = mock.MagicMock()
    job = models.Job(client, "12345", "presto", "SELECT COUNT(1) FROM nasdaq")
    job.success = mock.MagicMock(return_value=False)
    job.update = mock.MagicMock()
    with pytest.raises(ValueError) as error:
        for row in job.result_format("msgpack.gz"):
            pass
    assert not job.update.called


def test_job_finished():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert not job(client, "queued").finished()
    assert not job(client, "booting").finished()
    assert not job(client, "running").finished()
    assert job(client, "success").finished()
    assert job(client, "error").finished()
    assert job(client, "killed").finished()


def test_job_success():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert not job(client, "queued").success()
    assert not job(client, "booting").success()
    assert not job(client, "running").success()
    assert job(client, "success").success()
    assert not job(client, "error").success()
    assert not job(client, "killed").success()


def test_job_error():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert not job(client, "queued").error()
    assert not job(client, "booting").error()
    assert not job(client, "running").error()
    assert not job(client, "success").error()
    assert job(client, "error").error()
    assert not job(client, "killed").error()


def test_job_killed():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert not job(client, "queued").killed()
    assert not job(client, "booting").killed()
    assert not job(client, "running").killed()
    assert not job(client, "success").killed()
    assert not job(client, "error").killed()
    assert job(client, "killed").killed()


def test_job_queued():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert job(client, "queued").queued()
    assert not job(client, "booting").queued()
    assert not job(client, "running").queued()
    assert not job(client, "success").queued()
    assert not job(client, "error").queued()
    assert not job(client, "killed").queued()


def test_job_running():
    def job(client, status):
        stub = models.Job(
            client, "1", "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        stub._update_progress = mock.MagicMock()
        return stub

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    assert not job(client, "queued").running()
    assert not job(client, "booting").running()
    assert job(client, "running").running()
    assert not job(client, "success").running()
    assert not job(client, "error").running()
    assert not job(client, "killed").running()


def test_job_update_progress():
    def run(client, job_id, status):
        job = models.Job(
            client, job_id, "hive", "SELECT COUNT(1) FROM nasdaq", status=status
        )
        client.job_status.reset_mock()
        job._update_progress()

    client = mock.MagicMock()
    client.job_status(return_value="testing")

    run(client, "1", "queued")
    client.job_status.assert_called_with("1")

    run(client, "2", "booting")
    client.job_status.assert_called_with("2")

    run(client, "3", "running")
    client.job_status.assert_called_with("3")

    run(client, "4", "success")
    assert not client.job_status.called

    run(client, "5", "error")
    assert not client.job_status.called

    run(client, "6", "killed")
    assert not client.job_status.called


def test_job_update_status():
    client = mock.MagicMock()
    client.api.show_job = mock.MagicMock(
        return_value={
            "job_id": "67890",
            "type": "hive",
            "url": "http://console.example.com/jobs/67890",
            "query": "SELECT COUNT(1) FROM nasdaq",
            "status": "success",
            "debug": None,
            "start_at": datetime.datetime(
                2015, 2, 10, 0, 2, 14, tzinfo=dateutil.tz.tzutc()
            ),
            "end_at": datetime.datetime(
                2015, 2, 10, 0, 2, 27, tzinfo=dateutil.tz.tzutc()
            ),
            "created_at": datetime.datetime(
                2015, 2, 10, 0, 2, 13, tzinfo=dateutil.tz.tzutc()
            ),
            "updated_at": datetime.datetime(
                2015, 2, 10, 0, 2, 15, tzinfo=dateutil.tz.tzutc()
            ),
            "cpu_time": None,
            "result_size": 22,
            "result": None,
            "result_url": None,
            "hive_result_schema": [["cnt", "bigint"]],
            "priority": 1,
            "retry_limit": 0,
            "org_name": None,
            "database": "sample_datasets",
            "num_records": 1,
            "user_name": "Treasure Data",
            "linked_result_export_job_id": None,
            "result_export_target_job_id": None,
        }
    )
    job = models.Job(client, "67890", "hive", "SELECT COUNT(1) FROM nasdaq")
    job.finished = mock.MagicMock(return_value=False)
    assert job.status() == "success"
    client.api.show_job.assert_called_with("67890")
