#!/usr/bin/env python

import time
from unittest import mock

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_scheduled_job():
    client = mock.MagicMock()
    scheduled_job = models.ScheduledJob(
        client, "scheduled_at", "12345", "presto", "SELECT 1", result_url="result_url"
    )
    assert scheduled_job.scheduled_at == "scheduled_at"
    assert scheduled_job.id == "12345"
    assert scheduled_job.type == "presto"
    assert scheduled_job.query == "SELECT 1"
    assert scheduled_job.result_url == "result_url"


def test_schedule():
    client = mock.MagicMock()
    schedule = models.Schedule(
        client,
        "name",
        "cron",
        "query",
        database="database",
        result_url="result_url",
        timezone="timezone",
        delay="delay",
        next_time="next_time",
        priority="priority",
        retry_limit="retry_limit",
        org_name="org_name",
        user_name="user_name",
    )
    assert schedule.name == "name"
    assert schedule.cron == "cron"
    assert schedule.query == "query"
    assert schedule.database == "database"
    assert schedule.result_url == "result_url"
    assert schedule.timezone == "timezone"
    assert schedule.delay == "delay"
    assert schedule.priority == "priority"
    assert schedule.retry_limit == "retry_limit"
    assert schedule.org_name == "org_name"
    assert schedule.user_name == "user_name"


def test_schedule_run():
    client = mock.MagicMock()
    schedule = models.Schedule(
        client,
        "name",
        "cron",
        "query",
        database="database",
        result_url="result_url",
        timezone="timezone",
        delay="delay",
        next_time="next_time",
        priority="priority",
        retry_limit="retry_limit",
        org_name="org_name",
    )
    t = int(time.time())
    schedule.run(t)
    client.run_schedule.assert_called_with("name", t, None)
