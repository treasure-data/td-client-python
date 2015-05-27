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

def test_scheduled_job():
    client = mock.MagicMock()
    schedule = models.Schedule(client, "name", "cron", "query", database="database", result_url="result_url", timezone="timezone", delay="delay", next_time="next_time", priority="priority", retry_limit="retry_limit", org_name="org_name")
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

def test_schedule():
    pass
