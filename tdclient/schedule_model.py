#!/usr/bin/env python

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from tdclient.job_model import Job
from tdclient.model import Model

if TYPE_CHECKING:
    from tdclient.client import Client


class ScheduledJob(Job):
    """Scheduled job on Treasure Data Service"""

    def __init__(
        self,
        client: Client,
        scheduled_at: datetime.datetime,
        job_id: str,
        type: str,
        query: str,
        **kwargs: Any,
    ) -> None:
        super(ScheduledJob, self).__init__(client, job_id, type, query, **kwargs)
        self._scheduled_at = scheduled_at

    @property
    def scheduled_at(self) -> datetime.datetime:
        """a :class:`datetime.datetime` represents the schedule of next invocation of the job"""
        return self._scheduled_at


class Schedule(Model):
    """Schedule on Treasure Data Service"""

    def __init__(self, client: Client, *args: Any, **kwargs: Any) -> None:
        super(Schedule, self).__init__(client)
        if 0 < len(args):
            self._name: str | None = args[0]
            self._cron: str | None = args[1]
            self._query: str | None = args[2]
        else:
            self._name = kwargs.get("name")
            self._cron = kwargs.get("cron")
            self._query = kwargs.get("query")
        self._timezone: str | None = kwargs.get("timezone")
        self._delay: int | None = kwargs.get("delay")
        self._created_at: datetime.datetime | None = kwargs.get("created_at")
        self._type: str | None = kwargs.get("type")
        self._database: str | None = kwargs.get("database")
        self._user_name: str | None = kwargs.get("user_name")
        self._priority: int | str | None = kwargs.get("priority")
        self._retry_limit: int | None = kwargs.get("retry_limit")
        if "result_url" in kwargs:
            # backward compatibility for td-client-python < 0.6.0
            # TODO: remove this code if not necessary with fixing test
            self._result: str | None = kwargs.get("result_url")
        else:
            self._result = kwargs.get("result")
        self._next_time: datetime.datetime | None = kwargs.get("next_time")
        self._org_name: str | None = kwargs.get("org_name")

    @property
    def name(self) -> str | None:
        """The name of a scheduled job"""
        return self._name

    @property
    def cron(self) -> str | None:
        """The configured schedule of a scheduled job.

        Returns a string represents the schedule in cron form, or `None` if the
        job is not scheduled to run (saved query)
        """
        return self._cron

    @property
    def query(self) -> str | None:
        """The query string of a scheduled job"""
        return self._query

    @property
    def database(self) -> str | None:
        """The target database of a scheduled job"""
        return self._database

    @property
    def result_url(self) -> str | None:
        """The result output configuration in URL form of a scheduled job"""
        return self._result

    @property
    def timezone(self) -> str | None:
        """The time zone of a scheduled job"""
        return self._timezone

    @property
    def delay(self) -> int | None:
        """A delay ensures all buffered events are imported before running the query."""
        return self._delay

    @property
    def priority(self) -> str:
        """The priority of a scheduled job"""
        if self._priority in Job.JOB_PRIORITY:
            return Job.JOB_PRIORITY[self._priority]
        else:
            return str(self._priority)

    @property
    def retry_limit(self) -> int | None:
        """Automatic retry count."""
        return self._retry_limit

    @property
    def org_name(self) -> str | None:
        """
        TODO: add docstring
        """
        return self._org_name

    @property
    def next_time(self) -> datetime.datetime | None:
        """
        :obj:`datetime.datetime`: Schedule for next run
        """
        return self._next_time

    @property
    def created_at(self) -> datetime.datetime | None:
        """
        :obj:`datetime.datetime`: Create date
        """
        return self._created_at

    @property
    def type(self) -> str | None:
        """Query type. {"presto", "hive"}."""
        return self._type

    @property
    def user_name(self) -> str | None:
        """User name of a scheduled job"""
        return self._user_name

    def run(self, time: int, num: int | None = None) -> list[ScheduledJob]:
        """Run a scheduled job

        Args:
            time (int): Time in Unix epoch format that would be set as TD_SCHEDULED_TIME
            num (int): Indicates how many times the query will be executed.
                Value should be 9 or less.
        Returns:
            [:class:`tdclient.models.ScheduledJob`]
        """
        return self._client.run_schedule(self.name, time, num)
