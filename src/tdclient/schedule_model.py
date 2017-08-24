#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from tdclient.model import Model
from tdclient.job_model import Job

class ScheduledJob(Job):
    """Scheduled job on Treasure Data Service
    """
    def __init__(self, client, scheduled_at, job_id, type, query, **kwargs):
        super(ScheduledJob, self).__init__(client, job_id, type, query, **kwargs)
        self._scheduled_at = scheduled_at

    @property
    def scheduled_at(self):
        """
        Returns: a :class:`datetime.datetime` represents the schedule of next invocation of the job
        """
        return self._scheduled_at

class Schedule(Model):
    """Schedule on Treasure Data Service
    """

    def __init__(self, client, *args, **kwargs):
        super(Schedule, self).__init__(client)
        if 0 < len(args):
            self._name = args[0]
            self._cron = args[1]
            self._query = args[2]
        else:
            self._name = kwargs.get("name")
            self._cron = kwargs.get("cron")
            self._query = kwargs.get("query")
        self._timezone = kwargs.get("timezone")
        self._delay = kwargs.get("delay")
        self._created_at = kwargs.get("created_at")
        self._type = kwargs.get("type")
        self._database = kwargs.get("database")
        self._user_name = kwargs.get("user_name")
        self._priority = kwargs.get("priority")
        self._retry_limit = kwargs.get("retry_limit")
        if "result_url" in kwargs:
            # backward compatibility for td-client-python < 0.6.0
            # TODO: remove this code if not necessary with fixing test
            self._result = kwargs.get("result_url")
        else:
            self._result = kwargs.get("result")
        self._next_time = kwargs.get("next_time")
        self._org_name = kwargs.get("org_name")

    @property
    def name(self):
        """The name of a scheduled job
        """
        return self._name

    @property
    def cron(self):
        """The configured schedule of a scheduled job.

        Returns: a string represents the schedule in cron form, or `None` if the job is not scheduled to run (saved query)
        """
        return self._cron

    @property
    def query(self):
        """The query string of a scheduled job
        """
        return self._query

    @property
    def database(self):
        """The target database of a scheduled job
        """
        return self._database

    @property
    def result_url(self):
        """The result output configuration in URL form of a scheduled job
        """
        return self._result

    @property
    def timezone(self):
        """
        TODO: add docstring
        """
        return self._timezone

    @property
    def delay(self):
        """
        TODO: add docstring
        """
        return self._delay

    @property
    def priority(self):
        """The priority of a scheduled job
        """
        if self._priority in Job.JOB_PRIORITY:
            return Job.JOB_PRIORITY[self._priority]
        else:
            return str(self._priority)

    @property
    def retry_limit(self):
        """
        TODO: add docstring
        """
        return self._retry_limit

    @property
    def org_name(self):
        """
        TODO: add docstring
        """
        return self._org_name

    @property
    def next_time(self):
        """
        TODO: add docstring
        """
        return self._next_time

    @property
    def created_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

    @property
    def type(self):
        """
        TODO: add docstring
        """
        return self._type

    @property
    def user_name(self):
        """
        TODO: add docstring
        """
        return self._user_name

    def run(self, time, num=None):
        """Run a scheduled job
        """
        return self._client.run_schedule(self.name, time, num)
