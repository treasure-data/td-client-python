#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import time

from tdclient._model import Model

class Schema(object):
    """Schema of a database table on Treasure Data Service
    """

    class Field(object):
        def __init__(self, name, type):
            self._name = name
            self._type = type

        @property
        def name(self):
            """
            TODO: add docstring
            """
            return self._name

        @property
        def type(self):
            """
            TODO: add docstring
            """
            return self._type

    def __init__(self, fields=[]):
        self._fields = fields

    @property
    def fields(self):
        """
        TODO: add docstring
        """
        return self._fields

    def add_field(self, name, type):
        """
        TODO: add docstring
        """
        self._fields.append(Field(name, type))

class Job(Model):
    """Job on Treasure Data Service
    """

    STATUS_QUEUED = "queued"
    STATUS_BOOTING = "booting"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_ERROR = "error"
    STATUS_KILLED = "killed"
    FINISHED_STATUS = [STATUS_SUCCESS, STATUS_ERROR, STATUS_KILLED]

    JOB_PRIORITY = {
        -2: "VERY LOW",
        -1: "LOW",
        0: "NORMAL",
        1: "HIGH",
        2: "VERY HIGH",
    }

    def __init__(self, client, job_id, type, query, status=None, url=None, debug=None, start_at=None, end_at=None, cpu_time=None,
                 result_size=None, result=None, result_url=None, hive_result_schema=None, priority=None, retry_limit=None,
                 org_name=None, db_name=None):
        super(Job, self).__init__(client)
        self._job_id = job_id
        self._type = type
        self._url = url
        self._query = query
        self._status = status
        self._debug = debug
        self._start_at = start_at
        self._end_at = end_at
        self._cpu_time = cpu_time
        self._result_size = result_size
        self._result = result
        self._result_url = result_url
        self._hive_result_schema = hive_result_schema
        self._priority = priority
        self._retry_limit = retry_limit
        self._org_name = org_name
        self._db_name = db_name

    @property
    def id(self):
        """
        Returns: a string represents the identifier of the job
        """
        return self._job_id

    @property
    def job_id(self):
        """
        Returns: a string represents the identifier of the job
        """
        return self._job_id

    @property
    def type(self):
        """
        Returns: a string represents the engine type of the job (e.g. "hive", "presto", etc.)
        """
        return self._type

    @property
    def result_url(self):
        """
        Returns: a string of URL of the result on Treasure Data Service
        """
        return self._result_url

    @property
    def priority(self):
        """
        Returns: a string represents the priority of the job (e.g. "NORMAL", "HIGH", etc.)
        """
        if self._priority in self.JOB_PRIORITY:
            return self.JOB_PRIORITY[self._priority]
        else:
            # just convert the value in string
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
        Returns: organization name
        """
        return self._org_name

    @property
    def db_name(self):
        """
        Returns: a string represents the name of a database that job is running on
        """
        return self._db_name

    def wait(self, timeout=None):
        """Sleep until the job has been finished

        Params:
            timeout (int): Timeout in seconds. No timeout by default.
        """
        started_at = time.time()
        while not self.finished():
            if timeout is None or abs(time.time() - started_at) < timeout:
                time.sleep(1) # TODO: configurable
            else:
                raise RuntimeError("timeout") # TODO: throw proper error

    def kill(self):
        """Kill the job

        Returns: a string represents the status of killed job ("queued", "running")
        """
        return self._client.kill(self.job_id)

    @property
    def query(self):
        """
        Returns: a string represents the query string of the job
        """
        return self._query

    def status(self):
        """
        Returns: a string represents the status of the job ("success", "error", "killed", "queued", "running")
        """
        if self._query is not None and not self.finished():
            self._update_status()
        return self._status

    @property
    def url(self):
        """
        Returns: a string of URL of the job on Treasure Data Service
        """
        return self._url

    def result(self):
        """
        Returns: an iterator of rows in result set
        """
        if not self.finished():
            raise ValueError("result is not ready")
        else:
            if self._result is None:
                for row in self._client.job_result_each(self._job_id):
                    yield row
            else:
                for row in self._result:
                    yield row

    def result_format(self, format):
        """
        Params:
            format (str): output format of result set

        Returns: an iterator of rows in result set
        """
        if not self.finished():
            raise ValueError("result is not ready")
        else:
            if self._result is None:
                for row in self._client.job_result_format_each(self._job_id, format):
                    yield row
            else:
                for row in self._result:
                    yield row

    def finished(self):
        """
        Returns: `True` if the job has been finished in success, error or killed
        """
        self._update_progress()
        return self._status in self.FINISHED_STATUS

    def success(self):
        """
        Returns: `True` if the job has been finished in success
        """
        self._update_progress()
        return self._status == self.STATUS_SUCCESS

    def error(self):
        """
        Returns: `True` if the job has been finished in error
        """
        self._update_progress()
        return self._status == self.STATUS_ERROR

    def killed(self):
        """
        Returns: `True` if the job has been finished in killed
        """
        self._update_progress()
        return self._status == self.STATUS_KILLED

    def queued(self):
        """
        Returns: `True` if the job is queued
        """
        self._update_progress()
        return self._status == self.STATUS_QUEUED

    def running(self):
        """
        Returns: `True` if the job is running
        """
        self._update_progress()
        return self._status == self.STATUS_RUNNING

    def _update_progress(self):
        if self._status not in self.FINISHED_STATUS:
            self.__update_progress()

    def __update_progress(self):
        self._status = self._client.job_status(self._job_id)

    def _update_status(self):
        job = self._client.api.show_job(self._job_id)
        self._query = job.get("query")
        self._status = job.get("status")
        self._url = job.get("url")
        self._debug = job.get("debug")
        self._start_at = job.get("start_at")
        self._end_at = job.get("end_at")
        self._cpu_time = job.get("cpu_time")
        self._result_size = job.get("result_size")
        self._result_url = job.get("result_url")
        self._hive_result_schema = job.get("hive_result_schema")
        self._priority = job.get("priority")
        self._retry_limit = job.get("retry_limit")
        self._db_name = job.get("db_name")
