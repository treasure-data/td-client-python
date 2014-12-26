#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

class Model(object):
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        return self._client

class Database(Model):
    PERMISSIONS = ["administrator", "full_access", "import_only", "query_only"]
    PERMISSION_LIST_TABLES = ["administrator", "full_access"]

    def __init__(self, client, db_name, tables=None, count=None, created_at=None, updated_at=None, org_name=None, permission=None):
        super(Database, self).__init__(client)
        self._db_name = db_name
        self._tables = tables
        self._count = count
        self._created_at = created_at
        self._updated_at = updated_at
        self._org_name = org_name
        self._permission = permission

    @property
    def org_name(self):
        return self._org_name

    @property
    def permission(self):
        return self._permission

    @property
    def count(self):
        return self._count

    @property
    def name(self):
        return self._db_name

    def tables(self):
        if self._tables is None:
            self._update_tables
        return self._tables

    def table(self, table_name):
        return self._client.table(self._db_name, table_name)

    def query(self, q):
        return self._client.query(self._db_name, q)

    def created_at(self):
        return self._created_at # TODO: parse datetime string

    def updated_at(self):
        return self._updated_at # TODO: parse datetime string

    def _update_tables(self):
        self._tables = self._client.tables(self._db_name)
        for table in self._tables:
            table.database = self

class Table(Model):
    def __init__(self, client, db_name, table_name, _type, schema, count, created_at=None, updated_at=None, estimated_storage_size=None,
                 last_import=None, last_log_timestamp=None, expire_days=None, primary_key=None, primary_key_type=None):
        super(Table, self).__init__(client)
        self.database = None
        self._db_name = db_name
        self._table_name = table_name
        self._type = _type
        self._schema = schema
        self._count = count
        self._created_at = created_at
        self._updated_at = updated_at
        self._estimated_storage_size = estimated_storage_size
        self._last_import = last_import
        self._last_log_timestamp = last_log_timestamp
        self._expire_days = expire_days
        self._primary_key = primary_key
        self._primary_key_type = primary_key_type

    @property
    def type(self):
        return self._type

    @property
    def db_name(self):
        return self._db_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def schema(self):
        return self._schema

    @property
    def count(self):
        return self._count

    @property
    def estimated_storage_size(self):
        return self._estimated_storage_size

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def primary_key_type(self):
        return self._primary_key_type

    @property
    def database_name(self):
        return self._db_name

    @property
    def name(self):
        return self._table_name

    def created_at(self):
        return self._created_at # TODO: parse datetime string

    def updated_at(self):
        return self._updated_at # TODO: parse datetime string

    def last_import(self):
        return self._last_import # TODO: parse datetime string

    def last_log_timestamp(self):
        return self._last_log_timestamp # TODO: parse datetime string

    def expire_days(self):
        return self._expire_days # TODO: parse datetime string

    def permission(self):
        if self.database is None:
            self._update_database()
        return self.database.permission

    def identifier(self):
        return "%s.%s" % (self._db_name, self._table_name)

    def _update_database(self):
        self.database = self._client.database(self._db_name)

class Job(Model):
    STATUS_QUEUED = "queued"
    STATUS_BOOTING = "booting"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_ERROR = "error"
    STATUS_KILLED = "killed"
    FINISHED_STATUS = [STATUS_SUCCESS, STATUS_ERROR, STATUS_KILLED]
    
    def __init__(self, client, job_id, _type, query, status=None, url=None, debug=None, start_at=None, end_at=None, cpu_time=None,
                 result_size=None, result=None, result_url=None, hive_result_schema=None, priority=None, retry_limit=None,
                 org_name=None, db_name=None):
        super(Job, self).__init__(client)
        self._job_id = job_id
        self._type = _type
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
        self._db_name = db_name

    @property
    def job_id(self):
        return self._job_id

    @property
    def type(self):
        return self._type

    @property
    def result_url(self):
        return self._result_url

    @property
    def priority(self):
        return self._priority

    @property
    def retry_limit(self):
        return self._retry_limit

    @property
    def org_name(self):
        return self._org_name

    @property
    def db_name(self):
        return self._db_name

    def query(self):
        if self._query is None or self.finished():
            self._update_status()
        return self._query

    def status(self):
        if self._query is None or self.finished():
            self._update_status()
        return self._status

    def url(self):
        if self._query is None or self.finished():
            self._update_status()
        return self._url

    def result(self):
        if self._result is None:
            if not self.finished():
                return None
            else:
                self._result = self._client.job_result(self._job_id)
        return self._result

    def finished(self):
        if self._status is None:
            self._update_progress()
        return self._status in self.FINISHED_STATUS

    def success(self):
        if self._status is None:
            self._update_progress()
        return self._status == self.STATUS_SUCCESS

    def error(self):
        if self._status is None:
            self._update_progress()
        return self._status == self.STATUS_ERROR

    def killed(self):
        if self._status is None:
            self._update_progress()
        return self._status == self.STATUS_KILLED

    def queued(self):
        if self._status is None:
            self._update_progress()
        return self._status == self.STATUS_QUEUED

    def running(self):
        if self._status is None:
            self._update_progress()
        return self._status == self.STATUS_RUNNING

    def _update_progress(self):
        self._status = self._client.job_status(self._job_id)

    def _update_status(self):
        _type, query, status, url, debug, start_at, end_at, cpu_time, result_size, result_url, hive_result_schema, priority, retry_limit, org_name, db_name = self._client.api.show_job(self._job_id)
        self._query = query
        self._status = status
        self._url = url
        self._debug = debug
        self._start_at = start_at
        self._end_at = end_at
        self._cpu_time = cpu_time
        self._result_size = result_size
        self._result_url = result_url
        self._hive_result_schema = hive_result_schema
        self._priority = priority
        self._retry_limit = retry_limit
        self._db_name = db_name
