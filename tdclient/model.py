#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

class Model(object):
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """
        TODO: add docstring
        """
        return self._client

class Account(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, account_id, plan, storage_size=None, guaranteed_cores=None, maximum_cores=None, created_at=None):
        super(Account, self).__init__(client)
        self._account_id = account_id
        self._plan = plan
        self._storage_size = storage_size
        self._guaranteed_cores = guaranteed_cores
        self._maximum_cores = maximum_cores
        self._created_at = created_at

    @property
    def account_id(self):
        """
        TODO: add docstring
        """
        return self._account_id

    @property
    def plan(self):
        """
        TODO: add docstring
        """
        return self._plan

    @property
    def storage_size(self):
        """
        TODO: add docstring
        """
        return self._storage_size

    @property
    def guaranteed_cores(self):
        """
        TODO: add docstring
        """
        return self._guaranteed_cores

    @property
    def maximum_cores(self):
        """
        TODO: add docstring
        """
        return self._maximum_cores

    @property
    def created_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

    def storage_size_string(self):
        """
        TODO: add docstring
        """
        if self._storage_size <= 1024 * 1024:
            return "0.0 GB"
        elif self._storage_size <= 60 * 1024 * 1024:
            return "0.01 GB"
        elif self._storage_size <= 60 * 1024 * 1024 * 1024:
            return "%.1f GB" % (float(self._storage_size) / (1024 * 1024 * 1024))
        else:
            return "%d GB" % int(float(self._storage_size) / (1024 * 1024 * 1024))

class Database(Model):
    """
    TODO: add docstring
    """

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
        """
        TODO: add docstring
        """
        return self._org_name

    @property
    def permission(self):
        """
        TODO: add docstring
        """
        return self._permission

    @property
    def count(self):
        """
        TODO: add docstring
        """
        return self._count

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._db_name

    def tables(self):
        """
        TODO: add docstring
        """
        if self._tables is None:
            self._update_tables()
        return self._tables

    def create_log_table(self, name):
        """
        TODO: add docstring
        """
        return self._client.create_log_table(self._db_name, name)

    def create_item_table(self, name):
        """
        TODO: add docstring
        """
        return self._client.create_item_table(self._db_name, name)

    def table(self, table_name):
        """
        TODO: add docstring
        """
        return self._client.table(self._db_name, table_name)

    def delete(self):
        """
        TODO: add docstring
        """
        return self._client.delete_database(self._db_name)

    def query(self, q):
        """
        TODO: add docstring
        """
        return self._client.query(self._db_name, q)

    @property
    def created_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

    @property
    def updated_at(self):
        """
        TODO: add docstring
        """
        return self._updated_at

    def _update_tables(self):
        self._tables = self._client.tables(self._db_name)
        for table in self._tables:
            table.database = self

class Table(Model):
    """
    TODO: add docstring
    """

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
        """
        TODO: add docstring
        """
        return self._type

    @property
    def db_name(self):
        """
        TODO: add docstring
        """
        return self._db_name

    @property
    def table_name(self):
        """
        TODO: add docstring
        """
        return self._table_name

    @property
    def schema(self):
        """
        TODO: add docstring
        """
        return self._schema

    @property
    def count(self):
        """
        TODO: add docstring
        """
        return self._count

    @property
    def estimated_storage_size(self):
        """
        TODO: add docstring
        """
        return self._estimated_storage_size

    @property
    def primary_key(self):
        """
        TODO: add docstring
        """
        return self._primary_key

    @property
    def primary_key_type(self):
        """
        TODO: add docstring
        """
        return self._primary_key_type

    @property
    def database_name(self):
        """
        TODO: add docstring
        """
        return self._db_name

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._table_name

    @property
    def created_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

    @property
    def updated_at(self):
        """
        TODO: add docstring
        """
        return self._updated_at

    @property
    def last_import(self):
        """
        TODO: add docstring
        """
        return self._last_import

    @property
    def last_log_timestamp(self):
        """
        TODO: add docstring
        """
        return self._last_log_timestamp

    @property
    def expire_days(self):
        """
        TODO: add docstring
        """
        return self._expire_days

    def permission(self):
        """
        TODO: add docstring
        """
        if self.database is None:
            self._update_database()
        return self.database.permission

    def identifier(self):
        """
        TODO: add docstring
        """
        return "%s.%s" % (self._db_name, self._table_name)

    def delete(self):
        """
        TODO: add docstring
        """
        return self._client.delete_table(self._db_name, self._table_name)

    def tail(self, count, to=None, _from=None):
        """
        TODO: add docstring
        """
        return self._client.tail(self._db_name, self._table_name, count, to, _from)

    def import_data(self, format, stream, size):
        """
        TODO: add docstring
        """
        return self._client.import_data(self._db_name, self._table_name, format, stream, size)

    def export_data(self, storage_type, **kwargs):
        """
        TODO: add docstring
        """
        return self._client.export_data(self._db_name, self._table_name, storage_type, kwargs)

    def estimated_storage_size_string(self):
        """
        TODO: add docstring
        """
        if self._estimated_storage_size <= 1024*1024:
            return "0.0 GB"
        elif self._estimated_storage_size <= 60*1024*1024:
            return "0.01 GB"
        elif self._estimated_storage_size <= 60*1024*1024*1024:
            return "%.1f GB" % (float(self._estimated_storage_size) / (1024*1024*1024))
        else:
            return "%d GB" % int(float(self._estimated_storage_size) / (1024*1024*1024))

    def _update_database(self):
        self.database = self._client.database(self._db_name)

class Schema(object):
    """
    TODO: add docstring
    """

    class Field(object):
        def __init__(self, name, _type):
            self._name = name
            self._type = _type

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

    def add_field(self, name, _type):
        """
        TODO: add docstring
        """
        self._fields.append(Field(name, _type))

class Job(Model):
    """
    TODO: add docstring
    """

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
        self._org_name = org_name
        self._db_name = db_name

    @property
    def job_id(self):
        """
        TODO: add docstring
        """
        return self._job_id

    @property
    def type(self):
        """
        TODO: add docstring
        """
        return self._type

    @property
    def result_url(self):
        """
        TODO: add docstring
        """
        return self._result_url

    @property
    def priority(self):
        """
        TODO: add docstring
        """
        return self._priority

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
    def db_name(self):
        """
        TODO: add docstring
        """
        return self._db_name

    def wait(self, timeout=None):
        """
        TODO: add docstring
        """
        raise NotImplementedError # TODO

    def kill(self):
        """
        TODO: add docstring
        """
        raise NotImplementedError # TODO

    def query(self):
        """
        TODO: add docstring
        """
        if self._query is not None and not self.finished():
            self._update_status()
        return self._query

    def status(self):
        """
        TODO: add docstring
        """
        if self._query is not None and not self.finished():
            self._update_status()
        return self._status

    def url(self):
        """
        TODO: add docstring
        """
        if self._query is not None and not self.finished():
            self._update_status()
        return self._url

    def result(self):
        """
        TODO: add docstring
        """
        if not self.finished():
            pass
        else:
            if self._result is None:
                for row in self._client.job_result_each(self._job_id):
                    yield row
            else:
                for row in self._result:
                    yield row

    def result_format(self, format):
        """
        TODO: add docstring
        """
        if not self.finished():
            pass
        else:
            if self._result is None:
                for row in self._client.job_result_format_each(self._job_id, format):
                    yield row
            else:
                for row in self._result:
                    yield row

    def finished(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status in self.FINISHED_STATUS

    def success(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_SUCCESS

    def error(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_ERROR

    def killed(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_KILLED

    def queued(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_QUEUED

    def running(self):
        """
        TODO: add docstring
        """
        if self._status not in self.FINISHED_STATUS:
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

class ScheduledJob(Job):
    def __init__(self, client, scheduled_at, *args, **kwargs):
        super(ScheduledJob, self).__init__(client, *args, **kwargs)
        self._scheduled_at = scheduled_at

    @property
    def scheduled_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

class Schedule(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, name, cron, query, database=None, result_url=None, timezone=None, delay=None, next_time=None, priority=None, retry_limit=None, org_name=None):
        super(Schedule, self).__init__(client)
        self._name = name
        self._cron = cron
        self._query = query
        self._database = database
        self._result_url = result_url
        self._timezone = timezone
        self._delay = delay
        self._next_time = next_time
        self._priority = priority
        self._retry_limit = retry_limit
        self._org_name = org_name

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._name

    @property
    def cron(self):
        """
        TODO: add docstring
        """
        return self._cron

    @property
    def query(self):
        """
        TODO: add docstring
        """
        return self._query

    @property
    def database(self):
        """
        TODO: add docstring
        """
        return self._database

    @property
    def result_url(self):
        """
        TODO: add docstring
        """
        return self._result_url

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
        """
        TODO: add docstring
        """
        return self._priority

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

    def run(self, time, num):
        """
        TODO: add docstring
        """
        return self._client.run_schedule(time, num)

class Result(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, name, url, org_name):
        super(Result, self).__init__(client)
        self._name = name
        self._url = url
        self._org_name = org_name

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._name

    @property
    def url(self):
        """
        TODO: add docstring
        """
        return self._url

    @property
    def org_name(self):
        """
        TODO: add docstring
        """
        return self._org_name

class BulkImport(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, name=None, database=None, table=None, status=None, upload_frozen=None, job_id=None, valid_records=None, error_records=None, valid_parts=None, error_parts=None, **kwargs):
        super(BulkImport, self).__init__(client)
        self._name = name
        self._database = database
        self._table = table
        self._status = status
        self._upload_frozen = upload_frozen
        self._job_id = job_id
        self._valid_records = valid_records
        self._error_records = error_records
        self._valid_parts = valid_parts
        self._error_parts = error_parts

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._name

    @property
    def database(self):
        """
        TODO: add docstring
        """
        return self._database

    @property
    def table(self):
        """
        TODO: add docstring
        """
        return self._table

    @property
    def status(self):
        """
        TODO: add docstring
        """
        return self._status

    @property
    def job_id(self):
        """
        TODO: add docstring
        """
        return self._job_id

    @property
    def valid_records(self):
        """
        TODO: add docstring
        """
        return self._valid_records

    @property
    def error_records(self):
        """
        TODO: add docstring
        """
        return self._error_records

    @property
    def valid_parts(self):
        """
        TODO: add docstring
        """
        return self._valid_parts

    @property
    def error_parts(self):
        """
        TODO: add docstring
        """
        return self._error_parts

    def upload_frozen(self):
        """
        TODO: add docstring
        """
        return self._upload_frozen

class User(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, name, org_name, role_names, email):
        super(User, self).__init__(client)
        self._name = name
        self._org_name = org_name
        self._role_names = role_names
        self._email = email

    @property
    def name(self):
        """
        TODO: add docstring
        """
        return self._name

    @property
    def org_name(self):
        """
        TODO: add docstring
        """
        return self._org_name

    @property
    def role_names(self):
        """
        TODO: add docstring
        """
        return self._role_names

    @property
    def email(self):
        """
        TODO: add docstring
        """
        return self._email

class AccessControl(Model):
    """
    TODO: add docstring
    """

    def __init__(self, client, subject, action, scope, grant_option):
        super(AccessControl, self).__init__(client)
        self._subject = subject
        self._action = action
        self._scope = scope
        self._grant_option = grant_option

    @property
    def subject(self):
        """
        TODO: add docstring
        """
        return self._subject

    @property
    def action(self):
        """
        TODO: add docstring
        """
        return self._action

    @property
    def scope(self):
        """
        TODO: add docstring
        """
        return self._scope

    @property
    def grant_option(self):
        """
        TODO: add docstring
        """
        return self._grant_option
