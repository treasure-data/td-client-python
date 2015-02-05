#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import time

class Model(object):
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """
        Returns: a :class:`tdclient.Client` instance
        """
        return self._client

class Account(Model):
    """Account on Treasure Data Service
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

    @property
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
    """Database on Treasure Data Service
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
        Returns: organization name
        """
        return self._org_name

    @property
    def permission(self):
        """
        Returns: a string represents permission for the database (e.g. "administrator", "full_access", etc.)
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
        Returns: a name of the database in string
        """
        return self._db_name

    def tables(self):
        """
        Returns: a list of :class:`tdclient.model.Table`
        """
        if self._tables is None:
            self._update_tables()
        return self._tables

    def create_log_table(self, name):
        """
        Params:
            name (str): name of new log table

        Returns: :class:`tdclient.model.Table`
        """
        return self._client.create_log_table(self._db_name, name)

    def create_item_table(self, name):
        """
        Params:
            name (str): name of new item table

        Returns: :class:`tdclient.model.Table`
        """
        return self._client.create_item_table(self._db_name, name)

    def table(self, table_name):
        """
        Params:
            table_name (str): name of a table

        Returns: :class:`tdclient.model.Table`
        """
        return self._client.table(self._db_name, table_name)

    def delete(self):
        """Delete the database

        Returns: `True` if success
        """
        return self._client.delete_database(self._db_name)

    def query(self, q, **kwargs):
        """Run a query on the database

        Params:
            q (str): a query string

        Returns: :class:`tdclient.model.Job`
        """
        return self._client.query(self._db_name, q, **kwargs)

    @property
    def created_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._created_at

    @property
    def updated_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._updated_at

    def _update_tables(self):
        self._tables = self._client.tables(self._db_name)
        for table in self._tables:
            table.database = self

class Table(Model):
    """Database table on Treasure Data Service
    """

    def __init__(self, client, db_name, table_name, type, schema, count, created_at=None, updated_at=None, estimated_storage_size=None,
                 last_import=None, last_log_timestamp=None, expire_days=None, primary_key=None, primary_key_type=None):
        super(Table, self).__init__(client)
        self.database = None
        self._db_name = db_name
        self._table_name = table_name
        self._type = type
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
        Returns: a string represents the type of the table
        """
        return self._type

    @property
    def db_name(self):
        """
        Returns: a string represents the name of the database
        """
        return self._db_name

    @property
    def table_name(self):
        """
        Returns: a string represents the name of the table
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
        Returns: a string represents the name of the database
        """
        return self._db_name

    @property
    def name(self):
        """
        Returns: a string represents the name of the table
        """
        return self._table_name

    @property
    def created_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._created_at

    @property
    def updated_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._updated_at

    @property
    def last_import(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._last_import

    @property
    def last_log_timestamp(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._last_log_timestamp

    @property
    def expire_days(self):
        """
        Returns: an int represents the days until expiration
        """
        return self._expire_days

    @property
    def permission(self):
        """
        TODO: add docstring
        """
        if self.database is None:
            self._update_database()
        return self.database.permission

    @property
    def identifier(self):
        """
        Returns: a string identifier of the table
        """
        return "%s.%s" % (self._db_name, self._table_name)

    def delete(self):
        """
        Returns: a string represents the type of deleted table
        """
        return self._client.delete_table(self._db_name, self._table_name)

    def tail(self, count, to=None, _from=None):
        """
        TODO: add docstring
        """
        return self._client.tail(self._db_name, self._table_name, count, to, _from)

    def import_data(self, format, bytes_or_stream, size, unique_id=None):
        """Import data into Treasure Data Service

        Params:
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns: second in float represents elapsed time to import data
        """
        return self._client.import_data(self._db_name, self._table_name, format, bytes_or_stream, size, unique_id=unique_id)

    def import_file(self, format, file, unique_id=None):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

        Params:
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        return self._client.import_file(self._db_name, self._table_name, format, file, unique_id=unique_id)

    def export_data(self, storage_type, **kwargs):
        """
        TODO: add docstring
        """
        return self._client.export_data(self._db_name, self._table_name, storage_type, kwargs)

    @property
    def estimated_storage_size_string(self):
        """
        Returns: a string represents estimated size of the table in human-readable format
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
            if timeout is None or timeout < time.time() - started_at:
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
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status in self.FINISHED_STATUS

    def success(self):
        """
        Returns: `True` if the job has been finished in success
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_SUCCESS

    def error(self):
        """
        Returns: `True` if the job has been finished in error
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_ERROR

    def killed(self):
        """
        Returns: `True` if the job has been finished in killed
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_KILLED

    def queued(self):
        """
        Returns: `True` if the job is queued
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_QUEUED

    def running(self):
        """
        Returns: `True` if the job is running
        """
        if self._status not in self.FINISHED_STATUS:
            self._update_progress()
        return self._status == self.STATUS_RUNNING

    def _update_progress(self):
        self._status = self._client.job_status(self._job_id)

    def _update_status(self):
        type, query, status, url, debug, start_at, end_at, cpu_time, result_size, result_url, hive_result_schema, priority, retry_limit, org_name, db_name = self._client.api.show_job(self._job_id)
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
    """Scheduled job on Treasure Data Service
    """
    def __init__(self, client, scheduled_at, *args, **kwargs):
        super(ScheduledJob, self).__init__(client, *args, **kwargs)
        self._scheduled_at = scheduled_at

    @property
    def scheduled_at(self):
        """
        Returns: a :class:`datetime.datetime` represents the schedule of next invocation of the job
        """
        return self._created_at

class Schedule(Model):
    """Schedule on Treasure Data Service
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
    """Result on Treasure Data Service
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
    """Bulk-import session on Treasure Data Service
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
        Returns: name of the bulk import session
        """
        return self._name

    @property
    def database(self):
        """
        Returns: database name in a string which the bulk import session is working on
        """
        return self._database

    @property
    def table(self):
        """
        Returns: table name in a string which the bulk import session is working on
        """
        return self._table

    @property
    def status(self):
        """
        Returns: status of the bulk import session in a string
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

    @property
    def upload_frozen(self):
        """
        TODO: add docstring
        """
        return self._upload_frozen

    def error_record_items(self):
        """
        TODO: add docstring
        """
        for record in self._client.bulk_import_error_records(self.name):
            yield record

class User(Model):
    """User on Treasure Data Service
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
        Returns: name of the user
        """
        return self._name

    @property
    def org_name(self):
        """
        Returns: organization name
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
        Returns: e-mail address
        """
        return self._email

class AccessControl(Model):
    """Access control settings of a user on Treasure Data Service
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
