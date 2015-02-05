#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json

from tdclient import api
from tdclient import model

class Client(object):
    """API Client for Treasure Data Service
    """

    def __init__(self, *args, **kwargs):
        self._api = api.API(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def api(self):
        """
        Returns: an instance of :class:`tdclient.api.API`
        """
        return self._api

    @property
    def apikey(self):
        """
        Returns: API key string.
        """
        return self._api.apikey

    def server_status(self):
        """
        Returns: a string represents current server status.
        """
        return self.api.server_status()

    def create_database(self, db_name, **kwargs):
        """
        Params:
            db_name (str): name of a database to create

        Returns: `True` if success
        """
        return self.api.create_database(db_name, **kwargs)

    def delete_database(self, db_name):
        """
        Params:
            db_name (str): name of database to delete

        Returns: `True` if success
        """
        return self.api.delete_database(db_name)

    def account(self):
        """
        Returns: :class:`tdclient.model.Acount`
        """
        account_id, plan, storage, guaranteed_cores, maximum_cores, created_at = self.api.show_account()
        return model.Account(self, account_id, plan, storage, guaranteed_cores, maximum_cores, created_at)

    def core_utilization(self, _from, to):
        """
        TODO: add docstring
        """
        _from, to, interval, history = self.api.account_core_utilization(_from, to)
        return (_from, to, interval, history)

    def databases(self):
        """
        Returns: a list of :class:`tdclient.model.Database`
        """
        m = self.api.list_databases()
        return [ model.Database(self, db_name, None, *args) for (db_name, args) in m.items() ]

    def database(self, db_name):
        """
        Params:
            db_name (str): name of a database

        Returns: :class:`tdclient.model.Database`
        """
        m = self.api.list_databases()
        if db_name in m:
            return model.Database(self, db_name, None, *m[db_name])
        else:
            raise api.NotFoundError("Database '%s' does not exist" % (db_name))

    def create_log_table(self, db_name, table_name):
        """
        Params:
            db_name (str): name of a database
            table_name (str): name of a table to create

        Returns: `True` if success
        """
        return self.api.create_log_table(db_name, table_name)

    def create_item_table(self, db_name, table_name, primary_key, primary_key_type):
        """
        Params:
            db_name (str): name of a database
            table_name (str): name of a table to create
            primary_key (str): name of primary key column
            primary_key_type (str): type of primary key column

        Returns: `True` if success
        """
        return self.api.create_item_table(db_name, table_name, primary_key, primary_key_type)

    def swap_table(self, db_name, table_name1, table_name2):
        """
        Params:
            db_name (str): name of a database
            table_name1 (str): original table name
            table_name2 (str): table name you want to rename to

        Returns: `True` if success
        """
        return self.api.swap_table(db_name, table_name1, table_name2)

    def update_schema(self, db_name, table_name, schema):
        """Updates the schema of a table

        Params:
            db_name (str): name of a database
            table_name (str): name of a table
            schema (dict): a dictionary object represents the schema definition (will convert to JSON)

        Returns: `True` if success
        """
        return self.api.update_schema(db_name, table_name, json.dumps(schema))

    def update_expire(self, db_name, table_name, expire_days):
        """Set expiration date to a table

        Params:
            db_name (str): name of a database
            table_name (str): name of a table
            epire_days (int): expiration date in days from today

        Returns: `True` if success
        """
        return self.api.update_expire(db_name, table_name, expire_days)

    def delete_table(self, db_name, table_name):
        """Delete a table

        Params:
            db_name (str): name of a database
            table_name (str): name of a table

        Returns: a string represents the type of deleted table
        """
        return self.api.delete_table(db_name, table_name)

    def tables(self, db_name):
        """List existing tables

        Params:
            db_name (str): name of a database

        Returns: a list of :class:`tdclient.model.Table`
        """
        m = self.api.list_tables(db_name)
        return [ model.Table(self, db_name, table_name, *args) for (table_name, args) in m.items() ]

    def table(self, db_name, table_name):
        """
        Params:
            db_name (str): name of a database
            table_name (str): name of a table

        Returns: :class:`tdclient.model.Table`

        Raises:
            tdclient.api.NotFoundError: if the table doesn't exist
        """
        tables = self.tables(db_name)
        for table in tables:
            if table.table_name == table_name:
                return table
        raise api.NotFoundError("Table '%s.%s' does not exist" % (db_name, table_name))

    def tail(self, db_name, table_name, count, to=None, _from=None, block=None):
        """
        TODO: add docstring
        """
        return self.api.tail(db_name, table_name, count, to, _from, block)

    def query(self, db_name, q, result_url=None, priority=None, retry_limit=None, type="hive", **kwargs):
        """Run a query on specified database table.

        Params:
            db_name (str): name of a database
            q (str): a query string
            result_url (str): result output URL
            priority (str): priority
            retry_limit (int): retry limit
            type (str): name of a query engine

        Returns: :class:`tdclient.model.Job`

        Raises:
            ValueError: if unknown query type has been specified
        """
        # for compatibility, assume type is hive unless specifically specified
        if type not in ["hive", "pig", "impala", "presto"]:
            raise ValueError("The specified query type is not supported: %s" % (type))
        job_id = self.api.query(q, type, db_name, result_url, priority, retry_limit, **kwargs)
        return model.Job(self, job_id, type, q)

    def jobs(self, _from=None, to=None, status=None, conditions=None):
        """List jobs

        Params:
            _from (int):
            to (int):
            status (str):
            conditions (str)

        Returns: a list of :class:`tdclient.model.Job`
        """
        results = self.api.list_jobs(_from, to, status, conditions)
        def job(d):
            return model.Job(
                self,
                d["job_id"],
                d["type"],
                d["query"],
                status=d.get("status"),
                url=d.get("url"),
                debug=d.get("debug"),
                start_at=d.get("start_at"),
                end_at=d.get("end_at"),
                cpu_time=d.get("cpu_time"),
                result_size=d.get("result_size"),
                result=d.get("result"),
                result_url=d.get("result_url"),
                hive_result_schema=d.get("hive_result_schema"),
                priority=d.get("priority"),
                retry_limit=d.get("retry_limit"),
                org_name=d.get("org_name"),
                db_name=d.get("db_name"),
            )
        return [ job(d) for d in results ]

    def job(self, job_id):
        """Get a job from `job_id`

        Params:
            job_id (str): job id

        Returns: :class:`tdclient.model.Job`
        """
        d = self.api.show_job(str(job_id))
        return model.Job(
            self,
            job_id,
            d["type"],
            d["query"],
            status=d.get("status"),
            url=d.get("url"),
            debug=d.get("debug"),
            start_at=d.get("start_at"),
            end_at=d.get("end_at"),
            cpu_time=d.get("cpu_time"),
            result_size=d.get("result_size"),
            result=d.get("result"),
            result_url=d.get("result_url"),
            hive_result_schema=d.get("hive_result_schema"),
            priority=d.get("priority"),
            retry_limit=d.get("retry_limit"),
            org_name=d.get("org_name"),
            db_name=d.get("db_name"),
        )

    def job_status(self, job_id):
        """
        Params:
            job_id (str): job id

        Returns: a string represents the status of the job ("success", "error", "killed", "queued", "running")
        """
        return self.api.job_status(job_id)

    def job_result(self, job_id):
        """
        Params:
            job_id (str): job id

        Returns: a list of each rows in result set
        """
        return self.api.job_result(job_id)

    def job_result_each(self, job_id):
        """
        Params:
            job_id (str): job id

        Returns: an iterator of result set
        """
        for row in self.api.job_result_each(job_id):
            yield row

    def job_result_format(self, job_id, format):
        """
        Params:
            job_id (str): job id
            format (str): output format of result set

        Returns: a list of each rows in result set
        """
        return self.api.job_result_format(job_id, format)

    def job_result_format_each(self, job_id, format):
        """
        Params:
            job_id (str): job id
            format (str): output format of result set

        Returns: an iterator of rows in result set
        """
        for row in self.api.job_result_format_each(job_id, format):
            yield row

    def kill(self, job_id):
        """
        Params:
            job_id (str): job id

        Returns: a string represents the status of killed job ("queued", "running")
        """
        return self.api.kill(job_id)

    def export_data(self, db_name, table_name, storage_type, params={}):
        """Export data from Treasure Data Service

        Params:
            db_name (str): name of a database
            table_name (str): name of a table
            storage_type (str): type of the storage
            params (dict): optional parameters

        Returns: :class:`tdclient.model.Job`
        """
        job_id = self.api.export_data(db_name, table_name, storage_type, params)
        return model.Job(self, job_id, "export", None)

    def partial_delete(self, db_name, table_name, to, _from, params={}):
        """
        TODO: add docstring
        => :class:`tdclient.model.Job`
        """
        job_id = self.api.partial_delete(db_name, table_name, to, _from, params)
        return model.Job(self, job_id, "partialdelete", None)

    def create_bulk_import(self, name, database, table, params={}):
        """Create new bulk import session

        Params:
            name (str): name of new bulk import session
            database (str): name of a database
            table (str): name of a table
        """
        return self.api.create_bulk_import(name, database, table, params)

    def delete_bulk_import(self, name):
        """Delete a bulk import session

        Params:
            name (str): name of a bulk import session
        """
        return self.api.delete_bulk_import(name)

    def freeze_bulk_import(self, name):
        """Freeze a bulk import session

        Params:
            name (str): name of a bulk import session
        """
        return self.api.freeze_bulk_import(name)

    def unfreeze_bulk_import(self, name):
        """Unfreeze a bulk import session

        Params:
            name (str): name of a bulk import session
        """
        return self.api.unfreeze_bulk_import(name)

    def perform_bulk_import(self, name):
        """Perform a bulk import session

        Params:
            name (str): name of a bulk import session

        Returns: :class:`tdclient.model.Job`
        """
        job_id = self.api.perform_bulk_import(name)
        return model.Job(self, job_id, "bulk_import", None)

    def commit_bulk_import(self, name):
        """Commit a bulk import session

        Params:
            name (str): name of a bulk import session
        """
        return self.api.commit_bulk_import(name)

    def bulk_import_error_records(self, name):
        """
        Params:
            name (str): name of a bulk import session

        Returns: an iterator of error records
        """
        for record in self.api.bulk_import_error_records(name):
            yield record

    def bulk_import(self, name):
        """Get a bulk import session

        Params:
            name (str): name of a bulk import session

        Returns: :class:`tdclient.model.BulkImport`
        """
        data = self.api.show_bulk_import(name)
        return model.BulkImport(self, **data)

    def bulk_imports(self):
        """List bulk import sessions

        Returns: a list of :class:`tdclient.model.BulkImport`
        """
        return [ model.BulkImport(self, **data) for data in self.api.list_bulk_imports() ]

    def bulk_import_upload_part(self, name, part_name, stream, size):
        """Upload a part to a bulk import session

        Params:
            name (str): name of a bulk import session
            part_name (str): name of a part of the bulk import session
            stream (file-like): a file-like object contains the part
            size (int): the size of the part
        """
        return self.api.bulk_import_upload_part(name, part_name, stream, size)

    def bulk_import_delete_part(self, name, part_name):
        """Delete a part from a bulk import session

        Params:
            name (str): name of a bulk import session
            part_name (str): name of a part of the bulk import session
        """
        return self.api.bulk_import_delete_part(name, part_name)

    def list_bulk_import_parts(self, name):
        """List parts of a bulk import session

        Params:
            name (str): name of a bulk import session

        Returns: a list of string represents the name of parts
        """
        return self.api.list_bulk_import_parts(name)

    def create_schedule(self, name, params={}):
        """
        TODO: add docstring
        => first_time:datetime.datetime
        """
        if "cron" not in params:
            raise ValueError("'cron' option is required")
        if "query" not in params:
            raise ValueError("'query' option is required")
        return self.api.create_schedule(name, params)

    def delete_schedule(self, name):
        """
        TODO: add docstring
        => True
        """
        return self.api.delete_schedule(name)

    def schedules(self):
        """
        TODO: add docstring
        [:class:`tdclient.model.Schedule`]
        """
        result = self.api.list_schedules()
        def schedule(m):
            name,cron,query,database,result_url,timezone,delay,next_time,priority,retry_limit,org_name = m
            return model.Schedule(self, name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, org_name)
        return [ schedule(m) for m in result ]

    def update_schedule(self, name, params={}):
        """
        TODO: add docstring
        [:class:`tdclient.model.ScheduledJob`]
        """
        self.api.update_schedule(name, params)

    def history(self, name, _from=None, to=None):
        """
        TODO: add docstring
        [:class:`tdclient.model.ScheduledJob`]
        """
        result = self.api.history(name, _from, to)
        def scheduled_job(m):
            scheduled_at,job_id,type,status,query,start_at,end_at,result_url,priority,database = m
            job_param = [job_id, type, query, status,
                None, None, # url, debug
                start_at, end_at,
                None, # cpu_time
                None, None, # result_size, result
                result_url,
                None, # hive_result_schema
                priority,
                None, # retry_limit
                None, # TODO org_name
                database]
            return model.ScheduledJob(self, scheduled_at, *job_param)
        return [ scheduled_job(m) for m in result ]

    def run_schedule(self, name, time, num):
        """
        TODO: add docstring
        [:class:`tdclient.model.ScheduledJob`]
        """
        results = self.api.run_schedule(name, time, num)
        def scheduled_job(m):
            job_id,type,scheduled_at = m
            return model.ScheduledJob(self, scheduled_at, job_id, type, None)
        return [ scheduled_job(m) for m in results ]

    def import_data(self, db_name, table_name, format, bytes_or_stream, size, unique_id=None):
        """Import data into Treasure Data Service

        Params:
            db_name (str): name of a database
            table_name (str): name of a table
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns: second in float represents elapsed time to import data
        """
        return self.api.import_data(db_name, table_name, format, bytes_or_stream, size, unique_id=unique_id)

    def import_file(self, db_name, table_name, format, file, unique_id=None):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

        Params:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack", "json")
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        return self.api.import_file(db_name, table_name, format, file, unique_id=unique_id)

    def results(self):
        """
        Returns: a list of :class:`tdclient.model.Result`
        """
        results = self.api.list_result()
        def result(m):
            name,url,organizations = m
            return model.Result(self, name, url, organizations)
        return [ result(m) for m in results ]

    def create_result(self, name, url, params={}):
        """
        TODO: add docstring
        => True
        """
        return self.api.create_result(name, url, params)

    def delete_result(self, name):
        """
        TODO: add docstring
        => True
        """
        return self.api.delete_result(name)

    def users(self):
        """List users

        Returns: a liast of :class:`tdclient.model.User`
        """
        results = self.api.list_users()
        def user(m):
            name,org,roles,email = m
            return model.User(self, name, org, roles, email)
        return [ user(m) for m in results ]

    def add_user(self, name, org, email, password):
        """Add a new user

        Params:
            name (str): name of the user
            org (str): organization
            email: (str): e-mail address
            password (str): password

        Returns: `True` if success
        """
        return self.api.add_user(name, org, email, password)

    def remove_user(self, name):
        """Remove a user

        Params:
            name (str): name of the user

        Returns: `True` if success
        """
        return self.api.remove_user(name)

    def change_email(self, name, email):
        """
        Params:
            name (str): name of the user
            email (str) new e-mail address

        Returns: `True` if success
        """
        return self.api.change_email(name, email)

    def list_apikeys(self, name):
        """
        Params:
            name (str): name of the user

        Returns: a list of string of API key
        """
        return self.api.list_apikeys(name)

    def add_apikey(self, name):
        """
        Params:
            name (str): name of the user

        Returns: `True` if success
        """
        return self.api.add_apikey(name)

    def remove_apikey(self, name, apikey):
        """
        Params:
            name (str): name of the user
            apikey (str): an API key to remove

        Returns: `True` if success
        """
        return self.api.remove_apikey(name, apikey)

    def change_password(self, name, password):
        """
        Params:
            name (str): name of the user
            password (str): new password

        Returns: `True` if success
        """
        return self.api.change_password(name, password)

    def change_my_password(self, old_password, password):
        """
        Params:
            old_password (str): old password
            password (str): new password

        Returns: `True` if success
        """
        return self.api.change_my_password(old_password, password)

    def access_controls(self):
        """
        Returns: a list of :class:`tdclient.model.AccessControl`
        """
        results = self.api.list_access_controls()
        def access_control(m):
            subject,action,scope,grant_option = m
            return model.AccessControl(self, subject, action, scope, grant_option)
        return [ access_control(m) for m in results ]

    def grant_access_control(self, subject, action, scope, grant_option):
        """
        TODO: add docstring
        => True
        """
        return self.api.grant_access_control(subject, action, scope, grant_option)

    def revoke_access_control(self, subject, action, scope):
        """
        TODO: add docstring
        => True
        """
        return self.api.revoke_access_control(subject, action, scope)

    def test_access_control(self, user, action, scope):
        """
        TODO: add docstring
        => True
        """
        return self.api.test_access_control(user, action, scope)

    def close(self):
        """Close opened API connections.
        """
        return self._api.close()
