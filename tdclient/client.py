#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import json

from tdclient import api
from tdclient import model

class Client(object):
    def __init__(self, *args, **kwargs):
        self._api = api.API(*args, **kwargs)

    @property
    def api(self):
        return self._api

    @property
    def apikey(self):
        return self._api.apikey

    def server_status(self):
        return self.api.server_status()

    # => true
    def create_database(self, db_name, **kwargs):
        return self.api.create_database(db_name, **kwargs)

    # => true
    def delete_database(self, db_name):
        return self.api.delete_database(db_name)

    # => Account
    def account(self):
        account_id, plan, storage, guaranteed_cores, maximum_cores, created_at = self.api.show_account()
        return model.Account(self, account_id, plan, storage, guaranteed_cores, maximum_cores, created_at)

    def core_utilization(self, _from, to):
        _from, to, interval, history = self.api.account_core_utilization(_from, to)
        return (_from, to, interval, history)

    # => [Database]
    def databases(self):
        m = self.api.list_databases()
        return [ model.Database(self, db_name, None, *args) for (db_name, args) in m.items() ]

    # => Database
    def database(self, db_name):
        m = self.api.list_databases()
        if db_name in m:
            return model.Database(self, db_name, None, *m[db_name])
        else:
            raise api.NotFoundError("Database '%s' does not exist" % (db_name))

    # => true
    def create_log_table(self, db_name, table_name):
        return self.api.create_log_table(db_name, table_name)

    # => true
    def create_item_table(self, db_name, table_name, primary_key, primary_key_type):
        return self.api.create_item_table(db_name, table_name, primary_key, primary_key_type)

    # => true
    def swap_table(self, db_name, table_name1, table_name2):
        return self.api.swap_table(db_name, table_name1, table_name2)

    # => true
    def update_schema(self, db_name, table_name, schema):
        return self.api.update_schema(db_name, table_name, json.dumps(schema))

    # => true
    def update_expire(self, db_name, table_name, expire_days):
        return self.api.update_expire(db_name, table_name, expire_days)

    # => type:Symbol
    def delete_table(self, db_name, table_name):
        return self.api.delete_table(db_name, table_name)

    # => [Table]
    def tables(self, db_name):
        m = self.api.list_tables(db_name)
        return [ model.Table(self, db_name, table_name, *args) for (table_name, args) in m.items() ]

    # => Table
    def table(self, db_name, table_name):
        tables = self.tables(db_name)
        for table in tables:
            if table.table_name == table_name:
                return table
        raise api.NotFoundError("Table '%s.%s' does not exist" % (db_name, table_name))

    def tail(self, db_name, table_name, count, to=None, _from=None, block=None):
        return self.api.tail(db_name, table_name, count, to, _from, block)

    # => Job
    def query(self, db_name, q, result_url=None, priority=None, retry_limit=None, type="hive", **kwargs):
        # for compatibility, assume type is hive unless specifically specified
        if type not in ["hive", "pig", "impala", "presto"]:
            raise ValueError("The specified query type is not supported: %s" % (type))
        job_id = self.api.query(q, type, db_name, result_url, priority, retry_limit, **kwargs)
        return model.Job(self, job_id, type, q)

    # => [Job]
    def jobs(self, _from=None, to=None, status=None, conditions=None):
        results = self.api.list_jobs(_from, to, status, conditions)
        def job(lis):
            job_id, _type, status, query, start_at, end_at, cpu_time, result_size, result_url, priority, retry_limit, org, db = lis
            return model.Job(self, job_id, _type, query, status, None, None, start_at, end_at, cpu_time,
                             result_size, None, result_url, None, priority, retry_limit, org, db)
        return [ job(result) for result in results ]

    # => Job
    def job(self, job_id):
      job_id = str(job_id)
      _type, query, status, url, debug, start_at, end_at, cpu_time, result_size, result_url, hive_result_schema, priority, retry_limit, org, db = self.api.show_job(job_id)
      return model.Job(self, job_id, type, query, status, url, debug, start_at, end_at, cpu_time,
                       result_size, None, result_url, hive_result_schema, priority, retry_limit, org, db)

    # => status:String
    def job_status(self, job_id):
        return self.api.job_status(job_id)

    # => result:[{column:String=>value:Object]
    def job_result(self, job_id):
        return self.api.job_result(job_id)

    # => nil
    def job_result_each(self, job_id):
        for row in self.api.job_result_each(job_id):
            yield row

    def job_result_format(self, job_id, format):
        return self.api.job_result_format(job_id, format)

    def job_result_format_each(self, job_id, format):
        for row in self.api.job_result_format_each(job_id, format):
            yield row

    # => former_status:String
    def kill(self, job_id):
        return self.api.kill(job_id)

    # => Job
    def export_data(self, db_name, table_name, storage_type, opts={}):
        job_id = self.api.export_data(db_name, table_name, storage_type, opts)
        return model.Job(self, job_id, "export", nil)

    # => Job
    def partial_delete(self, db_name, table_name, to, _from, opts={}):
        job_id = self.api.partial_delete(db_name, table_name, to, _from, opts)
        return model.Job(self, job_id, "partialdelete", None)

    # => nil
    def create_bulk_import(self, name, database, table, opts={}):
        return self.api.create_bulk_import(name, database, table, opts)

    # => nil
    def delete_bulk_import(self, name):
        return self.api.delete_bulk_import(name)

    # => nil
    def freeze_bulk_import(self, name):
        return self.api.freeze_bulk_import(name)

    # => nil
    def unfreeze_bulk_import(self, name):
        return self.api.unfreeze_bulk_import(name)

    # => Job
    def perform_bulk_import(self, name):
        job_id = self.api.perform_bulk_import(name)
        return model.Job(self, job_id, "bulk_import", None)

    # => nil
    def commit_bulk_import(self, name):
        return self.api.commit_bulk_import(name)

    # => records:[row:Hash]
    def bulk_import_error_records(self, name, block=None):
        return self.api.bulk_import_error_records(name, block)

    # => BulkImport
    def bulk_import(self, name):
        data = self.api.show_bulk_import(name)
        return model.BulkImport(self, data)

    # => [BulkImport]
    def bulk_imports(self):
        return [ model.BulkImport(self, data) for data in self.api.list_bulk_imports() ]

    # => nil
    def bulk_import_upload_part(self, name, part_name, stream, size):
        return self.api.bulk_import_upload_part(name, part_name, stream, size)

    # => nil
    def bulk_import_delete_part(self, name, part_name):
        return self.api.bulk_import_delete_part(name, part_name)

    def list_bulk_import_parts(self, name):
        return self.api.list_bulk_import_parts(name)

    # => first_time:Time
    def create_schedule(self, name, opts):
        if "cron" not in opts:
            raise ValueError("'cron' option is required")
        if "query" not in opts:
            raise ValueError("'query' option is required")
        start = self.api.create_schedule(name, opts)
        return start # TODO: parse datetime string

    # => true
    def delete_schedule(self, name):
        return self.api.delete_schedule(name)

    # [Schedule]
    def schedules(self):
        result = self.api.list_schedules()
        def schedule(m):
            name,cron,query,database,result_url,timezone,delay,next_time,priority,retry_limit,org_name = m
            return model.Schedule(self, name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, org_name)
        return [ schedule(m) for m in result ]

    def update_schedule(self, name, params):
        self.api.update_schedule(name, params)

    # [ScheduledJob]
    def history(self, name, _from=None, to=None):
        result = self.api.history(name, _from, to)
        def scheduled_job(m):
            scheduled_at,job_id,_type,status,query,start_at,end_at,result_url,priority,database = m
            job_param = [job_id, _type, query, status,
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

    # [ScheduledJob]
    def run_schedule(self, name, time, num):
        results = self.api.run_schedule(name, time, num)
        def scheduled_job(m):
            job_id,_type,scheduled_at = m
            return model.ScheduledJob(self, scheduled_at, job_id, _type, None)
        return [ scheduled_job(m) for m in results ]

    # => time:Flaot
    def import_data(self, db_name, table_name, _format, stream, size, unique_id=None):
        return self.api.import_data(db_name, table_name, _format, stream, size, unique_id)

    # => [Result]
    def results(self):
        results = self.api.list_result()
        def result(m):
            name,url,organizations = m
            return model.Result(self, name, url, organizations)
        return [ result(m) for m in results ]

    # => true
    def create_result(self, name, url, opts={}):
        return self.api.create_result(name, url, opts)

    # => true
    def delete_result(self, name):
        return self.api.delete_result(name)

    # => [User]
    def users(self):
        results = self.api.list_users()
        def user(m):
            name,org,roles,email = m
            return model.User(self, name, org, roles, email)
        return [ user(m) for m in results ]

    # => true
    def add_user(self, name, org, email, password):
        return self.api.add_user(name, org, email, password)

    # => true
    def remove_user(self, user):
        return self.api.remove_user(user)

    # => true
    def change_email(self, user, email):
        return self.api.change_email(user, email)

    # => [apikey:String]
    def list_apikeys(self, user):
        return self.api.list_apikeys(user)

    # => true
    def add_apikey(self, user):
        return self.api.add_apikey(user)

    # => true
    def remove_apikey(self, user, apikey):
        return self.api.remove_apikey(user, apikey)

    # => true
    def change_password(self, user, password):
        return self.api.change_password(user, password)

    # => true
    def change_my_password(self, old_password, password):
        return self.api.change_my_password(old_password, password)

    # => [User]
    def access_controls(self):
        results = self.api.list_access_controls()
        def access_control(m):
            subject,action,scope,grant_option = m
            return model.AccessControl(self, subject, action, scope, grant_option)
        return [ access_control(m) for m in results ]

    # => true
    def grant_access_control(self, subject, action, scope, grant_option):
        return self.api.grant_access_control(subject, action, scope, grant_option)

    # => true
    def revoke_access_control(self, subject, action, scope):
        return self.api.revoke_access_control(subject, action, scope)

    # => true
    def test_access_control(self, user, action, scope):
        return self.api.test_access_control(user, action, scope)
