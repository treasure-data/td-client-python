#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from tdclient import api
from tdclient import model

class Client(object):
    def __init__(self, apikey, **kwargs):
        self._api = api.API(apikey, **kwargs)

    @property
    def api(self):
        return self._api

    def apikey(self):
        return self._api.apikey

    def server_status(self):
        return self.api.server_status()

    # => true
    def create_database(self, db_name, **kwargs):
        return self.api.create_database(db_name, **kwargs)

    # => [Database]
    def databases(self):
        m = self.api.list_databases()
        print(repr(m))
        return [ model.Database(self, db_name, None, *args) for (db_name, args) in m.items() ]

    # => Database
    def database(self, db_name):
        m = self.api.list_databases()
        if db_name in m:
            return model.Database(self, db_name, None, *m[db_name])
        else:
            raise api.NotFoundError("Database '%s' does not exist" % (db_name))

    # => [Table]
    def tables(self, db_name):
        m = self.api.list_tables(db_name)
        return [ model.Table(self, db_name, table_name, *args) for (table_name, args) in m.items() ]

    # => Table
    def table(self, db_name, table_name):
        tables = self.tables(db_name)
        if table_name in tables:
            return tables[table_name]
        else:
            raise api.NotFoundError("Table '%s.%s' does not exist" % (db_name, table_name))

    # => Job
    def query(self, db_name, q, result_url=None, priority=None, retry_limit=None, **kwargs):
        # for compatibility, assume type is hive unless specifically specified
        _type = kwargs.get("type", "hive")
        if _type not in ["hive", "pig", "impala", "presto"]:
            raise ValueError("The specified query type is not supported: %s" % (_type))
        job_id = self.api.query(q, _type, db_name, result_url, priority, retry_limit, **kwargs)
        return model.Job(self, job_id, _type, q)

    # => [Job]
    def jobs(self, _from=None, to=None, status=None, conditions=None):
        results = self.api.list_jobs(_from, to, status, conditions)
        def job(lis):
            job_id, _type, status, query, start_at, end_at, cpu_time, result_size, result_url, priority, retry_limit, org, db = lis
            return model.Job(self, job_id, _type, query, status, None, None, start_at, end_at, cpu_time,
                       result_size, None, result_url, None, priority, retry_limit, org, db)
        return map(job, results)

    # => Job
    def job(self, job_id):
      job_id = str(job_id)
      _type, query, status, url, debug, start_at, end_at, cpu_time, result_size, result_url, hive_result_schema, priority, retry_limit, org, db = self.api.show_job(job_id)
      return Job(self, job_id, type, query, status, url, debug, start_at, end_at, cpu_time,
                 result_size, nil, result_url, hive_result_schema, priority, retry_limit, org, db)

    # => status:String
    def job_status(self, job_id):
        return self.api.job_status(job_id)

    # => result:[{column:String=>value:Object]
    def job_result(self, job_id):
        return self.api.job_result(job_id)

    # => former_status:String
    def kill(self, job_id):
        return self.api.kill(job_id)
