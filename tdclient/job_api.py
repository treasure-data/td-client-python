#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import json
import msgpack
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class JobAPI(object):
    ####
    ## Job API
    ##

    # => [(jobId:String, type:Symbol, status:String, start_at:String, end_at:String, result_url:String)]
    def list_jobs(self, _from=0, to=None, status=None, conditions=None):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        if status is not None:
            params["status"] = str(status)
        if conditions is not None:
            params.update(conditions)
        with self.get("/v3/job/list", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List jobs failed", res, body)
            js = self.checked_json(body, ["jobs"])
            result = []
            for m in js["jobs"]:
                job_id = m.get("job_id")
                _type = m.get("type", "?")
                database = m.get("database")
                status = m.get("status")
                query = m.get("query")
                start_at = m.get("start_at")
                end_at = m.get("end_at")
                cpu_time = m.get("cpu_time")
                result_size = m.get("result_size") # compressed result size in msgpack.gz format
                result_url = m.get("result")
                priority = m.get("priority")
                retry_limit = m.get("retry_limit")
                result.append([job_id, _type, status, query, start_at, end_at, cpu_time,
                     result_size, result_url, priority, retry_limit, None, database])
            return result

    # => (type:Symbol, status:String, result:String, url:String, result:String)
    def show_job(self, job_id):
        # use v3/job/status instead of v3/job/show to poll finish of a job
        with self.get("/v3/job/show/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show job failed", res, body)
            js = self.checked_json(body, ["status"])
            _type = js.get("type", "?")
            database = js.get("database")
            query = js.get("query")
            status = js.get("status")
            debug = js.get("debug")
            url = js.get("url")
            start_at = js.get("start_at")
            end_at = js.get("end_at")
            cpu_time = js.get("cpu_time")
            result_size = js.get("result_size") # compressed result size in msgpack.gz format
            result = js.get("result") # result target URL
            hive_result_schema = js.get("hive_result_schema", "")
            if hive_result_schema is None or len(str(hive_result_schema)) < 1:
                hive_result_schema = None
            else:
                hive_result_schema = json.loads(hive_result_schema)
            priority = js.get("priority")
            retry_limit = js.get("retry_limit")
            return [_type, query, status, url, debug, start_at, end_at, cpu_time,
                    result_size, result, hive_result_schema, priority, retry_limit, None, database]

    def job_status(self, job_id):
        with self.get("/v3/job/status/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Get job status failed", res, body)

            js = self.checked_json(body, ["status"])
            return js["status"]

    def job_result(self, job_id):
        with self.get("/v3/job/result/%s" % (urlquote(str(job_id))), {"format": "msgpack"}) as res:
            code = res.status
            if code != 200:
                self.raise_error("Get job result failed", res, body)
            unpacker = msgpack.Unpacker(res)
            for row in unpacker:
                yield row

    def job_result_raw(self, job_id, _format):
        with self.get("/v3/job/result/%s" % (urlquote(str(job_id))), {"format": _format}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Get job result failed", res, body)
            return body

    def kill(self, job_id):
        with self.post("/v3/job/kill/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Kill job failed", res, body)
            js = self.checked_json(body, [])
            former_status = js.get("former_status")
            return former_status

    # => jobId:String
    def hive_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        return self.query(q, "hive", db, result_url, priority, retry_limit, **kwargs)

    # => jobId:String
    def pig_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        return self.query(q, "pig", db, result_url, priority, retry_limit, **kwargs)

    # => jobId:String
    def query(self, q, _type="hive", db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        params = {"query": q}
        params.update(kwargs)
        if result_url is not None:
            params["result"] = result_url
        if priority is not None:
            params["priority"] = priority
        if retry_limit is not None:
            params["retry_limit"] = retry_limit
        with self.post("/v3/job/issue/%s/%s" % (urlquote(str(_type)), urlquote(str(db))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Query failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
