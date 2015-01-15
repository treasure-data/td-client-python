#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import codecs
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

    def list_jobs(self, _from=0, to=None, status=None, conditions=None):
        """
        TODO: add docstring
        => [(jobId:str, type:str, status:str, start_at:str, end_at:str, result_url:str)]
        """
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
                type = m.get("type", "?")
                database = m.get("database")
                status = m.get("status")
                query = m.get("query")
                start_at = self.parsedate(self.get_or_else(m, "start_at", "1970-01-01T00:00:00Z"))
                end_at = self.parsedate(self.get_or_else(m, "end_at", "1970-01-01T00:00:00Z"))
                cpu_time = m.get("cpu_time")
                result_size = m.get("result_size") # compressed result size in msgpack.gz format
                result_url = m.get("result")
                priority = m.get("priority")
                retry_limit = m.get("retry_limit")
                result.append((job_id, type, status, query, start_at, end_at, cpu_time,
                     result_size, result_url, priority, retry_limit, None, database))
            return result

    def show_job(self, job_id):
        """
        TODO: add docstring
        => (type:str, status:str, result:str, url:str, result:str)
        """
        # use v3/job/status instead of v3/job/show to poll finish of a job
        with self.get("/v3/job/show/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show job failed", res, body)
            js = self.checked_json(body, ["status"])
            type = js.get("type", "?")
            database = js.get("database")
            query = js.get("query")
            status = js.get("status")
            debug = js.get("debug")
            url = js.get("url")
            start_at = self.parsedate(self.get_or_else(js, "start_at", "1970-01-01T00:00:00Z"))
            end_at = self.parsedate(self.get_or_else(js, "end_at", "1970-01-01T00:00:00Z"))
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
            return (type, query, status, url, debug, start_at, end_at, cpu_time,
                    result_size, result, hive_result_schema, priority, retry_limit, None, database)

    def job_status(self, job_id):
        """
        TODO: add docstring
        """
        with self.get("/v3/job/status/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Get job status failed", res, body)

            js = self.checked_json(body, ["status"])
            return js["status"]

    def job_result(self, job_id):
        """
        TODO: add docstring
        """
        result = []
        for row in self.job_result_format_each(job_id, "msgpack"):
            result.append(row)
        return result

    def job_result_each(self, job_id):
        """
        TODO: add docstring
        """
        for row in self.job_result_format_each(job_id, "msgpack"):
            yield row

    def job_result_format(self, job_id, format):
        """
        TODO: add docstring
        """
        result = []
        for row in self.job_result_format_each(job_id, format):
            result.append(row)
        return result

    def job_result_format_each(self, job_id, format):
        """
        TODO: add docstring
        """
        with self.get("/v3/job/result/%s" % (urlquote(str(job_id))), {"format": format}) as res:
            code = res.status
            if code != 200:
                self.raise_error("Get job result failed", res)
            if format == "msgpack":
                unpacker = msgpack.Unpacker(res, encoding=str("utf-8"))
                for row in unpacker:
                    yield row
            elif format == "json":
                unpacker = json.load(codecs.getreader("utf-8")(res))
                for row in unpacker:
                    yield row
            else:
                yield res.read()

    def kill(self, job_id):
        """
        TODO: add docstring
        """
        with self.post("/v3/job/kill/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Kill job failed", res, body)
            js = self.checked_json(body, [])
            former_status = js.get("former_status")
            return former_status

    def hive_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        """
        TODO: add docstring
        => jobId:str
        """
        return self.query(q, "hive", db, result_url, priority, retry_limit, **kwargs)

    def pig_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        """
        TODO: add docstring
        => jobId:str
        """
        return self.query(q, "pig", db, result_url, priority, retry_limit, **kwargs)

    def query(self, q, type="hive", db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        """
        TODO: add docstring
        => jobId:str
        """
        params = {"query": q}
        params.update(kwargs)
        if result_url is not None:
            params["result"] = result_url
        if priority is not None:
            params["priority"] = priority
        if retry_limit is not None:
            params["retry_limit"] = retry_limit
        with self.post("/v3/job/issue/%s/%s" % (urlquote(str(type)), urlquote(str(db))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Query failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
