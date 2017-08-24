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
import warnings

class JobAPI(object):
    ####
    ## Job API
    ##

    JOB_PRIORITY = {
        "VERY LOW": -2,
        "VERY-LOW": -2,
        "VERY_LOW": -2,
        "LOW": -1,
        "NORM": 0,
        "NORMAL": 0,
        "HIGH": 1,
        "VERY HIGH": 2,
        "VERY-HIGH": 2,
        "VERY_HIGH": 2,
    }

    def list_jobs(self, _from=0, to=None, status=None, conditions=None):
        """
        Params:
            _from (int):
            to (int):
            status (str):
            conditions (str):

        Returns: a list of :class:`dict` which represents a job
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
            jobs = []
            for m in js["jobs"]:
                if m.get("result") is not None and 0 < len(str(m["result"])):
                    result = m["result"]
                else:
                    result = None
                if m.get("hive_result_schema") is not None and 0 < len(str(m["hive_result_schema"])):
                    hive_result_schema = json.loads(m["hive_result_schema"])
                else:
                    hive_result_schema = None
                job = {
                    "job_id": m.get("job_id"),
                    "type": m.get("type", "?"),
                    "url": m.get("url"),
                    "query": m.get("query"),
                    "status": m.get("status"),
                    "debug": m.get("debug"),
                    "start_at": self._parsedate(self.get_or_else(m, "start_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ"),
                    "end_at": self._parsedate(self.get_or_else(m, "end_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ"),
                    "cpu_time": m.get("cpu_time"),
                    "result_size": m.get("result_size"), # compressed result size in msgpack.gz format
                    "result": result,
                    "result_url": m.get("result_url"),
                    "hive_result_schema": hive_result_schema,
                    "priority": m.get("priority"),
                    "retry_limit": m.get("retry_limit"),
                    "org_name": None,
                    "database": m.get("database"),
                }
                jobs.append(job)
            return jobs

    def show_job(self, job_id):
        """
        Params:
            job_id (str): job ID

        Returns: :class:`dict`
        """
        # use v3/job/status instead of v3/job/show to poll finish of a job
        with self.get("/v3/job/show/%s" % (urlquote(str(job_id)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show job failed", res, body)
            js = self.checked_json(body, ["status"])
            if js.get("result") is not None and 0 < len(str(js["result"])):
                result = js["result"]
            else:
                result = None
            if js.get("hive_result_schema") is not None and 0 < len(str(js["hive_result_schema"])):
                hive_result_schema = json.loads(js["hive_result_schema"])
            else:
                hive_result_schema = None
            job = {
                "job_id": job_id,
                "type": js.get("type", "?"),
                "url": js.get("url"),
                "query": js.get("query"),
                "status": js.get("status"),
                "debug": js.get("debug"),
                "start_at": self._parsedate(self.get_or_else(js, "start_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ"),
                "end_at": self._parsedate(self.get_or_else(js, "end_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ"),
                "cpu_time": js.get("cpu_time"),
                "result_size": js.get("result_size"), # compressed result size in msgpack.gz format
                "result": result,
                "result_url": js.get("result_url"),
                "hive_result_schema": hive_result_schema,
                "priority": js.get("priority"),
                "retry_limit": js.get("retry_limit"),
                "org_name": None,
                "database": js.get("database"),
            }
            return job

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
                self.raise_error("Get job result failed", res, "")
            if format == "msgpack":
                unpacker = msgpack.Unpacker(res, encoding=str("utf-8"))
                for row in unpacker:
                    yield row
            elif format == "json":
                for row in codecs.getreader("utf-8")(res):
                    yield json.loads(row)
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

    def hive_query(self, q, **kwargs):
        warnings.warn("hive_query(q, ...) will be removed from future release. Please use query(q, type=\"hive\", ...)", category=DeprecationWarning)
        kwargs = dict(kwargs)
        kwargs["type"] = "hive"
        return self.query(q, **kwargs)

    def pig_query(self, q, **kwargs):
        warnings.warn("pig_query(q, ...) will be removed from future release. Please use query(q, type=\"pig\", ...)", category=DeprecationWarning)
        kwargs = dict(kwargs)
        kwargs["type"] = "pig"
        return self.query(q, **kwargs)

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
            if not isinstance(priority, int):
                priority_name = str(priority).upper()
                if priority_name in self.JOB_PRIORITY:
                    priority = self.JOB_PRIORITY[priority_name]
                else:
                    raise(ValueError("unknown job priority: %s" % (priority_name,)))
            params["priority"] = priority
        if retry_limit is not None:
            params["retry_limit"] = retry_limit
        with self.post("/v3/job/issue/%s/%s" % (urlquote(str(type)), urlquote(str(db))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Query failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
