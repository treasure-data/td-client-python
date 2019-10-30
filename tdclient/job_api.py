#!/usr/bin/env python

import codecs
import json

import msgpack

from .util import create_url, get_or_else, parse_date


class JobAPI:
    """Access to Job API

    This class is inherited by :class:`tdclient.api.API`.
    """

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
        """Show the list of Jobs.

        Args:
            _from (int): Gets the Job from the nth index in the list. Default: 0
            to (int, optional): Gets the Job up to the nth index in the list.
                By default, the first 20 jobs in the list are displayed
            status (str, optional): Filter by given status. {"queued", "running", "success", "error"}
            conditions (str, optional): Condition for ``TIMESTAMPDIFF()`` to search for slow queries.
                Avoid using this parameter as it can be dangerous.

        Returns:
             a list of :class:`dict` which represents a job
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
                if m.get("hive_result_schema") is not None and 0 < len(
                    str(m["hive_result_schema"])
                ):
                    hive_result_schema = json.loads(m["hive_result_schema"])
                else:
                    hive_result_schema = None
                start_at = get_or_else(m, "start_at")
                end_at = get_or_else(m, "end_at")
                created_at = get_or_else(m, "created_at")
                updated_at = get_or_else(m, "updated_at")
                job = {
                    "job_id": m.get("job_id"),
                    "type": m.get("type", "?"),
                    "url": m.get("url"),
                    "query": m.get("query"),
                    "status": m.get("status"),
                    "debug": m.get("debug"),
                    "start_at": parse_date(start_at) if start_at else None,
                    "end_at": parse_date(end_at) if end_at else None,
                    "created_at": parse_date(created_at) if created_at else None,
                    "updated_at": parse_date(updated_at) if updated_at else None,
                    "cpu_time": m.get("cpu_time"),
                    "result_size": m.get(
                        "result_size"
                    ),  # compressed result size in msgpack.gz format
                    "result": result,
                    "result_url": m.get("result_url"),
                    "hive_result_schema": hive_result_schema,
                    "priority": m.get("priority"),
                    "retry_limit": m.get("retry_limit"),
                    "org_name": None,
                    "database": m.get("database"),
                    "num_records": m.get("num_records"),
                    "user_name": m.get("user_name"),
                    "linked_result_export_job_id": m.get("linked_result_export_job_id"),
                    "result_export_target_job_id": m.get("result_export_target_job_id"),
                }
                jobs.append(job)
            return jobs

    def show_job(self, job_id):
        """Return detailed information of a Job.

        Args:
            job_id (str): job ID

        Returns:
             :class:`dict`: Detailed information of a job
        """
        # use v3/job/status instead of v3/job/show to poll finish of a job
        with self.get(create_url("/v3/job/show/{job_id}", job_id=job_id)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show job failed", res, body)
            js = self.checked_json(body, ["status"])
            if js.get("result") is not None and 0 < len(str(js["result"])):
                result = js["result"]
            else:
                result = None
            if js.get("hive_result_schema") is not None and 0 < len(
                str(js["hive_result_schema"])
            ):
                hive_result_schema = json.loads(js["hive_result_schema"])
            else:
                hive_result_schema = None
            start_at = get_or_else(js, "start_at")
            end_at = get_or_else(js, "end_at")
            created_at = get_or_else(js, "created_at")
            updated_at = get_or_else(js, "updated_at")
            job = {
                "job_id": job_id,
                "type": js.get("type", "?"),
                "url": js.get("url"),
                "query": js.get("query"),
                "status": js.get("status"),
                "debug": js.get("debug"),
                "start_at": parse_date(start_at) if start_at else None,
                "end_at": parse_date(end_at) if end_at else None,
                "created_at": parse_date(created_at) if created_at else None,
                "updated_at": parse_date(updated_at) if updated_at else None,
                "cpu_time": js.get("cpu_time"),
                "result_size": js.get(
                    "result_size"
                ),  # compressed result size in msgpack.gz format
                "result": result,
                "result_url": js.get("result_url"),
                "hive_result_schema": hive_result_schema,
                "priority": js.get("priority"),
                "retry_limit": js.get("retry_limit"),
                "org_name": None,
                "database": js.get("database"),
                "num_records": js.get("num_records"),
                "user_name": js.get("user_name"),
                "linked_result_export_job_id": js.get("linked_result_export_job_id"),
                "result_export_target_job_id": js.get("result_export_target_job_id"),
            }
            return job

    def job_status(self, job_id):
        """"Show job status
        Args:
            job_id (str): job ID

        Returns:
             The status information of the given job id at last execution.
        """
        with self.get(create_url("/v3/job/status/{job_id}", job_id=job_id)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Get job status failed", res, body)

            js = self.checked_json(body, ["status"])
            return js["status"]

    def job_result(self, job_id):
        """Return the job result.

        Args:
            job_id (int): Job ID

        Returns:
             Job result in :class:`list`
        """
        result = []
        for row in self.job_result_format_each(job_id, "msgpack"):
            result.append(row)
        return result

    def job_result_each(self, job_id):
        """Yield a row of the job result.

        Args:
            job_id (int): Job ID

        Yields:
             Row in a result
        """
        for row in self.job_result_format_each(job_id, "msgpack"):
            yield row

    def job_result_format(self, job_id, format):
        """Return the job result with specified format.

        Args:
            job_id (int): Job ID
            format (str): Output format of the job result information.
                "json" or "msgpack"

        Returns:
             The query result of the specified job in.
        """
        result = []
        for row in self.job_result_format_each(job_id, format):
            result.append(row)
        return result

    def job_result_format_each(self, job_id, format):
        """Yield a row of the job result with specified format.

        Args:
            job_id (int): job ID
            format (str): Output format of the job result information. "json" or "msgpack"

        Yields:
             The query result of the specified job in.
        """
        with self.get(
            create_url("/v3/job/result/{job_id}", job_id=job_id), {"format": format}
        ) as res:
            code = res.status
            if code != 200:
                self.raise_error("Get job result failed", res, "")
            if format == "msgpack":
                unpacker = msgpack.Unpacker(res, raw=False)
                for row in unpacker:
                    yield row
            elif format == "json":
                for row in codecs.getreader("utf-8")(res):
                    yield json.loads(row)
            else:
                yield res.read()

    def kill(self, job_id):
        """Stop the specific job if it is running.

        Args:
            job_id (str): Job Id to kill

        Returns:
             Job status before killing
        """
        with self.post(create_url("/v3/job/kill/{job_id}", job_id=job_id)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Kill job failed", res, body)
            js = self.checked_json(body, [])
            former_status = js.get("former_status")
            return former_status

    def query(
        self,
        q,
        type="hive",
        db=None,
        result_url=None,
        priority=None,
        retry_limit=None,
        **kwargs
    ):
        """Create a job for given query.

        Args:
            q (str): Query string.
            type (str): Query type. `hive`, `presto`, `bulkload`. Default: `hive`
            db (str): Database name.
            result_url (str): Result output URL. e.g.,
                ``postgresql://<username>:<password>@<hostname>:<port>/<database>/<table>``
            priority (int or str): Job priority.
                In str, "Normal", "Very low", "Low", "High", "Very high".
                In int, the number in the range of -2 to 2.
            retry_limit (int): Automatic retry count.
            **kwargs: Extra options.

        Returns:
            str: Job ID issued for the query
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
                    raise (ValueError("unknown job priority: %s" % (priority_name,)))
            params["priority"] = priority
        if retry_limit is not None:
            params["retry_limit"] = retry_limit
        with self.post(
            create_url("/v3/job/issue/{type}/{db}", type=type, db=db), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Query failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
