#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class ScheduleAPI(object):
    ####
    ## Schedule API
    ##

    def create_schedule(self, name, params={}):
        """
        TODO: add docstring
        => start:str
        """
        params.update({"type": params.get("type", "hive")})
        with self.post("/v3/schedule/create/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create schedule failed", res, body)
            js = self.checked_json(body, ["start"])
            return self.parsedate(js["start"])

    def delete_schedule(self, name):
        """
        TODO: add docstring
        => cron:str, query:str
        """
        with self.post("/v3/schedule/delete/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete schedule failed", res, body)
            js = self.checked_json(body, ["cron", "query"])
            return (js["cron"], js["query"])

    def list_schedules(self):
        """
        TODO: add docstring
        => [(name:str, cron:str, query:str, database:str, result_url:str)]
        """
        with self.get("/v3/schedule/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List schedules failed", res, body)
            js = self.checked_json(body, ["schedules"])
            def schedule(m):
                name = m.get("name")
                cron = m.get("cron")
                query = m.get("query")
                database = m.get("database")
                result_url = m.get("result")
                timezone = m.get("timezone", "UTC")
                delay = m.get("delay")
                next_time = self.parsedate(self.get_or_else(m, "next_time", "1970-01-01T00:00:00Z"))
                priority = m.get("priority")
                retry_limit = m.get("retry_limit")
                return (name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, None) # same as database
            return [ schedule(m) for m in js["schedules"] ]

    def update_schedule(self, name, params={}):
        """
        TODO: add docstring
        """
        with self.post("/v3/schedule/update/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Update schedule failed", res, body)

    def history(self, name, _from=0, to=None):
        """
        TODO: add docstring
        """
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        with self.get("/v3/schedule/history/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List history failed", res, body)
            js = self.checked_json(body, ["history"])
            def history(m):
                job_id = m.get("job_id")
                type = m.get("type", "?")
                database = m.get("database")
                status = m.get("status")
                query = m.get("query")
                start_at = self.parsedate(self.get_or_else(m, "start_at", "1970-01-01T00:00:00Z"))
                end_at = self.parsedate(self.get_or_else(m, "end_at", "1970-01-01T00:00:00Z"))
                scheduled_at = self.parsedate(self.get_or_else(m, "scheduled_at", "1970-01-01T00:00:00Z"))
                result_url = m.get("result")
                priority = m.get("priority")
                return (scheduled_at, job_id, type, status, query, start_at, end_at, result_url, priority, database)
            return [ history(m) for m in js["history"] ]

    def run_schedule(self, name, time, num=None):
        """
        TODO: add docstring
        """
        params = {}
        if num is not None:
            params = {"num": num}
        with self.post("/v3/schedule/run/%s/%s" % (urlquote(str(name)), urlquote(str(time))), params) as res:
            code, body = res.status, res.read()
        if code != 200:
            self.raise_error("Run schedule failed", res, body)
        js = self.checked_json(body, ["jobs"])
        def job(m):
            job_id = m.get("job_id")
            scheduled_at = self.parsedate(self.get_or_else(m, "scheduled_at", "1970-01-01T00:00:00Z"))
            type = m.get("type", "?")
            return (job_id, type, scheduled_at)
        return [ job(m) for m in js["jobs"] ]
