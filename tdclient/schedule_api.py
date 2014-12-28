#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

class ScheduleAPI(object):
    ####
    ## Schedule API
    ##

    # => start:String
    def create_schedule(self, name, params={}):
        params.update({"type": params.get("type", "hive")})
        code, body, res = self.post("/v3/schedule/create/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Create schedule failed", res)
        js = self.checked_json(body, ["start"])
        return js["start"]

    # => cron:String, query:String
    def delete_schedule(self, name):
        code, body, res = self.post("/v3/schedule/delete/%s" % (urlquote(str(name))))
        if code != 200:
            self.raise_error("Delete schedule failed", res)
        js = self.checked_json(body, [])
        return (js["cron"], js["query"])

    # => [(name:String, cron:String, query:String, database:String, result_url:String)]
    def list_schedules(self):
        code, body, res = self.get("/v3/schedule/list")
        if code != 200:
            self.raise_error("List schedules failed", res)
        js = self.checked_json(body, ["schedules"])
        def schedule(m):
            name = m.get("name")
            cron = m.get("cron")
            query = m.get("query")
            database = m.get("database")
            result_url = m.get("result")
            timezone = m.get("timezone")
            delay = m.get("delay")
            next_time = m.get("next_time")
            priority = m.get("priority")
            retry_limit = m.get("retry_limit")
            return [name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, None] # same as database
        return [ schedule(m) for m in js["schedules"] ]

    def update_schedule(self, name, params):
      code, body, res = post("/v3/schedule/update/%s" % (urlquote(str(name))), params)
      if code != 200:
          self.raise_error("Update schedule failed", res)
      return None

    def history(self, name, _from=0, to=None):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        code, body, res = self.get("/v3/schedule/history/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("List history failed", res)
        js = self.checked_json(body, ["history"])
        def history(m):
            job_id = m.get("job_id")
            _type = m.get("type", "?")
            database = m.get("database")
            status = m.get("status")
            query = m.get("query")
            start_at = m.get("start_at")
            end_at = m.get("end_at")
            scheduled_at = m.get("scheduled_at")
            result_url = m.get("result")
            priority = m.get("priority")
            return [scheduled_at, job_id, _type, status, query, start_at, end_at, result_url, priority, database]
        return [ history(m) for m in js["history"] ]

    def run_schedule(self, name, time, num):
        params = {}
        if num is not None:
            params = {"num": num}
        code, body, res = self.post("/v3/schedule/run/%s/%s" % (urlquote(str(name)), urlquote(str(time))), params)
        if code != 200:
            self.raise_error("Run schedule failed", res)
        js = self.checked_json(body, ["jobs"])
        def job(m):
            job_id = m.get("job_id")
            scheduled_at = m.get("scheduled_at")
            _type = m.get("type", "?")
            return [job_id, _type, scheduled_at]
        return [ job(m) for m in js["jobs"] ]


