#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class DatabaseAPI(object):
    ####
    ## Database API
    ##

    def list_databases(self):
        """
        TODO: add docstring
        => [name:str]
        """
        with self.get("/v3/database/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List databases failed", res, body)
            js = self.checked_json(body, ["databases"])
            result = {}
            for m in js["databases"]:
                name = m.get("name")
                count = m.get("count")
                created_at = self.parsedate(self.get_or_else(m, "created_at", "1970-01-01T00:00:00Z"))
                updated_at = self.parsedate(self.get_or_else(m, "updated_at", "1970-01-01T00:00:00Z"))
                permission = m.get("permission")
                result[name] = [count, created_at, updated_at, None, permission] # set None to org for API copatibility
            return result

    def delete_database(self, db):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/database/delete/%s" % urlquote(str(db))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete database failed", res, body)
            return True

    def create_database(self, db, params={}):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/database/create/%s" % urlquote(str(db)), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create database failed", res, body)
            return True
