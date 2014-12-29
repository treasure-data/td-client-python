#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

class DatabaseAPI(object):
    ####
    ## Database API
    ##

    # => [name:String]
    def list_databases(self):
        code, body, res = self.get("/v3/database/list")
        if code != 200:
            self.raise_error("List databases failed", res, body)
        js = self.checked_json(body, ["databases"])
        result = {}
        for m in js["databases"]:
            name = m.get("name")
            count = m.get("count")
            created_at = m.get("created_at")
            updated_at = m.get("updated_at")
            permission = m.get("permission")
            result[name] = [count, created_at, updated_at, None, permission] # set nil to org for API copatibility
        return result

    # => true
    def delete_database(self, db):
        code, body, res = self.post("/v3/database/delete/%s" % urlquote(str(db)))
        if code != 200:
            self.raise_error("Delete database failed", res, body)
        return True

    # => true
    def create_database(self, db, params):
        code, body, res = self.post("/v3/database/create/%s" % urlquote(str(db)), params)
        if code != 200:
            self.raise_error("Create database failed", res, body)
        return True
