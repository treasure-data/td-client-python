#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote
import warnings

class TableAPI(object):
    ####
    ## Table API
    ##

    def list_tables(self, db):
        """
        TODO: add docstring
        => {name:str => [type:str, count:int]}
        """
        with self.get("/v3/table/list/%s" % (urlquote(str(db)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List tables failed", res, body)
            js = self.checked_json(body, ["tables"])
            result = {}
            for m in js["tables"]:
                m = dict(m)
                m["type"] = m.get("type", "?")
                m["count"] = int(m.get("count", 0))
                m["created_at"] = self._parsedate(self.get_or_else(m, "created_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")
                m["updated_at"] = self._parsedate(self.get_or_else(m, "updated_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")
                m["last_import"] = self._parsedate(self.get_or_else(m, "counter_updated_at", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")
                m["last_log_timestamp"] = self._parsedate(self.get_or_else(m, "last_log_timestamp", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ")
                m["estimated_storage_size"] = int(m["estimated_storage_size"])
                m["schema"] = json.loads(m.get("schema", "[]"))
                result[m["name"]] = m
            return result

    def create_log_table(self, db, table):
        """
        TODO: add docstring
        => True
        """
        return self._create_table(db, table, "log")

    def create_item_table(self, db, table, primary_key, primary_key_type):
        """
        TODO: add docstring
        => True
        """
        warnings.warn("item tables have been deprecated. will be deleted from future releases.", category=DeprecationWarning)
        params = {"primary_key": primary_key, "primary_key_type": primary_key_type}
        return self._create_table(db, table, "item", params)

    def _create_table(self, db, table, type, params=None):
        params = {} if params is None else params
        with self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(type))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create %s table failed" % (type), res, body)
            return True

    def swap_table(self, db, table1, table2):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/table/swap/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table1)), urlquote(str(table2)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Swap tables failed", res, body)
            return True

    def update_schema(self, db, table, schema_json):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/table/update-schema/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"schema": schema_json}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create schema table failed", res, body)
            return True

    def update_expire(self, db, table, expire_days):
        """
        TODO: add docstring
        """
        with self.post("/v3/table/update/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"expire_days": expire_days}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Update table expiration failed", res, body)
            return True

    def delete_table(self, db, table):
        """
        TODO: add docstring
        => type:str
        """
        with self.post("/v3/table/delete/%s/%s" % (urlquote(str(db)), urlquote(str(table)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete table failed", res, body)
            js = self.checked_json(body, [])
            t = js.get("type", "?")
            return t
