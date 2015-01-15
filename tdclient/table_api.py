#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

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
                name = m.get("name")
                type = m.get("type", "?")
                count = int(m.get("count", 0))
                created_at = self.parsedate(self.get_or_else(m, "created_at", "1970-01-01T00:00:00Z"))
                updated_at = self.parsedate(self.get_or_else(m, "updated_at", "1970-01-01T00:00:00Z"))
                last_import = self.parsedate(self.get_or_else(m, "counter_updated_at", "1970-01-01T00:00:00Z"))
                last_log_timestamp = self.parsedate(self.get_or_else(m, "last_log_timestamp", "1970-01-01T00:00:00Z"))
                estimated_storage_size = int(m.get("estimated_storage_size", 0))
                schema = json.loads(m.get("schema", "[]"))
                expire_days = m.get("expire_days")
                primary_key = m.get("primary_key")
                primary_key_type = m.get("primary_key_type")
                result[name] = (type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days, primary_key, primary_key_type)
            return result

    def _create_log_or_item_table(self, db, table, type):
        with self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(type)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create #{type} table failed", res, body)
            return True

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
        params = {"primary_key": primary_key, "primary_key_type": primary_key_type}
        return self._create_table(db, table, "item", params)

    def _create_table(self, db, table, type, params={}):
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
            type = js.get("type", "?")
            return type
