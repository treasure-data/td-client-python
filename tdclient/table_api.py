#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import json
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class TableAPI(object):
    ####
    ## Table API
    ##

    # => {name:String => [type:Symbol, count:Integer]}
    def list_tables(self, db):
        with self.get("/v3/table/list/%s" % (urlquote(str(db)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List tables failed", res, body)
            js = self.checked_json(body, ["tables"])
            result = {}
            for m in js["tables"]:
                name = m.get("name")
                _type = m.get("type", "?")
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
                result[name] = (_type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days, primary_key, primary_key_type)
            return result

    def _create_log_or_item_table(self, db, table, _type):
        with self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(_type)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create #{type} table failed", res, body)
            return True

    # => true
    def create_log_table(self, db, table):
        return self._create_table(db, table, "log")

    # => true
    def create_item_table(self, db, table, primary_key, primary_key_type):
        params = {"primary_key": primary_key, "primary_key_type": primary_key_type}
        return self._create_table(db, table, "item", params)

    def _create_table(self, db, table, _type, params={}):
        with self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(_type))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create %s table failed" % (_type), res, body)
            return True

    # => true
    def swap_table(self, db, table1, table2):
        with self.post("/v3/table/swap/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table1)), urlquote(str(table2)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Swap tables failed", res, body)
            return True

    # => true
    def update_schema(self, db, table, schema_json):
        with self.post("/v3/table/update-schema/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"schema": schema_json}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create schema table failed", res, body)
            return True

    def update_expire(self, db, table, expire_days):
        with self.post("/v3/table/update/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"expire_days": expire_days}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Update table expiration failed", res, body)
            return True

    # => type:Symbol
    def delete_table(self, db, table):
        with self.post("/v3/table/delete/%s/%s" % (urlquote(str(db)), urlquote(str(table)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete table failed", res, body)
            js = self.checked_json(body, [])
            _type = js.get("type", "?")
            return _type
