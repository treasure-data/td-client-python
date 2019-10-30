#!/usr/bin/env python

import json

import msgpack

from .util import create_url, get_or_else, parse_date


class TableAPI:
    """Access to Table API

    This class is inherited by :class:`tdclient.api.API`.
    """

    def list_tables(self, db):
        """Gets the list of table in the database.

        Args:
            db (str): Target database name.

        Returns:
            dict: Detailed table information.

        Examples:
            >>> td.api.list_tables("my_db")
            { 'iris': {'id': 21039862,
              'name': 'iris',
              'estimated_storage_size': 1236,
              'counter_updated_at': '2019-09-18T07:14:28Z',
              'last_log_timestamp': datetime.datetime(2019, 1, 30, 5, 34, 42, tzinfo=tzutc()),
              'delete_protected': False,
              'created_at': datetime.datetime(2019, 1, 30, 5, 34, 42, tzinfo=tzutc()),
              'updated_at': datetime.datetime(2019, 1, 30, 5, 34, 46, tzinfo=tzutc()),
              'type': 'log',
              'include_v': True,
              'count': 150,
              'schema': [['sepal_length', 'double', 'sepal_length'],
               ['sepal_width', 'double', 'sepal_width'],
               ['petal_length', 'double', 'petal_length'],
               ['petal_width', 'double', 'petal_width'],
               ['species', 'string', 'species']],
              'expire_days': None,
              'last_import': datetime.datetime(2019, 9, 18, 7, 14, 28, tzinfo=tzutc())},
            }
        """
        with self.get(create_url("/v3/table/list/{db}", db=db)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List tables failed", res, body)
            js = self.checked_json(body, ["tables"])
            result = {}
            for m in js["tables"]:
                m = dict(m)
                m["type"] = m.get("type", "?")
                m["count"] = int(m.get("count", 0))
                m["created_at"] = parse_date(
                    get_or_else(m, "created_at", "1970-01-01T00:00:00Z")
                )
                m["updated_at"] = parse_date(
                    get_or_else(m, "updated_at", "1970-01-01T00:00:00Z")
                )
                m["last_import"] = parse_date(
                    get_or_else(m, "counter_updated_at", "1970-01-01T00:00:00Z")
                )
                m["last_log_timestamp"] = parse_date(
                    get_or_else(m, "last_log_timestamp", "1970-01-01T00:00:00Z")
                )
                m["estimated_storage_size"] = int(m["estimated_storage_size"])
                m["schema"] = json.loads(m.get("schema", "[]"))
                result[m["name"]] = m
            return result

    def create_log_table(self, db, table):
        """Create a new table in the database and registers it in PlazmaDB.

        Args:
            db (str): Target database name.
            table (str): Target table name.

        Returns:
            bool: `True` if succeeded.
        """
        return self._create_table(db, table, "log")

    def _create_table(self, db, table, type, params=None):
        params = {} if params is None else params
        with self.post(
            create_url(
                "/v3/table/create/{db}/{table}/{type}", db=db, table=table, type=type
            ),
            params,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create %s table failed" % (type), res, body)
            return True

    def swap_table(self, db, table1, table2):
        """Swap the two specified tables with each other belonging to the same database
        and basically exchanges their names.

        Args:
            db (str): Target database name
            table1 (str): First target table for the swap.
            table2 (str): Second target table for the swap.
        Returns:
            bool: `True` if succeeded.
        """
        with self.post(
            create_url(
                "/v3/table/swap/{db}/{table1}/{table2}",
                db=db,
                table1=table1,
                table2=table2,
            )
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Swap tables failed", res, body)
            return True

    def update_schema(self, db, table, schema_json):
        """Update the table schema.

        Args:
            db (str): Target database name.
            table (str): Target table name.
            schema_json (str): Schema format JSON string. See also: ~`Client.update_schema`
                e.g. '[["sep_len", "long", "sep_len"], ["sep_wid", "long", "sep_wid"]]'

        Returns:
            bool: `True` if succeeded.
        """
        with self.post(
            create_url("/v3/table/update-schema/{db}/{table}", db=db, table=table),
            {"schema": schema_json},
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create schema table failed", res, body)
            return True

    def update_expire(self, db, table, expire_days):
        """Update the expire days for the specified table

        Args:
            db (str): Target database name.
            table (str): Target table name.
            expire_days (int): Number of days where the contents of the specified table
                would expire.
        Returns:
            bool: True if succeeded.
        """
        with self.post(
            create_url("/v3/table/update/{db}/{table}", db=db, table=table),
            {"expire_days": expire_days},
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Update table expiration failed", res, body)
            return True

    def delete_table(self, db, table):
        """Delete the specified table.

        Args:
            db (str): Target database name.
            table (str): Target table name.

        Returns:
            str: Type information of the table (e.g. "log").
        """
        with self.post(
            create_url("/v3/table/delete/{db}/{table}", db=db, table=table)
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete table failed", res, body)
            js = self.checked_json(body, [])
            t = js.get("type", "?")
            return t

    def tail(self, db, table, count, to=None, _from=None, block=None):
        """Get the contents of the table in reverse order based on the registered time
        (last data first).

        Args:
            db (str): Target database name.
            table (str): Target table name.
            count (int): Number for record to show up from the end.
            to: Deprecated parameter.
            _from: Deprecated parameter.
            block: Deprecated parameter.

        Returns:
            [dict]: Contents of the table.
        """
        params = {"count": count, "format": "msgpack"}
        with self.get(
            create_url("/v3/table/tail/{db}/{table}", db=db, table=table), params
        ) as res:
            code = res.status
            if code != 200:
                self.raise_error("Tail table failed", res, "")

            unpacker = msgpack.Unpacker(res, raw=False)
            result = []
            for row in unpacker:
                result.append(row)

            return result

    def change_database(self, db, table, dest_db):
        """Move a target table from it's original database to new destination database.

        Args:
            db (str): Target database name.
            table (str): Target table name.
            dest_db (str): Destination database name.

        Returns:
            bool: `True` if succeeded
        """
        params = {"dest_database_name": dest_db}
        with self.post(
            create_url("/v3/table/change_database/{db}/{table}", db=db, table=table),
            params,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Change database failed", res, body)
            return True
