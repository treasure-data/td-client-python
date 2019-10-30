#!/usr/bin/env python

from .util import create_url, get_or_else, parse_date


class DatabaseAPI:
    """Access to Database of Treasure Data Service.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def list_databases(self):
        """Get the list of all the databases of the account.

        Returns:
            dict: Detailed database information. Each key of the dict is database name.
        """
        with self.get("/v3/database/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List databases failed", res, body)
            js = self.checked_json(body, ["databases"])
            result = {}
            for m in js["databases"]:
                name = m.get("name")
                m = dict(m)
                m["created_at"] = parse_date(
                    get_or_else(m, "created_at", "1970-01-01T00:00:00Z")
                )
                m["updated_at"] = parse_date(
                    get_or_else(m, "updated_at", "1970-01-01T00:00:00Z")
                )
                m["org_name"] = None  # set None to org for API compatibility
                result[name] = m
            return result

    def delete_database(self, db):
        """Delete a database.

        Args:
            db (str): Target database name.
        Returns:
            bool: `True` if succeeded.
        """
        with self.post(create_url("/v3/database/delete/{db}", db=db)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete database failed", res, body)
            return True

    def create_database(self, db, params=None):
        """Create a new database with the given name.

        Args:
            db (str): Target database name.
            params (dict): Extra parameters.
        Returns:
            bool: `True` if succeeded.
        """
        params = {} if params is None else params
        with self.post(create_url("/v3/database/create/{db}", db=db), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create database failed", res, body)
            return True
