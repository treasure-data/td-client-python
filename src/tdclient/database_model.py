#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import warnings

from tdclient.model import Model

class Database(Model):
    """Database on Treasure Data Service
    """

    PERMISSIONS = ["administrator", "full_access", "import_only", "query_only"]
    PERMISSION_LIST_TABLES = ["administrator", "full_access"]

    def __init__(self, client, db_name, **kwargs):
        super(Database, self).__init__(client)
        self._db_name = db_name
        self._tables = kwargs.get("tables")
        self._count = kwargs.get("count")
        self._created_at = kwargs.get("created_at")
        self._updated_at = kwargs.get("updated_at")
        self._org_name = kwargs.get("org_name")
        self._permission = kwargs.get("permission")

    @property
    def org_name(self):
        """
        Returns: organization name
        """
        return self._org_name

    @property
    def permission(self):
        """
        Returns: a string represents permission for the database (e.g. "administrator", "full_access", etc.)
        """
        return self._permission

    @property
    def count(self):
        """
        TODO: add docstring
        """
        return self._count

    @property
    def name(self):
        """
        Returns: a name of the database in string
        """
        return self._db_name

    def tables(self):
        """
        Returns: a list of :class:`tdclient.model.Table`
        """
        if self._tables is None:
            self._update_tables()
        return self._tables

    def create_log_table(self, name):
        """
        Params:
            name (str): name of new log table

        Returns: :class:`tdclient.model.Table`
        """
        return self._client.create_log_table(self._db_name, name)

    def create_item_table(self, name):
        """
        Params:
            name (str): name of new item table

        Returns: :class:`tdclient.model.Table`
        """
        warnings.warn("item tables have been deprecated. will be deleted from future releases.", category=DeprecationWarning)
        return self._client.create_item_table(self._db_name, name)

    def table(self, table_name):
        """
        Params:
            table_name (str): name of a table

        Returns: :class:`tdclient.model.Table`
        """
        return self._client.table(self._db_name, table_name)

    def delete(self):
        """Delete the database

        Returns: `True` if success
        """
        return self._client.delete_database(self._db_name)

    def query(self, q, **kwargs):
        """Run a query on the database

        Params:
            q (str): a query string

        Returns: :class:`tdclient.model.Job`
        """
        return self._client.query(self._db_name, q, **kwargs)

    @property
    def created_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._created_at

    @property
    def updated_at(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._updated_at

    def _update_tables(self):
        self._tables = self._client.tables(self._db_name)
        for table in self._tables:
            table.database = self
