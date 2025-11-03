#!/usr/bin/env python

import datetime
from typing import TYPE_CHECKING, Any

from tdclient.model import Model

if TYPE_CHECKING:
    from tdclient.client import Client
    from tdclient.job_model import Job
    from tdclient.table_model import Table


class Database(Model):
    """Database on Treasure Data Service"""

    PERMISSIONS = ["administrator", "full_access", "import_only", "query_only"]
    PERMISSION_LIST_TABLES = ["administrator", "full_access"]

    def __init__(self, client: "Client", db_name: str, **kwargs: Any) -> None:
        super().__init__(client)
        self._db_name = db_name
        self._tables: list[Table] | None = kwargs.get("tables")
        self._count: int | None = kwargs.get("count")
        self._created_at: datetime.datetime | None = kwargs.get("created_at")
        self._updated_at: datetime.datetime | None = kwargs.get("updated_at")
        self._org_name: str | None = kwargs.get("org_name")
        self._permission: str | None = kwargs.get("permission")

    @property
    def org_name(self) -> str | None:
        """
        str: organization name
        """
        return self._org_name

    @property
    def permission(self) -> str | None:
        """
        str: permission for the database (e.g. "administrator", "full_access", etc.)
        """
        return self._permission

    @property
    def count(self) -> int | None:
        """
        int: Total record counts in a database.
        """
        return self._count

    @property
    def name(self) -> str:
        """
        str: a name of the database
        """
        return self._db_name

    def tables(self) -> list["Table"]:
        """
        Returns:
             a list of :class:`tdclient.model.Table`
        """
        if self._tables is None:
            self._update_tables()
        assert self._tables is not None
        return self._tables

    def create_log_table(self, name: str) -> "Table":
        """
        Args:
            name (str): name of new log table

        Returns:
             :class:`tdclient.model.Table`
        """
        return self._client.create_log_table(self._db_name, name)

    def table(self, table_name: str) -> "Table":
        """
        Args:
            table_name (str): name of a table

        Returns:
             :class:`tdclient.model.Table`
        """
        return self._client.table(self._db_name, table_name)

    def delete(self) -> bool:
        """Delete the database

        Returns:
             `True` if success
        """
        return self._client.delete_database(self._db_name)

    def query(self, q: str, **kwargs: Any) -> "Job":
        """Run a query on the database

        Args:
            q (str): a query string

        Returns:
            :class:`tdclient.model.Job`
        """
        return self._client.query(self._db_name, q, **kwargs)

    @property
    def created_at(self) -> datetime.datetime | None:
        """
        :class:`datetime.datetime`
        """
        return self._created_at

    @property
    def updated_at(self) -> datetime.datetime | None:
        """
        :class:`datetime.datetime`
        """
        return self._updated_at

    def _update_tables(self) -> None:
        self._tables = self._client.tables(self._db_name)
        assert self._tables is not None
        for table in self._tables:
            table.database = self
