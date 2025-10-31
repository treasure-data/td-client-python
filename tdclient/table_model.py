#!/usr/bin/env python

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

from tdclient.model import Model
from tdclient.types import DataFormat, FileLike

if TYPE_CHECKING:
    from tdclient.database_model import Database
    from tdclient.job_model import Job


class Table(Model):
    """Database table on Treasure Data Service"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(args[0])

        self.database: Database | None = None
        self._db_name: str = args[1]
        self._table_name: str = args[2]

        if 3 < len(args):
            self._type: str | None = args[3]
            self._schema: list[tuple[str, str, str]] | None = args[4]
            self._count: int | None = args[5]
        else:
            self._type = kwargs.get("type")
            self._schema = kwargs.get("schema")
            self._count = kwargs.get("count")

        self._created_at: datetime.datetime | None = kwargs.get("created_at")
        self._updated_at: datetime.datetime | None = kwargs.get("updated_at")
        self._estimated_storage_size: int | None = kwargs.get("estimated_storage_size")
        self._last_import: datetime.datetime | None = kwargs.get("last_import")
        self._last_log_timestamp: datetime.datetime | None = kwargs.get(
            "last_log_timestamp"
        )
        self._expire_days: int | None = kwargs.get("expire_days")
        self._primary_key: str | None = kwargs.get("primary_key")
        self._primary_key_type: str | None = kwargs.get("primary_key_type")

    @property
    def type(self) -> str | None:
        """a string represents the type of the table"""
        return self._type

    @property
    def db_name(self) -> str:
        """a string represents the name of the database"""
        return self._db_name

    @property
    def table_name(self) -> str:
        """a string represents the name of the table"""
        return self._table_name

    @property
    def schema(self) -> list[tuple[str, str, str]] | None:
        """
        [[column_name:str, column_type:str, alias:str]]: The :obj:`list` of a schema
        """
        return self._schema

    @property
    def count(self) -> int | None:
        """int: total number of the table"""
        return self._count

    @property
    def estimated_storage_size(self) -> int | None:
        """estimated storage size"""
        return self._estimated_storage_size

    @property
    def primary_key(self) -> str | None:
        """
        TODO: add docstring
        """
        return self._primary_key

    @property
    def primary_key_type(self) -> str | None:
        """
        TODO: add docstring
        """
        return self._primary_key_type

    @property
    def database_name(self) -> str:
        """a string represents the name of the database"""
        return self._db_name

    @property
    def name(self) -> str:
        """a string represents the name of the table"""
        return self._table_name

    @property
    def created_at(self) -> datetime.datetime | None:
        """
        :class:`datetime.datetime`: Created datetime
        """
        return self._created_at

    @property
    def updated_at(self) -> datetime.datetime | None:
        """
        :class:`datetime.datetime`: Updated datetime
        """
        return self._updated_at

    @property
    def last_import(self) -> datetime.datetime | None:
        """:class:`datetime.datetime`"""
        return self._last_import

    @property
    def last_log_timestamp(self) -> datetime.datetime | None:
        """:class:`datetime.datetime`"""
        return self._last_log_timestamp

    @property
    def expire_days(self) -> int | None:
        """an int represents the days until expiration"""
        return self._expire_days

    @property
    def permission(self) -> str | None:
        """
        str: permission for the database (e.g. "administrator", "full_access", etc.)
        """
        if self.database is None:
            self._update_database()
        assert self.database is not None
        return self.database.permission

    @property
    def identifier(self) -> str:
        """a string identifier of the table"""
        return f"{self._db_name}.{self._table_name}"

    def delete(self) -> str:
        """a string represents the type of deleted table"""
        return self._client.delete_table(self._db_name, self._table_name)

    def tail(
        self, count: int, to: int | None = None, _from: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Args:
            count (int): Number for record to show up from the end.
            to: Deprecated parameter.
            _from: Deprecated parameter.

        Returns:
             the contents of the table in reverse order based on the registered time
             (last data first).
        """
        return self._client.tail(self._db_name, self._table_name, count, to, _from)

    def import_data(
        self,
        format: DataFormat,
        bytes_or_stream: FileLike,
        size: int,
        unique_id: str | None = None,
    ) -> float:
        """Import data into Treasure Data Service

        Args:
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns:
             second in float represents elapsed time to import data
        """
        return self._client.import_data(
            self._db_name,
            self._table_name,
            format,
            bytes_or_stream,
            size,
            unique_id=unique_id,
        )

    def import_file(
        self, format: DataFormat, file: FileLike, unique_id: str | None = None
    ) -> float:
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

        Args:
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns:
             float represents the elapsed time to import data
        """
        return self._client.import_file(
            self._db_name, self._table_name, format, file, unique_id=unique_id
        )

    def export_data(self, storage_type: str, **kwargs: Any) -> Job:
        """Export data from Treasure Data Service

        Args:
            storage_type (str): type of the storage
            **kwargs (dict): optional parameters. Assuming the following keys:

                - access_key_id (str):
                    ID to access the information to be exported.
                - secret_access_key (str):
                    Password for the `access_key_id`.
                - file_prefix (str, optional):
                    Filename of exported file.
                    Default: "<database_name>/<table_name>"
                - file_format (str, optional):
                    File format of the information to be
                    exported. {"jsonl.gz", "tsv.gz", "json.gz"}
                - from (int, optional):
                    From Time of the data to be exported in Unix epoch format.
                - to (int, optional):
                    End Time of the data to be exported in Unix epoch format.
                - assume_role (str, optional):
                    Assume role.
                - bucket (str):
                    Name of bucket to be used.
                - domain_key (str, optional):
                    Job domain key.
                - pool_name (str, optional):
                    For Presto only. Pool name to be used, if not
                    specified, default pool would be used.

        Returns:
             :class:`tdclient.models.Job`
        """
        return self._client.export_data(
            self._db_name, self._table_name, storage_type, kwargs
        )

    @property
    def estimated_storage_size_string(self) -> str:
        """a string represents estimated size of the table in human-readable format"""
        if self._estimated_storage_size is None:
            return "0.0 GB"
        elif self._estimated_storage_size <= 1024 * 1024:
            return "0.0 GB"
        elif self._estimated_storage_size <= 60 * 1024 * 1024:
            return "0.01 GB"
        elif self._estimated_storage_size <= 60 * 1024 * 1024 * 1024:
            return "%.1f GB" % (
                float(self._estimated_storage_size) / (1024 * 1024 * 1024)
            )
        else:
            return (
                f"{int(float(self._estimated_storage_size) / (1024 * 1024 * 1024))} GB"
            )

    def _update_database(self) -> None:
        self.database = self._client.database(self._db_name)
