#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import time

from tdclient.model import Model

class Table(Model):
    """Database table on Treasure Data Service
    """

    def __init__(self, *args, **kwargs):
        super(Table, self).__init__(args[0])

        self.database = None
        self._db_name = args[1]
        self._table_name = args[2]

        if 3 < len(args):
            self._type = args[3]
            self._schema = args[4]
            self._count = args[5]
        else:
            self._type = kwargs.get("type")
            self._schema = kwargs.get("schema")
            self._count = kwargs.get("count")

        self._created_at = kwargs.get("created_at")
        self._updated_at = kwargs.get("updated_at")
        self._estimated_storage_size = kwargs.get("estimated_storage_size")
        self._last_import = kwargs.get("last_import")
        self._last_log_timestamp = kwargs.get("last_log_timestamp")
        self._expire_days = kwargs.get("expire_days")
        self._primary_key = kwargs.get("primary_key")
        self._primary_key_type = kwargs.get("primary_key_type")

    @property
    def type(self):
        """
        Returns: a string represents the type of the table
        """
        return self._type

    @property
    def db_name(self):
        """
        Returns: a string represents the name of the database
        """
        return self._db_name

    @property
    def table_name(self):
        """
        Returns: a string represents the name of the table
        """
        return self._table_name

    @property
    def schema(self):
        """
        TODO: add docstring
        """
        return self._schema

    @property
    def count(self):
        """
        TODO: add docstring
        """
        return self._count

    @property
    def estimated_storage_size(self):
        """
        TODO: add docstring
        """
        return self._estimated_storage_size

    @property
    def primary_key(self):
        """
        TODO: add docstring
        """
        return self._primary_key

    @property
    def primary_key_type(self):
        """
        TODO: add docstring
        """
        return self._primary_key_type

    @property
    def database_name(self):
        """
        Returns: a string represents the name of the database
        """
        return self._db_name

    @property
    def name(self):
        """
        Returns: a string represents the name of the table
        """
        return self._table_name

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

    @property
    def last_import(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._last_import

    @property
    def last_log_timestamp(self):
        """
        Returns: :class:`datetime.datetime`
        """
        return self._last_log_timestamp

    @property
    def expire_days(self):
        """
        Returns: an int represents the days until expiration
        """
        return self._expire_days

    @property
    def permission(self):
        """
        TODO: add docstring
        """
        if self.database is None:
            self._update_database()
        return self.database.permission

    @property
    def identifier(self):
        """
        Returns: a string identifier of the table
        """
        return "%s.%s" % (self._db_name, self._table_name)

    def delete(self):
        """
        Returns: a string represents the type of deleted table
        """
        return self._client.delete_table(self._db_name, self._table_name)

    def tail(self, count, to=None, _from=None):
        """
        TODO: add docstring
        """
        return self._client.tail(self._db_name, self._table_name, count, to, _from)

    def import_data(self, format, bytes_or_stream, size, unique_id=None):
        """Import data into Treasure Data Service

        Params:
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns: second in float represents elapsed time to import data
        """
        return self._client.import_data(self._db_name, self._table_name, format, bytes_or_stream, size, unique_id=unique_id)

    def import_file(self, format, file, unique_id=None):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

        Params:
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        return self._client.import_file(self._db_name, self._table_name, format, file, unique_id=unique_id)

    def export_data(self, storage_type, **kwargs):
        """
        TODO: add docstring
        """
        return self._client.export_data(self._db_name, self._table_name, storage_type, kwargs)

    @property
    def estimated_storage_size_string(self):
        """
        Returns: a string represents estimated size of the table in human-readable format
        """
        if self._estimated_storage_size <= 1024*1024:
            return "0.0 GB"
        elif self._estimated_storage_size <= 60*1024*1024:
            return "0.01 GB"
        elif self._estimated_storage_size <= 60*1024*1024*1024:
            return "%.1f GB" % (float(self._estimated_storage_size) / (1024*1024*1024))
        else:
            return "%d GB" % int(float(self._estimated_storage_size) / (1024*1024*1024))

    def _update_database(self):
        self.database = self._client.database(self._db_name)
