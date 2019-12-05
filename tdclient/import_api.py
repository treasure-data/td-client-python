#!/usr/bin/env python

import contextlib
import os

from .util import create_url


class ImportAPI:
    """Import data into Treasure Data Service.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def import_data(self, db, table, format, bytes_or_stream, size, unique_id=None):
        """Import data into Treasure Data Service

        This method expects data from a file-like object formatted with "msgpack.gz".

        Args:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns:
             float represents the elapsed time to import data
        """
        if unique_id is not None:
            path = create_url(
                "/v3/table/import_with_id/{db}/{table}/{unique_id}/{format}",
                db=db,
                table=table,
                unique_id=unique_id,
                format=format,
            )
        else:
            path = create_url(
                "/v3/table/import/{db}/{table}/{format}",
                db=db,
                table=table,
                format=format,
            )

        kwargs = {}
        with self.put(path, bytes_or_stream, size, **kwargs) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Import failed", res, body)
            js = self.checked_json(body, ["elapsed_time"])
            time = float(js["elapsed_time"])
            return time

    def import_file(self, db, table, format, file, unique_id=None, **kwargs):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").
        This method is a wrapper function to `import_data`.

        Args:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack", "json")
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns:
             float represents the elapsed time to import data
        """
        with contextlib.closing(self._prepare_file(file, format, **kwargs)) as fp:
            size = os.fstat(fp.fileno()).st_size
            return self.import_data(
                db, table, "msgpack.gz", fp, size, unique_id=unique_id
            )
