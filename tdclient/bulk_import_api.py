#!/usr/bin/env python

import collections
import contextlib
import gzip
import io
import os

import msgpack

from .util import create_url


class BulkImportAPI:
    """Enable bulk importing of data to the targeted database and table.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def create_bulk_import(self, name, db, table, params=None):
        """Enable bulk importing of data to the targeted database and table and stores
        it in the default resource pool. Default expiration for bulk import is 30days.

        Args:
            name (str): Name of the bulk import.
            db (str): Name of target database.
            table (str): Name of target table.
            params (dict, optional): Extra parameters.

        Returns:
             True if succeeded
        """
        params = {} if params is None else params
        with self.post(
            create_url(
                "/v3/bulk_import/create/{name}/{db}/{table}",
                name=name,
                db=db,
                table=table,
            ),
            params,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create bulk import failed", res, body)
            return True

    def delete_bulk_import(self, name, params=None):
        """Delete the imported information with the specified name

        Args:
            name (str): Name of bulk import.
            params (dict, optional): Extra parameters.
        Returns:
             True if succeeded
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/bulk_import/delete/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete bulk import failed", res, body)
            return True

    def show_bulk_import(self, name):
        """Show the details of the bulk import with the specified name

        Args:
            name (str): Name of bulk import.
        Returns:
            dict: Detailed information of the bulk import.
        """
        with self.get(create_url("/v3/bulk_import/show/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show bulk import failed", res, body)
            js = self.checked_json(body, ["status"])
            return js

    def list_bulk_imports(self, params=None):
        """Return the list of available bulk imports
        Args:
            params (dict, optional): Extra parameters.
        Returns:
            [dict]:  The list of available bulk import details.
        """
        params = {} if params is None else params
        with self.get("/v3/bulk_import/list", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List bulk imports failed", res, body)
            js = self.checked_json(body, ["bulk_imports"])
            return js["bulk_imports"]

    def list_bulk_import_parts(self, name, params=None):
        """Return the list of available parts uploaded through
        :func:`~BulkImportAPI.bulk_import_upload_part`.

        Args:
            name (str): Name of bulk import.
            params (dict, optional): Extra parameteres.
        Returns:
            [str]: The list of bulk import part name.
        """
        params = {} if params is None else params
        with self.get(
            create_url("/v3/bulk_import/list_parts/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List bulk import parts failed", res, body)
            js = self.checked_json(body, ["parts"])
            return js["parts"]

    @staticmethod
    def validate_part_name(part_name):
        """Make sure the part_name is valid

        Args:
            part_name (str): The part name the user is trying to use
        """
        # Check for duplicate periods
        d = collections.defaultdict(int)
        for char in part_name:
            d[char] += 1

        if 1 < d["."]:
            raise ValueError(
                "part names cannot contain multiple periods: %s" % (repr(part_name))
            )

        if 0 < part_name.find("/"):
            raise ValueError("part name must not contain '/': %s" % (repr(part_name)))

    def bulk_import_upload_part(self, name, part_name, stream, size):
        """Upload bulk import having the specified name and part in the path.

        Args:
            name (str): Bulk import name.
            part_name (str): Bulk import part name.
            stream (str or file-like): Byte string or file-like object contains the data
            size (int): The length of the data.
        """
        self.validate_part_name(part_name)
        with self.put(
            create_url(
                "/v3/bulk_import/upload_part/{name}/{part_name}",
                name=name,
                part_name=part_name,
            ),
            stream,
            size,
        ) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Upload a part failed", res, body)

    def bulk_import_upload_file(self, name, part_name, format, file, **kwargs):
        """Upload a file with bulk import having the specified name.

        Args:
            name (str): Bulk import name.
            part_name (str): Bulk import part name.
            format (str): Format name. {msgpack, json, csv, tsv}
            file (str or file-like): the name of a file, or a file-like object,
              containing the data
            **kwargs: Extra arguments.

        There is more documentation on `format`, `file` and `**kwargs` at
        `file import parameters`_.

        In particular, for "csv" and "tsv" data, you can change how data columns
        are parsed using the ``dtypes`` and ``converters`` arguments.

        * ``dtypes`` is a dictionary used to specify a datatype for individual
          columns, for instance ``{"col1": "int"}``. The available datatypes
          are ``"bool"``, ``"float"``, ``"int"``, ``"str"`` and ``"guess"``.
          If a column is also mentioned in ``converters``, then the function
          will be used, NOT the datatype.

        * ``converters`` is a dictionary used to specify a function that will
          be used to parse individual columns, for instace ``{"col1", int}``.

        The default behaviour is ``"guess"``, which makes a best-effort to decide
        the column datatype. See `file import parameters`_ for more details.
        
        .. _`file import parameters`:
           https://tdclient.readthedocs.io/en/latest/file_import_parameters.html
        """
        self.validate_part_name(part_name)
        with contextlib.closing(self._prepare_file(file, format, **kwargs)) as fp:
            size = os.fstat(fp.fileno()).st_size
            return self.bulk_import_upload_part(name, part_name, fp, size)

    def bulk_import_delete_part(self, name, part_name, params=None):
        """Delete the imported information with the specified name.

        Args:
            name (str): Bulk import name.
            part_name (str): Bulk import part name.
            params (dict, optional): Extra parameters.
        Returns:
             True if succeeded.
        """
        self.validate_part_name(part_name)
        params = {} if params is None else params
        with self.post(
            create_url(
                "/v3/bulk_import/delete_part/{name}/{part_name}",
                name=name,
                part_name=part_name,
            ),
            params,
        ) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Delete a part failed", res, body)
            return True

    def freeze_bulk_import(self, name, params=None):
        """Freeze the bulk import with the specified name.

        Args:
            name (str): Bulk import name.
            params (dict, optional): Extra parameters.
        Returns:
             True if succeeded.
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/bulk_import/freeze/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Freeze bulk import failed", res, body)
            return True

    def unfreeze_bulk_import(self, name, params=None):
        """Unfreeze bulk_import with the specified name.

        Args:
            name (str): Bulk import name.
            params (dict, optional): Extra parameters.
        Returns:
             True if succeeded.
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/bulk_import/unfreeze/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Unfreeze bulk import failed", res, body)
            return True

    def perform_bulk_import(self, name, params=None):
        """Execute a job to perform bulk import with the indicated priority using the
        resource pool if indicated, else it will use the account's default.

        Args:
            name (str): Bulk import name.
            params (dict, optional): Extra parameters.
        Returns:
            str: Job ID
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/bulk_import/perform/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Perform bulk import failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def commit_bulk_import(self, name, params=None):
        """Commit the bulk import information having the specified name.

        Args:
            name (str): Bulk import name.
            params (dict, optional): Extra parameters.
        Returns:
            True if succeeded.
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/bulk_import/commit/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Commit bulk import failed", res, body)
            return True

    def bulk_import_error_records(self, name, params=None):
        """List the records that have errors under the specified bulk import name.

        Args:
            name (str): Bulk import name.
            params (dict, optional): Extra parameters.
        Yields:
            Row of the data
        """
        params = {} if params is None else params
        with self.get(
            create_url("/v3/bulk_import/error_records/{name}", name=name), params
        ) as res:
            code = res.status
            if code != 200:
                body = res.read()
                self.raise_error("Failed to get bulk import error records", res, body)

            body = io.BytesIO(res.read())
            decompressor = gzip.GzipFile(fileobj=body)

            unpacker = msgpack.Unpacker(decompressor, raw=False)
            for row in unpacker:
                yield row
