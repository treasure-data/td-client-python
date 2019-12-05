#!/usr/bin/env python

import time

from tdclient.model import Model


class BulkImport(Model):
    """Bulk-import session on Treasure Data Service
    """

    STATUS_UPLOADING = "uploading"
    STATUS_PERFORMING = "performing"
    STATUS_READY = "ready"
    STATUS_COMMITTING = "committing"
    STATUS_COMMITTED = "committed"

    def __init__(self, client, **kwargs):
        super(BulkImport, self).__init__(client)
        self._feed(kwargs)

    def _feed(self, data=None):
        data = {} if data is None else data
        self._name = data["name"]
        self._database = data.get("database")
        self._table = data.get("table")
        self._status = data.get("status")
        self._upload_frozen = data.get("upload_frozen")
        self._job_id = data.get("job_id")
        self._valid_records = data.get("valid_records")
        self._error_records = data.get("error_records")
        self._valid_parts = data.get("valid_parts")
        self._error_parts = data.get("error_parts")

    def update(self):
        data = self._client.api.show_bulk_import(self.name)
        self._feed(data)

    @property
    def name(self):
        """A name of the bulk import session
        """
        return self._name

    @property
    def database(self):
        """A database name in a string which the bulk import session is working on
        """
        return self._database

    @property
    def table(self):
        """A table name in a string which the bulk import session is working on
        """
        return self._table

    @property
    def status(self):
        """The status of the bulk import session in a string
        """
        return self._status

    @property
    def job_id(self):
        """Job ID
        """
        return self._job_id

    @property
    def valid_records(self):
        """The number of valid records.
        """
        return self._valid_records

    @property
    def error_records(self):
        """The number of error records.
        """
        return self._error_records

    @property
    def valid_parts(self):
        """The number of valid parts.
        """
        return self._valid_parts

    @property
    def error_parts(self):
        """The number of error parts.
        """
        return self._error_parts

    @property
    def upload_frozen(self):
        """The number of upload frozen.
        """
        return self._upload_frozen

    def delete(self):
        """Delete bulk import
        """
        return self._client.delete_bulk_import(self.name)

    def freeze(self):
        """Freeze bulk import
        """
        response = self._client.freeze_bulk_import(self.name)
        self.update()
        return response

    def unfreeze(self):
        """Unfreeze bulk import
        """
        response = self._client.unfreeze_bulk_import(self.name)
        self.update()
        return response

    def perform(self, wait=False, wait_interval=5, wait_callback=None):
        """Perform bulk import

        Args:
            wait (bool, optional): Flag for wait bulk import job. Default `False`
            wait_interval (int, optional): wait interval in second. Default `5`.
            wait_callback (callable, optional): A callable to be called on every tick of
                wait interval.
        """
        self.update()
        if not self.upload_frozen:
            raise (
                RuntimeError('bulk import session "%s" is not frozen' % (self.name,))
            )
        job = self._client.perform_bulk_import(self.name)
        if wait:
            job.wait(wait_interval=wait_interval, wait_callback=wait_callback)
        self.update()
        return job

    def commit(self, wait=False, wait_interval=5, timeout=None):
        """Commit bulk import
        """
        response = self._client.commit_bulk_import(self.name)
        if wait:
            started_at = time.time()
            while self._status != self.STATUS_COMMITTED:
                if timeout is None or abs(time.time() - started_at) < timeout:
                    time.sleep(wait_interval)
                else:
                    raise RuntimeError("timeout")  # TODO: throw proper error
                self.update()
        else:
            self.update()
        return response

    def error_record_items(self):
        """Fetch error record rows.

        Yields:
            Error record
        """
        for record in self._client.bulk_import_error_records(self.name):
            yield record

    def upload_part(self, part_name, bytes_or_stream, size):
        """Upload a part to bulk import session

        Args:
            part_name (str): name of a part of the bulk import session
            bytes_or_stream (file-like): a file-like object contains the part
            size (int): the size of the part
        """
        response = self._client.bulk_import_upload_part(
            self.name, part_name, bytes_or_stream, size
        )
        self.update()
        return response

    def upload_file(self, part_name, fmt, file_like, **kwargs):
        """Upload a part to Bulk Import session, from an existing file on filesystem.

        Args:
            part_name (str): name of a part of the bulk import session
            fmt (str): format of data type (e.g. "msgpack", "json", "csv", "tsv")
            file_like (str or file-like): the name of a file, or a file-like object,
              containing the data
            **kwargs: extra arguments.

        There is more documentation on `fmt`, `file_like` and `**kwargs` at
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
        response = self._client.bulk_import_upload_file(
            self.name, part_name, fmt, file_like, **kwargs,
        )
        self.update()
        return response

    def delete_part(self, part_name):
        """Delete a part of a Bulk Import session

        Args:
            part_name (str): name of a part of the bulk import session
        Returns:
             True if succeeded.
        """
        response = self._client.bulk_import_delete_part(self.name, part_name)
        self.update()
        return response

    def list_parts(self):
        """Return the list of available parts uploaded through
        :func:`~BulkImportAPI.bulk_import_upload_part`.

        Returns:
            [str]: The list of bulk import part name.
        """
        response = self._client.list_bulk_import_parts(self.name)
        self.update()
        return response
