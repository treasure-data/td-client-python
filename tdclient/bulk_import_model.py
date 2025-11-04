#!/usr/bin/env python

import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

from tdclient.model import Model
from tdclient.types import BytesOrStream, DataFormat, FileLike

if TYPE_CHECKING:
    from tdclient.client import Client
    from tdclient.job_model import Job


class BulkImport(Model):
    """Bulk-import session on Treasure Data Service"""

    STATUS_UPLOADING = "uploading"
    STATUS_PERFORMING = "performing"
    STATUS_READY = "ready"
    STATUS_COMMITTING = "committing"
    STATUS_COMMITTED = "committed"

    def __init__(self, client: "Client", **kwargs: Any) -> None:
        super().__init__(client)
        self._feed(kwargs)

    def _feed(self, data: dict[str, Any] | None = None) -> None:
        data = {} if data is None else data
        self._name: str = data["name"]
        self._database: str | None = data.get("database")
        self._table: str | None = data.get("table")
        self._status: str | None = data.get("status")
        self._upload_frozen: bool | None = data.get("upload_frozen")
        self._job_id: str | None = data.get("job_id")
        self._valid_records: int | None = data.get("valid_records")
        self._error_records: int | None = data.get("error_records")
        self._valid_parts: int | None = data.get("valid_parts")
        self._error_parts: int | None = data.get("error_parts")

    def update(self) -> None:
        data = self._client.api.show_bulk_import(self.name)
        self._feed(data)

    @property
    def name(self) -> str:
        """A name of the bulk import session"""
        return self._name

    @property
    def database(self) -> str | None:
        """A database name in a string which the bulk import session is working on"""
        return self._database

    @property
    def table(self) -> str | None:
        """A table name in a string which the bulk import session is working on"""
        return self._table

    @property
    def status(self) -> str | None:
        """The status of the bulk import session in a string"""
        return self._status

    @property
    def job_id(self) -> str | None:
        """Job ID"""
        return self._job_id

    @property
    def valid_records(self) -> int | None:
        """The number of valid records."""
        return self._valid_records

    @property
    def error_records(self) -> int | None:
        """The number of error records."""
        return self._error_records

    @property
    def valid_parts(self) -> int | None:
        """The number of valid parts."""
        return self._valid_parts

    @property
    def error_parts(self) -> int | None:
        """The number of error parts."""
        return self._error_parts

    @property
    def upload_frozen(self) -> bool | None:
        """The number of upload frozen."""
        return self._upload_frozen

    def delete(self) -> bool:
        """Delete bulk import"""
        return self._client.delete_bulk_import(self.name)

    def freeze(self) -> bool:
        """Freeze bulk import"""
        response = self._client.freeze_bulk_import(self.name)
        self.update()
        return response

    def unfreeze(self) -> bool:
        """Unfreeze bulk import"""
        response = self._client.unfreeze_bulk_import(self.name)
        self.update()
        return response

    def perform(
        self,
        wait: bool = False,
        wait_interval: int = 5,
        wait_callback: Callable[["Job"], None] | None = None,
        timeout: float | None = None,
    ) -> "Job":
        """Perform bulk import

        Args:
            wait (bool, optional): Flag for wait bulk import job. Default `False`
            wait_interval (int, optional): wait interval in second. Default `5`.
            wait_callback (callable, optional): A callable to be called on every tick of
                wait interval.
            timeout (int, optional): Timeout in seconds. No timeout by default.
        """
        self.update()
        if not self.upload_frozen:
            raise (RuntimeError(f'bulk import session "{self.name}" is not frozen'))
        job = self._client.perform_bulk_import(self.name)
        if wait:
            job.wait(
                timeout=timeout,
                wait_interval=wait_interval,
                wait_callback=wait_callback,
            )
        self.update()
        return job

    def commit(
        self, wait: bool = False, wait_interval: int = 5, timeout: float | None = None
    ) -> bool:
        """Commit bulk import"""
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

    def error_record_items(self) -> Iterator[dict[str, Any]]:
        """Fetch error record rows.

        Yields:
            Error record
        """
        yield from self._client.bulk_import_error_records(self.name)

    def upload_part(
        self, part_name: str, bytes_or_stream: BytesOrStream, size: int
    ) -> None:
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

    def upload_file(
        self, part_name: str, fmt: DataFormat, file_like: FileLike, **kwargs: Any
    ) -> None:
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
          be used to parse individual columns, for instance ``{"col1", int}``.

        The default behaviour is ``"guess"``, which makes a best-effort to decide
        the column datatype. See `file import parameters`_ for more details.

        .. _`file import parameters`:
           https://tdclient.readthedocs.io/en/latest/file_import_parameters.html
        """
        response = self._client.bulk_import_upload_file(
            self.name,
            part_name,
            fmt,
            file_like,
            **kwargs,
        )
        self.update()
        return response

    def delete_part(self, part_name: str) -> bool:
        """Delete a part of a Bulk Import session

        Args:
            part_name (str): name of a part of the bulk import session
        Returns:
             True if succeeded.
        """
        response = self._client.bulk_import_delete_part(self.name, part_name)
        self.update()
        return response

    def list_parts(self) -> list[str]:
        """Return the list of available parts uploaded through
        :func:`~BulkImportAPI.bulk_import_upload_part`.

        Returns:
            [str]: The list of bulk import part name.
        """
        response = self._client.list_bulk_import_parts(self.name)
        self.update()
        return response
