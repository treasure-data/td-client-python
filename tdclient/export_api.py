#!/usr/bin/env python

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    import urllib3

from tdclient.types import ExportParams
from tdclient.util import create_url


class ExportAPI:
    """Access to Export API.

    This class is inherited by :class:`tdclient.api.API`.
    """

    # Methods from API class
    def post(
        self, url: str, params: dict[str, Any] | None = None
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def raise_error(
        self, msg: str, res: urllib3.BaseHTTPResponse, body: bytes
    ) -> None: ...
    def checked_json(self, body: bytes, required: list[str]) -> dict[str, Any]: ...

    def export_data(
        self, db: str, table: str, storage_type: str, params: ExportParams | None = None
    ) -> str:
        """Creates a job to export the contents from the specified database and table
        names.

        Args:
            db (str): Target database name.
            table (str): Target table name.
            storage_type (str): Name of storage type. e.g. "s3"
            params (dict): Extra parameters. Assuming the following keys:

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
                     From Time of the data to be exported in Unix epoch
                     format.
                - to (int, optional):
                     End Time of the data to be exported in Unix epoch
                     format.
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
            str: Job ID.
        """
        post_params = {} if params is None else dict(params)
        post_params["storage_type"] = storage_type
        with self.post(
            create_url("/v3/export/run/{db}/{table}", db=db, table=table), post_params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Export failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
