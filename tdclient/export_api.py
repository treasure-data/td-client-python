#!/usr/bin/env python

from .util import create_url


class ExportAPI:
    """Access to Export API.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def export_data(self, db, table, storage_type, params=None):
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
        params = {} if params is None else params
        params["storage_type"] = storage_type
        with self.post(
            create_url("/v3/export/run/{db}/{table}", db=db, table=table), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Export failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
