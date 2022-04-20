#!/usr/bin/env python

import datetime
import json
from collections.abc import Iterator
from typing import Any, Literal, cast

from tdclient import api, models
from tdclient.types import (
    BulkImportParams,
    BytesOrStream,
    DataFormat,
    ExportParams,
    FileLike,
    Priority,
    ResultFormat,
    ResultParams,
    ScheduleParams,
)


class Client:
    """API Client for Treasure Data Service"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._api = api.API(*args, **kwargs)

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        self.close()

    @property
    def api(self) -> api.API:
        """
        an instance of :class:`tdclient.api.API`
        """
        return self._api

    @property
    def apikey(self) -> str | None:
        """
        API key string.
        """
        return self._api.apikey

    def server_status(self) -> str:
        """
        Returns:
             a string represents current server status.
        """
        return self.api.server_status()

    def create_database(self, db_name: str, **kwargs: Any) -> bool:
        """
        Args:
            db_name (str): name of a database to create

        Returns:
             `True` if success
        """
        return self.api.create_database(db_name, **kwargs)

    def delete_database(self, db_name: str) -> bool:
        """
        Args:
            db_name (str): name of database to delete

        Returns:
             `True` if success
        """
        return self.api.delete_database(db_name)

    def databases(self) -> list[models.Database]:
        """
        Returns:
            a list of :class:`tdclient.models.Database`
        """
        databases = self.api.list_databases()
        return [
            models.Database(self, db_name, **kwargs)
            for (db_name, kwargs) in databases.items()
        ]

    def database(self, db_name: str) -> models.Database:
        """
        Args:
            db_name (str): name of a database

        Returns:
             :class:`tdclient.models.Database`
        """
        databases = self.api.list_databases()
        for name, kwargs in databases.items():
            if name == db_name:
                return models.Database(self, name, **kwargs)
        raise api.NotFoundError(f"Database '{db_name}' does not exist")

    def create_log_table(self, db_name: str, table_name: str) -> bool:
        """
        Args:
            db_name (str): name of a database
            table_name (str): name of a table to create

        Returns:
             `True` if success
        """
        return self.api.create_log_table(db_name, table_name)

    def swap_table(self, db_name: str, table_name1: str, table_name2: str) -> bool:
        """
        Args:
            db_name (str): name of a database
            table_name1 (str): original table name
            table_name2 (str): table name you want to rename to

        Returns:
            `True` if success
        """
        return self.api.swap_table(db_name, table_name1, table_name2)

    def update_schema(
        self, db_name: str, table_name: str, schema: list[list[str]]
    ) -> bool:
        """Updates the schema of a table

        Args:
            db_name (str): name of a database
            table_name (str): name of a table
            schema (list): a dictionary object represents the schema definition (will
                be converted to JSON)
                e.g.

                .. code-block:: python

                    [
                        ["member_id", # column name
                         "string", # data type
                         "mem_id", # alias of the column name
                        ],
                        ["row_index", "long", "row_ind"],
                        ...
                    ]

        Returns:
             `True` if success
        """
        return self.api.update_schema(db_name, table_name, json.dumps(schema))

    def update_expire(self, db_name: str, table_name: str, expire_days: int) -> bool:
        """Set expiration date to a table

        Args:
            db_name (str): name of a database
            table_name (str): name of a table
            epire_days (int): expiration date in days from today

        Returns:
             `True` if success
        """
        return self.api.update_expire(db_name, table_name, expire_days)

    def delete_table(self, db_name: str, table_name: str) -> str:
        """Delete a table

        Args:
            db_name (str): name of a database
            table_name (str): name of a table

        Returns:
             a string represents the type of deleted table
        """
        return self.api.delete_table(db_name, table_name)

    def tables(self, db_name: str) -> list[models.Table]:
        """List existing tables

        Args:
            db_name (str): name of a database

        Returns:
             a list of :class:`tdclient.models.Table`
        """
        m = self.api.list_tables(db_name)
        return [
            models.Table(self, db_name, table_name, **kwargs)
            for (table_name, kwargs) in m.items()
        ]

    def table(self, db_name: str, table_name: str) -> models.Table:
        """
        Args:
            db_name (str): name of a database
            table_name (str): name of a table

        Returns:
            :class:`tdclient.models.Table`

        Raises:
            tdclient.api.NotFoundError: if the table doesn't exist
        """
        tables = self.tables(db_name)
        for table in tables:
            if table.table_name == table_name:
                return table
        raise api.NotFoundError(f"Table '{db_name}.{table_name}' does not exist")

    def tail(
        self,
        db_name: str,
        table_name: str,
        count: int,
        to: None = None,
        _from: None = None,
        block: None = None,
    ) -> list[dict[str, Any]]:
        """Get the contents of the table in reverse order based on the registered time
        (last data first).

        Args:
            db_name (str): Target database name.
            table_name (str): Target table name.
            count (int): Number for record to show up from the end.
            to: Deprecated parameter.
            _from: Deprecated parameter.
            block: Deprecated parameter.

        Returns:
            [dict]: Contents of the table.
        """
        return self.api.tail(db_name, table_name, count, to, _from, block)

    def change_database(self, db_name: str, table_name: str, new_db_name: str) -> bool:
        """Move a target table from it's original database to new destination database.

        Args:
            db_name (str): Target database name.
            table_name (str): Target table name.
            new_db_name (str): Destination database name to be moved.

        Returns:
            bool: `True` if succeeded.
        """
        return self.api.change_database(db_name, table_name, new_db_name)

    def query(
        self,
        db_name: str,
        q: str,
        result_url: str | None = None,
        priority: Priority | None = None,
        retry_limit: int | None = None,
        type: str = "hive",
        **kwargs: Any,
    ) -> models.Job:
        """Run a query on specified database table.

        Args:
            db_name (str): name of a database
            q (str): a query string
            result_url (str): result output URL. e.g.,
                ``postgresql://<username>:<password>@<hostname>:<port>/<database>/<table>``
            priority (int or str): priority (e.g. "NORMAL", "HIGH", etc.)
            retry_limit (int): retry limit
            type (str): name of a query engine

        Returns:
            :class:`tdclient.models.Job`

        Raises:
            ValueError: if unknown query type has been specified
        """
        # for compatibility, assume type is hive unless specifically specified
        if type not in ["hive", "pig", "impala", "presto", "trino"]:
            raise ValueError(f"The specified query type is not supported: {type}")
        # Cast type to expected literal since we've validated it
        query_type = cast(Literal["hive", "presto", "trino", "bulkload"], type)
        job_id = self.api.query(
            q,
            type=query_type,
            db=db_name,
            result_url=result_url,
            priority=priority,
            retry_limit=retry_limit,
            **kwargs,
        )
        return models.Job(self, job_id, type, q)

    def jobs(
        self,
        _from: int | None = None,
        to: int | None = None,
        status: str | None = None,
        conditions: dict[str, Any] | None = None,
    ) -> list[models.Job]:
        """List jobs

        Args:
            _from (int, optional): Gets the Job from the nth index in the list. Default: 0.
            to (int, optional): Gets the Job up to the nth index in the list.
                By default, the first 20 jobs in the list are displayed
            status (str, optional): Filter by given status. {"queued", "running", "success", "error"}
            conditions (dict[str, Any], optional): Condition for ``TIMESTAMPDIFF()`` to search for slow queries.
                Avoid using this parameter as it can be dangerous.

        Returns:
             a list of :class:`tdclient.models.Job`
        """
        results = self.api.list_jobs(_from or 0, to, status, conditions)

        return [job_from_dict(self, d) for d in results]

    def job(self, job_id: str | int) -> models.Job:
        """Get a job from `job_id`

        Args:
            job_id (str): job id

        Returns:
             :class:`tdclient.models.Job`
        """
        d = self.api.show_job(str(job_id))
        return job_from_dict(self, d, job_id=job_id)

    def job_status(self, job_id: str | int) -> str:
        """
        Args:
            job_id (str): job id

        Returns:
             a string represents the status of the job ("success", "error", "killed", "queued", "running")
        """
        return self.api.job_status(str(job_id))

    def job_result(self, job_id: str | int) -> list[Any]:
        """
        Args:
            job_id (str): job id

        Returns:
             a list of each rows in result set
        """
        return self.api.job_result(str(job_id))

    def job_result_each(self, job_id: str | int) -> Iterator[dict[str, Any]]:
        """
        Args:
            job_id (str): job id

        Returns:
             an iterator of result set
        """
        yield from self.api.job_result_each(str(job_id))

    def job_result_format(
        self, job_id: str | int, format: ResultFormat, header: bool = False
    ) -> list[Any]:
        """
        Args:
            job_id (str): job id
            format (str): output format of result set

        Returns:
             a list of each rows in result set
        """
        return self.api.job_result_format(str(job_id), format, header=header)

    def job_result_format_each(
        self,
        job_id: str | int,
        format: ResultFormat,
        header: bool = False,
        store_tmpfile: bool = False,
        num_threads: int = 4,
    ) -> Iterator[dict[str, Any]]:
        """
        Args:
            job_id (str): job id
            format (str): output format of result set
            header (bool, optional): include header in the result set. Default: False
            store_tmpfile (bool, optional): store result to a temporary file.
                Works only when fmt is "msgpack". Default is False.
            num_threads (int, optional): number of threads to download result.
                Works only when store_tmpfile is True. Default is 4.


        Returns:
             an iterator of rows in result set
        """
        yield from self.api.job_result_format_each(
            str(job_id),
            format,
            header=header,
            store_tmpfile=store_tmpfile,
            num_threads=num_threads,
        )

    def download_job_result(
        self, job_id: str | int, path: str, num_threads: int = 4
    ) -> bool:
        """Save the job result into a msgpack.gz file.
        Args:
            job_id (str): job id
            path (str): path to save the result
            num_threads (int, optional): number of threads to download the result.
                Default: 4

        Returns:
             `True` if success
        """
        return self.api.download_job_result(str(job_id), path, num_threads=num_threads)

    def kill(self, job_id: str | int) -> str | None:
        """
        Args:
            job_id (str): job id

        Returns:
             a string represents the status of killed job ("queued", "running")
        """
        return self.api.kill(str(job_id))

    def export_data(
        self,
        db_name: str,
        table_name: str,
        storage_type: str,
        params: ExportParams | None = None,
    ) -> models.Job:
        """Export data from Treasure Data Service

        Args:
            db_name (str): name of a database
            table_name (str): name of a table
            storage_type (str): type of the storage
            params (dict): optional parameters. Assuming the following keys:

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
                - assume_role (str, optional): Assume role.
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
        params = {} if params is None else params
        job_id = self.api.export_data(db_name, table_name, storage_type, params)
        return models.Job(self, job_id, "export", None)

    def create_bulk_import(
        self,
        name: str,
        database: str,
        table: str,
        params: BulkImportParams | None = None,
    ) -> models.BulkImport:
        """Create new bulk import session

        Args:
            name (str): name of new bulk import session
            database (str): name of a database
            table (str): name of a table

        Returns:
             :class:`tdclient.models.BulkImport`
        """
        params = {} if params is None else params
        self.api.create_bulk_import(name, database, table, params)
        return models.BulkImport(self, name=name, database=database, table=table)

    def delete_bulk_import(self, name: str) -> bool:
        """Delete a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             `True` if success
        """
        return self.api.delete_bulk_import(name)

    def freeze_bulk_import(self, name: str) -> bool:
        """Freeze a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             `True` if success
        """
        return self.api.freeze_bulk_import(name)

    def unfreeze_bulk_import(self, name: str) -> bool:
        """Unfreeze a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             `True` if success
        """
        return self.api.unfreeze_bulk_import(name)

    def perform_bulk_import(self, name: str) -> models.Job:
        """Perform a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             :class:`tdclient.models.Job`
        """
        job_id = self.api.perform_bulk_import(name)
        return models.Job(self, job_id, "bulk_import", None)

    def commit_bulk_import(self, name: str) -> bool:
        """Commit a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             `True` if success
        """
        return self.api.commit_bulk_import(name)

    def bulk_import_error_records(self, name: str) -> Iterator[dict[str, Any]]:
        """
        Args:
            name (str): name of a bulk import session

        Returns:
             an iterator of error records
        """
        yield from self.api.bulk_import_error_records(name)

    def bulk_import(self, name: str) -> models.BulkImport:
        """Get a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             :class:`tdclient.models.BulkImport`
        """
        data = self.api.show_bulk_import(name)
        return models.BulkImport(self, **data)

    def bulk_imports(self) -> list[models.BulkImport]:
        """List bulk import sessions

        Returns:
             a list of :class:`tdclient.models.BulkImport`
        """
        return [
            models.BulkImport(self, **data) for data in self.api.list_bulk_imports()
        ]

    def bulk_import_upload_part(
        self, name: str, part_name: str, bytes_or_stream: BytesOrStream, size: int
    ) -> None:
        """Upload a part to a bulk import session

        Args:
            name (str): name of a bulk import session
            part_name (str): name of a part of the bulk import session
            bytes_or_stream (file-like): a file-like object contains the part
            size (int): the size of the part
        """
        return self.api.bulk_import_upload_part(name, part_name, bytes_or_stream, size)

    def bulk_import_upload_file(
        self,
        name: str,
        part_name: str,
        format: DataFormat,
        file: FileLike,
        **kwargs: Any,
    ) -> None:
        """Upload a part to Bulk Import session, from an existing file on filesystem.

        Args:
            name (str): name of a bulk import session
            part_name (str): name of a part of the bulk import session
            format (str): format of data type (e.g. "msgpack", "json", "csv", "tsv")
            file (str or file-like): the name of a file, or a file-like object,
              containing the data
            **kwargs: extra arguments.

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
          be used to parse individual columns, for instance ``{"col1", int}``.

        The default behaviour is ``"guess"``, which makes a best-effort to decide
        the column datatype. See `file import parameters`_ for more details.

        .. _`file import parameters`:
           https://tdclient.readthedocs.io/en/latest/file_import_parameters.html
        """
        return self.api.bulk_import_upload_file(name, part_name, format, file, **kwargs)

    def bulk_import_delete_part(self, name: str, part_name: str) -> bool:
        """Delete a part from a bulk import session

        Args:
            name (str): name of a bulk import session
            part_name (str): name of a part of the bulk import session

        Returns:
             `True` if success
        """
        return self.api.bulk_import_delete_part(name, part_name)

    def list_bulk_import_parts(self, name: str) -> list[str]:
        """List parts of a bulk import session

        Args:
            name (str): name of a bulk import session

        Returns:
             a list of string represents the name of parts
        """
        return self.api.list_bulk_import_parts(name)

    def create_schedule(
        self, name: str, params: ScheduleParams | None = None
    ) -> datetime.datetime | None:
        """Create a new scheduled query with the specified name.

        Args:
            name (str): Scheduled query name.
            params (dict, optional): Extra parameters.

                - type (str):
                     Query type. {"presto", "hive"}. Default: "hive"
                - database (str):
                     Target database name.
                - timezone (str):
                     Scheduled query's timezone. e.g. "UTC"
                     For details, see also: https://gist.github.com/frsyuki/4533752
                - cron (str, optional):
                     Schedule of the query.
                     {``"@daily"``, ``"@hourly"``, ``"10 * * * *"`` (custom cron)}
                     See also: https://docs.treasuredata.com/display/PD/Scheduling+Jobs+Using+TD+Console
                - delay (int, optional):
                     A delay ensures all buffered events are imported
                     before running the query. Default: 0
                - query (str):
                     Is a language used to retrieve, insert, update and modify
                     data. See also: https://docs.treasuredata.com/display/PD/SQL+Examples+of+Scheduled+Queries
                - priority (int, optional):
                     Priority of the query.
                     Range is from -2 (very low) to 2 (very high). Default: 0
                - retry_limit (int, optional):
                     Automatic retry count. Default: 0
                - engine_version (str, optional):
                     Engine version to be used. If none is
                     specified, the account's default engine version would be set.
                     {"stable", "experimental"}
                - pool_name (str, optional):
                     For Presto only. Pool name to be used, if not
                     specified, default pool would be used.
                - result (str, optional):
                     Location where to store the result of the query.
                     e.g. 'tableau://user:password@host.com:1234/datasource'
        Returns:
            :class:`datetime.datetime`: Start date time.
        """
        params = {} if params is None else params
        if "cron" not in params:
            raise ValueError("'cron' option is required")
        if "query" not in params:
            raise ValueError("'query' option is required")
        return self.api.create_schedule(name, params)

    def delete_schedule(self, name: str) -> tuple[str, str]:
        """Delete the scheduled query with the specified name.

        Args:
            name (str): Target scheduled query name.
        Returns:
            (str, str): Tuple of cron and query.
        """
        return self.api.delete_schedule(name)

    def schedules(self) -> list[models.Schedule]:
        """Get the list of all the scheduled queries.

        Returns:
            [:class:`tdclient.models.Schedule`]
        """
        result = self.api.list_schedules()
        return [models.Schedule(self, **m) for m in result]

    def update_schedule(self, name: str, params: ScheduleParams | None = None) -> None:
        """Update the scheduled query.

        Args:
            name (str): Target scheduled query name.
            params (dict): Extra parameters.

                - type (str):
                     Query type. {"presto", "hive"}. Default: "hive"
                - database (str):
                     Target database name.
                - timezone (str):
                     Scheduled query's timezone. e.g. "UTC"
                     For details, see also: https://gist.github.com/frsyuki/4533752
                - cron (str, optional):
                     Schedule of the query.
                     {``"@daily"``, ``"@hourly"``, ``"10 * * * *"`` (custom cron)}
                     See also: https://docs.treasuredata.com/display/PD/Scheduling+Jobs+Using+TD+Console
                - delay (int, optional):
                     A delay ensures all buffered events are imported
                     before running the query. Default: 0
                - query (str):
                     Is a language used to retrieve, insert, update and modify
                     data. See also: https://docs.treasuredata.com/display/PD/SQL+Examples+of+Scheduled+Queries
                - priority (int, optional):
                     Priority of the query.
                     Range is from -2 (very low) to 2 (very high). Default: 0
                - retry_limit (int, optional):
                     Automatic retry count. Default: 0
                - engine_version (str, optional):
                     Engine version to be used. If none is
                     specified, the account's default engine version would be set.
                     {"stable", "experimental"}
                - pool_name (str, optional):
                     For Presto only. Pool name to be used, if not
                     specified, default pool would be used.
                - result (str, optional):
                     Location where to store the result of the query.
                     e.g. 'tableau://user:password@host.com:1234/datasource'
        """
        params = {} if params is None else params
        self.api.update_schedule(name, params)

    def history(
        self, name: str, _from: int | None = None, to: int | None = None
    ) -> list[models.ScheduledJob]:
        """Get the history details of the saved query for the past 90days.

        Args:
            name (str): Target name of the scheduled query.
            _from (int, optional): Indicates from which nth record in the run history
                would be fetched.
                Default: 0.
                Note: Count starts from zero. This means that the first record in the
                list has a count of zero.
            to (int, optional): Indicates up to which nth record in the run history
                would be fetched.
                Default: 20
        Returns:
            [:class:`tdclient.models.ScheduledJob`]
        """
        result = self.api.history(name, _from or 0, to)

        def scheduled_job(m):
            (
                scheduled_at,
                job_id,
                type,
                status,
                query,
                start_at,
                end_at,
                result_url,
                priority,
                database,
            ) = m
            job_param = {
                "url": None,
                "debug": None,
                "start_at": start_at,
                "end_at": end_at,
                "cpu_time": None,
                "result_size": None,
                "result": None,
                "result_url": result_url,
                "hive_result_schema": None,
                "priority": priority,
                "retry_limit": None,
                "org_name": None,
                "database": database,
            }
            return models.ScheduledJob(
                self, scheduled_at, job_id, type, query, **job_param
            )

        return [scheduled_job(m) for m in result]

    def run_schedule(self, name: str, time: int, num: int) -> list[models.ScheduledJob]:
        """Execute the specified query.

        Args:
            name (str): Target scheduled query name.
            time (int): Time in Unix epoch format that would be set as TD_SCHEDULED_TIME
            num (int): Indicates how many times the query will be executed.
                Value should be 9 or less.
        Returns:
            [:class:`tdclient.models.ScheduledJob`]
        """
        results = self.api.run_schedule(name, time, num)

        def scheduled_job(m):
            job_id, type, scheduled_at = m
            return models.ScheduledJob(self, scheduled_at, job_id, type, None)

        return [scheduled_job(m) for m in results]

    def import_data(
        self,
        db_name: str,
        table_name: str,
        format: DataFormat,
        bytes_or_stream: BytesOrStream,
        size: int,
        unique_id: str | None = None,
    ) -> float:
        """Import data into Treasure Data Service

        Args:
            db_name (str): name of a database
            table_name (str): name of a table
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns:
             second in float represents elapsed time to import data
        """
        return self.api.import_data(
            db_name, table_name, format, bytes_or_stream, size, unique_id=unique_id
        )

    def import_file(
        self,
        db_name: str,
        table_name: str,
        format: DataFormat,
        file: FileLike,
        unique_id: str | None = None,
    ) -> float:
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

        Args:
            db_name (str): name of a database
            table_name (str): name of a table
            format (str): format of data type (e.g. "msgpack", "json")
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns:
             float represents the elapsed time to import data
        """
        return self.api.import_file(
            db_name, table_name, format, file, unique_id=unique_id
        )

    def results(self) -> list[models.Result]:
        """Get the list of all the available authentications.

        Returns:
             a list of :class:`tdclient.models.Result`
        """
        results = self.api.list_result()

        def result(m):
            name, url, organizations = m
            return models.Result(self, name, url, organizations)

        return [result(m) for m in results]

    def create_result(
        self, name: str, url: str, params: ResultParams | None = None
    ) -> bool:
        """Create a new authentication with the specified name.

        Args:
            name (str): Authentication name.
            url (str):  Url of the authentication to be created. e.g. "ftp://test.com/"
            params (dict, optional): Extra parameters.
        Returns:
            bool: True if succeeded.
        """
        params = {} if params is None else params
        return self.api.create_result(name, url, params)

    def delete_result(self, name: str) -> bool:
        """Delete the authentication having the specified name.

        Args:
            name (str): Authentication name.
        Returns:
            bool: True if succeeded.
        """
        return self.api.delete_result(name)

    def users(self):
        """List users

        Returns:
             a list of :class:`tdclient.models.User`
        """
        results = self.api.list_users()

        def user(m):
            name, org, roles, email = m
            return models.User(self, name, org, roles, email)

        return [user(m) for m in results]

    def add_user(self, name: str, org: str, email: str, password: str) -> bool:
        """Add a new user

        Args:
            name (str): name of the user
            org (str): organization
            email: (str): e-mail address
            password (str): password

        Returns:
             `True` if success
        """
        return self.api.add_user(name, org, email, password)

    def remove_user(self, name: str) -> bool:
        """Remove a user

        Args:
            name (str): name of the user

        Returns:
             `True` if success
        """
        return self.api.remove_user(name)

    def list_apikeys(self, name: str) -> list[str]:
        """
        Args:
            name (str): name of the user

        Returns:
             a list of string of API key
        """
        return self.api.list_apikeys(name)

    def add_apikey(self, name: str) -> bool:
        """
        Args:
            name (str): name of the user

        Returns:
             `True` if success
        """
        return self.api.add_apikey(name)

    def remove_apikey(self, name: str, apikey: str) -> bool:
        """
        Args:
            name (str): name of the user
            apikey (str): an API key to remove

        Returns:
             `True` if success
        """
        return self.api.remove_apikey(name, apikey)

    def close(self) -> None:
        """Close opened API connections."""
        return self._api.close()


def job_from_dict(client: Client, dd: dict[str, Any], **values: Any) -> models.Job:
    d = dict()
    d.update(dd)
    d.update(values)
    return models.Job(
        client,
        d["job_id"],
        d["type"],
        d["query"],
        status=d.get("status"),
        url=d.get("url"),
        debug=d.get("debug"),
        start_at=d.get("start_at"),
        end_at=d.get("end_at"),
        created_at=d.get("created_at"),
        updated_at=d.get("updated_at"),
        cpu_time=d.get("cpu_time"),
        result_size=d.get("result_size"),
        result=d.get("result"),
        result_url=d.get("result_url"),
        hive_result_schema=d.get("hive_result_schema"),
        priority=d.get("priority"),
        retry_limit=d.get("retry_limit"),
        org_name=d.get("org_name"),
        database=d.get("database"),
        num_records=d.get("num_records"),
        user_name=d.get("user_name"),
        linked_result_export_job_id=d.get("linked_result_export_job_id"),
        result_export_target_job_id=d.get("result_export_target_job_id"),
    )
