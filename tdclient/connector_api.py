#!/usr/bin/env python

import json
from contextlib import AbstractContextManager
from typing import Any

import urllib3

from tdclient.types import BytesOrStream
from tdclient.util import create_url, normalize_connector_config


class ConnectorAPI:
    """Access Data Connector API which handles Data Connector.

    This class is inherited by :class:`tdclient.api.API`.
    """

    # Methods from API class
    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def post(
        self,
        path: str,
        params: dict[str, Any] | bytes | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def put(
        self,
        path: str,
        bytes_or_stream: BytesOrStream,
        size: int,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def raise_error(
        self, msg: str, res: urllib3.BaseHTTPResponse, body: bytes
    ) -> None: ...
    def checked_json(self, body: bytes, required: list[str]) -> dict[str, Any]: ...

    def connector_guess(self, job: dict[str, Any] | bytes) -> dict[str, Any]:
        """Guess the Data Connector configuration

        Args:
            job (dict): :class:`dict` representation of `seed.yml`
                        See Also: https://www.embulk.org/docs/built-in.html#guess-executor

        Returns:
             :class:`dict`: The configuration of the Data Connector.

        Examples:
            >>> config = {
            ...     "in": {
            ...         "type": "s3",
            ...         "bucket": "your-bucket",
            ...         "path_prefix": "logs/csv-",
            ...         "access_key_id": "YOUR-AWS-ACCESS-KEY",
            ...         "secret_access_key": "YOUR-AWS-SECRET-KEY"
            ...     },
            ...     "out": {"mode": "append"},
            ...     "exec": {"guess_plugins": ["json", "query_string"]},
            ... }
            >>> td.api.connector_guess(config)
            {'config': {'in': {'type': 's3',
               'bucket': 'your-bucket',
               'path_prefix': 'logs/csv-',
               'access_key_id': 'YOUR-AWS-ACCESS-KEY',
               'secret_access_key': 'YOU-AWS-SECRET-KEY',
               'parser': {'charset': 'UTF-8',
                'newline': 'LF',
                'type': 'csv',
                'delimiter': ',',
                'quote': '"',
                'escape': '"',
                'trim_if_not_quoted': False,
                'skip_header_lines': 1,
                'allow_extra_columns': False,
                'allow_optional_columns': False,
                'columns': [{'name': 'sepal.length', 'type': 'double'},
                 {'name': 'sepal.width', 'type': 'double'},
                 {'name': 'petal.length', 'type': 'double'},
                 {'name': 'petal.width', 'type': 'string'},
                 {'name': 'variety', 'type': 'string'}]}},
              'out': {'mode': 'append'},
              'exec': {'guess_plugin': ['json', 'query_string']},
              'filters': [{'rules': [{'rule': 'upper_to_lower'},
                 {'pass_types': ['a-z', '0-9'],
                  'pass_characters': '_',
                  'replace': '_',
                  'rule': 'character_types'},
                 {'pass_types': ['a-z'],
                  'pass_characters': '_',
                  'prefix': '_',
                  'rule': 'first_character_types'},
                 {'rule': 'unique_number_suffix', 'max_length': 128}],
                'type': 'rename'},
               {'from_value': {'mode': 'upload_time'},
                'to_column': {'name': 'time'},
                'type': 'add_time'}]}}
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        if isinstance(job, dict):
            job = {"config": normalize_connector_config(job)}
            payload = json.dumps(job).encode("utf-8")
        else:
            # Not checking the format. Assuming the right format
            payload = job

        with self.post("/v3/bulk_loads/guess", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector configuration guess failed", res, body)
            return self.checked_json(body, [])

    def connector_preview(self, job: dict[str, Any]) -> dict[str, Any]:
        """Show the preview of the Data Connector job.

        Args:
            job (dict): :class:`dict` representation of `load.yml`

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        payload = json.dumps(job).encode("utf-8")
        with self.post("/v3/bulk_loads/preview", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job preview failed", res, body)
            return self.checked_json(body, [])

    def connector_issue(self, db: str, table: str, job: dict[str, Any]) -> str:
        """Create a Data Connector job.

        Args:
            db (str): name of the database to perform connector job
            table (str): name of the table to perform connector job
            job (dict): :class:`dict` representation of `load.yml`

        Returns:
             str: job Id
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        params = dict(job)
        params["database"] = db
        params["table"] = table
        payload = json.dumps(params).encode("utf-8")
        with self.post(
            create_url("/v3/job/issue/bulkload/{db}", db=db), payload, headers=headers
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job issuing failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def connector_list(self) -> list[dict[str, Any]]:
        """Show the list of available Data Connector sessions.

        Returns:
             :class:`list`
        """
        with self.get("/v3/bulk_loads") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession list retrieve failed", res, body)
            # cannot use `checked_json` since `GET /v3/bulk_loads` returns an array
            return json.loads(body.decode("utf-8"))

    def connector_create(
        self,
        name: str,
        database: str,
        table: str,
        job: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a Data Connector session.

        Args:
            name (str): name of the connector job
            database (str): name of the database to perform connector job
            table (str): name of the table to perform connector job
            job (dict): :class:`dict` representation of `load.yml`
            params (dict, optional): Extra parameters

                - config (str):
                     Embulk configuration as JSON format.
                     See also https://www.embulk.org/docs/built-in.html#embulk-configuration-file-format
                - cron (str, optional):
                     Schedule of the query.
                     {``"@daily"``, ``"@hourly"``, ``"10 * * * *"`` (custom cron)}
                     See also: https://docs.treasuredata.com/articles/#!pd/Scheduling-Jobs-Using-TD-Console
                - delay (int, optional):
                     A delay ensures all buffered events are imported
                     before running the query. Default: 0
                - database (str):
                     Target database for the Data Connector session
                - name (str):
                     Name of the Data Connector session
                - table (str):
                     Target table for the Data Connector session
                - time_column (str, optional):
                     Column in the table for registering config.out.time
                - timezone (str):
                     Timezone for scheduled Data Connector session.
                     See here for list of supported timezones https://gist.github.com/frsyuki/4533752

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        params = {} if params is None else dict(params)
        params.update(job)
        params["name"] = name
        params["database"] = database
        params["table"] = table
        payload = json.dumps(params).encode("utf-8")
        with self.post("/v3/bulk_loads", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"DataConnectorSession: {name} created failed", res, body
                )
            return self.checked_json(body, [])

    def connector_show(self, name: str) -> dict[str, Any]:
        """Show a specific Data Connector session information.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`dict`
        """
        with self.get(create_url("/v3/bulk_loads/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"DataConnectorSession: {name} retrieve failed", res, body
                )
            return self.checked_json(body, [])

    def connector_update(self, name: str, job: dict[str, Any]) -> dict[str, Any]:
        """Update a specific Data Connector session.

        Args:
          name (str): name of the connector job
          job (dict): :class:`dict` representation of `load.yml`.
              For detailed format, see also: https://www.embulk.org/docs/built-in.html#embulk-configuration-file-format

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        payload = json.dumps(job).encode("utf-8")
        with self.put(
            create_url("/v3/bulk_loads/{name}", name=name),
            payload,
            len(payload),
            headers=headers,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"DataConnectorSession: {name} update failed", res, body
                )
            return self.checked_json(body, [])

    def connector_delete(self, name: str) -> dict[str, Any]:
        """Delete a Data Connector session.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`dict`
        """
        with self.delete(create_url("/v3/bulk_loads/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"DataConnectorSession: {name} delete failed", res, body
                )
            return self.checked_json(body, [])

    def connector_history(self, name: str) -> list[dict[str, Any]]:
        """Show the list of the executed jobs information for the Data Connector job.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`list`
        """
        with self.get(create_url("/v3/bulk_loads/{name}/jobs", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"history of DataConnectorSession: {name} retrieve failed",
                    res,
                    body,
                )
            return json.loads(body.decode("utf-8"))

    def connector_run(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Create a job to execute Data Connector session.

        Args:
            name (str): name of the connector job
            **kwargs (optional): Extra parameters.

                - scheduled_time (int):
                    Time in Unix epoch format that would be set as
                    `TD_SCHEDULED_TIME`.
                - domain_key (str):
                    Job domain key which is assigned to a single job.

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        payload = json.dumps(kwargs).encode("utf-8")
        with self.post(
            create_url("/v3/bulk_loads/{name}/jobs", name=name),
            payload,
            headers=headers,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    f"DataConnectorSession: {name} job create failed", res, body
                )
            return self.checked_json(body, [])
