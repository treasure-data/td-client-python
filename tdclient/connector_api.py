#!/usr/bin/env python

import json
from urllib.parse import quote as urlquote


class ConnectorAPI:
    """Access Data Connector API which handles Data Connector.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def connector_guess(self, job):
        """Guess the Data Connector configuration

        Args:
            job (dict): :class:`dict` representation of `seed.yml`

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        payload = json.dumps(job).encode("utf-8") if isinstance(job, dict) else job
        with self.post("/v3/bulk_loads/guess", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector configuration guess failed", res, body)
            return self.checked_json(body, [])

    def connector_preview(self, job):
        """Show the preview of the Data Connector job.

        Args:
            job (dict): :class:`dict` representation of `load.yml`

        Returns:
             :class:`dict`
        """
        headers = {"content-type": "application/json; charset=utf-8"}
        payload = json.dumps(job).encode("utf-8") if isinstance(job, dict) else job
        with self.post("/v3/bulk_loads/preview", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job preview failed", res, body)
            return self.checked_json(body, [])

    def connector_issue(self, db, table, job):
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
            "/v3/job/issue/bulkload/%s" % (urlquote(str(db))), payload, headers=headers
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job issuing failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def connector_list(self):
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

    def connector_create(self, name, database, table, job, params=None):
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
                     See also: https://support.treasuredata.com/hc/en-us/articles/360001451088-Scheduled-Jobs-Web-Console
                - delay (int, optional):
                     A delay ensures all buffered events are imported
                     before running the query. Default: 0
                - database (str):
                     Target databse for the Data Connector session
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
                    "DataConnectorSession: %s created failed" % (name,), res, body
                )
            return self.checked_json(body, [])

    def connector_show(self, name):
        """Show a specific Data Connector session information.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`dict`
        """
        with self.get("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    "DataConnectorSession: %s retrieve failed" % (name,), res, body
                )
            return self.checked_json(body, [])

    def connector_update(self, name, job):
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
            "/v3/bulk_loads/%s" % (urlquote(str(name)),),
            payload,
            len(payload),
            headers=headers,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    "DataConnectorSession: %s update failed" % (name,), res, body
                )
            return self.checked_json(body, [])

    def connector_delete(self, name):
        """Delete a Data Connector session.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`dict`
        """
        with self.delete("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    "DataConnectorSession: %s delete failed" % (name,), res, body
                )
            return self.checked_json(body, [])

    def connector_history(self, name):
        """Show the list of the executed jobs information for the Data Connector job.

        Args:
            name (str): name of the connector job

        Returns:
             :class:`list`
        """
        with self.get("/v3/bulk_loads/%s/jobs" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    "history of DataConnectorSession: %s retrieve failed" % (name,),
                    res,
                    body,
                )
            return json.loads(body.decode("utf-8"))

    def connector_run(self, name, **kwargs):
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
            "/v3/bulk_loads/%s/jobs" % (urlquote(str(name)),), payload, headers=headers
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error(
                    "DataConnectorSession: %s job create failed" % (name,), res, body
                )
            return self.checked_json(body, [])
