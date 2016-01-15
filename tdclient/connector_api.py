#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class ConnectorAPI(object):
    ####
    ## Data Connector API
    ##

    def connector_guess(self, job):
        """
        Params:
          job (dict): :class:`dict` representation of `seed.yml`

        Returns: :class:`dict`
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        payload = json.dumps(job).encode("utf-8") if isinstance(job, dict) else job
        with self.post("/v3/bulk_loads/guess", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector configuration guess failed", res, body)
            return self.checked_json(body, [])

    def connector_preview(self, job):
        """
        Params:
          job (dict): :class:`dict` representation of `load.yml`

        Returns: :class:`dict`
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        payload = json.dumps(job).encode("utf-8") if isinstance(job, dict) else job
        with self.post("/v3/bulk_loads/preview", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job preview failed", res, body)
            return self.checked_json(body, [])

    def connector_issue(self, db, table, job):
        """
        Params:
          db (str): name of the database to perform connector job
          table (str): name of the table to perform connector job
          job (dict): :class:`dict` representation of `load.yml`

        Returns: jobId:str
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        params = dict(job)
        params["database"] = db
        params["table"] = table
        payload = json.dumps(params).encode("utf-8")
        with self.post("/v3/job/issue/bulkload/%s" % (urlquote(str(db))), payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnector job issuing failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def connector_list(self):
        """
        Returns: :class:`list`
        """
        with self.get("/v3/bulk_loads") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession list retrieve failed", res, body)
            # cannot use `checked_json` since `GET /v3/bulk_loads` returns an array
            return json.loads(body.decode("utf-8"))

    def connector_create(self, name, database, table, job, params=None):
        """
        Params:
          name (str): name of the connector job
          database (str): name of the database to perform connector job
          table (str): name of the table to perform connector job
          job (dict): :class:`dict` representation of `load.yml`

        Returns: :class:`dict`
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        params = {} if params is None else dict(params)
        params.update(job)
        params["name"] = name
        params["database"] = database
        params["table"] = table
        payload = json.dumps(params).encode("utf-8")
        with self.post("/v3/bulk_loads", payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession: %s created failed" % (name,), res, body)
            return self.checked_json(body, [])

    def connector_show(self, name):
        """
        Params:
          name (str): name of the connector job

        Returns: :class:`dict`
        """
        with self.get("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession: %s retrieve failed" % (name,), res, body)
            return self.checked_json(body, [])

    def connector_update(self, name, job):
        """
        Params:
          name (str): name of the connector job
          job (dict): :class:`dict` representation of `load.yml`

        Returns: :class:`dict`
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        payload = json.dumps(job).encode("utf-8")
        with self.put("/v3/bulk_loads/%s" % (urlquote(str(name)),), payload, len(payload), headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession: %s update failed" % (name,), res, body)
            return self.checked_json(body, [])

    def connector_delete(self, name):
        """
        Params:
          name (str): name of the connector job

        Returns: :class:`dict`
        """
        with self.delete("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession: %s delete failed" % (name,), res, body)
            return self.checked_json(body, [])

    def connector_history(self, name):
        """
        Params:
          name (str): name of the connector job

        Returns: :class:`list`
        """
        with self.get("/v3/bulk_loads/%s/jobs" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("history of DataConnectorSession: %s retrieve failed" % (name,), res, body)
            return json.loads(body.decode("utf-8"))

    def connector_run(self, name, **kwargs):
        """
        Params:
          name (str): name of the connector job

        Returns: :class:`dict`
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
        }
        payload = json.dumps(kwargs).encode("utf-8")
        with self.post("/v3/bulk_loads/%s/jobs" % (urlquote(str(name)),), payload, headers=headers) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("DataConnectorSession: %s job create failed" % (name,), res, body)
            return self.checked_json(body, [])
