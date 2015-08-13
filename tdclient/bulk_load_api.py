#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class BulkLoadAPI(object):
    ####
    ## Bulk load API
    ##

    def bulk_load_guess(self, job):
        """
        TODO: add docstring
        """
        with self.post("/v3/bulk_loads/guess", job) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoad configuration guess failed", res, body)
            return json.loads(body)

    def bulk_load_preview(self, job):
        """
        TODO: add docstring
        """
        with self.post("/v3/bulk_loads/preview", job) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoad job preview failed", res, body)
            return json.loads(body)

    def bulk_load_issue(self, db, table, job):
        """
        TODO: add docstring
        """
        params = dict(job)
        params["database"] = db
        params["table"] = table
        with self.post("/v3/job/issue/bulkload/%s" % (urlquote(str(db))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoad job issuing failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def bulk_load_list(self):
        """
        TODO: add docstring
        """
        with self.get("/v3/bulk_loads") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession list retrieve failed", res, body)
            return json.loads(body)

    def bulk_load_create(self, name, database, table, job, params=None):
        """
        TODO: add docstring
        """
        params = {} if params is None else dict(params)
        params.update(job)
        params["name"] = name
        params["database"] = database
        params["table"] = table
        with self.post("/v3/bulk_loads", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession: %s created failed" % (name,), res, body)
            return json.loads(body)

    def bulk_load_show(self, name):
        """
        TODO: add docstring
        """
        with self.get("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession: %s retrieve failed" % (name,), res, body)
            return json.loads(body)

    def bulk_load_update(self, name, job):
        """
        TODO: add docstring
        """
        with self.put("/v3/bulk_loads/%s" % (urlquote(str(name)),), job) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession: %s update failed" % (name,), res, body)
            return json.loads(body)

    def bulk_load_delete(self, name):
        """
        TODO: add docstring
        """
        with self.delete("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession: %s delete failed" % (name,), res, body)
            return json.loads(body)

    def bulk_load_history(self, name):
        """
        TODO: add docstring
        """
        with self.get("/v3/bulk_loads/%s" % (urlquote(str(name)),)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("history of BulkLoadSession: %s retrieve failed" % (name,), res, body)
            return json.loads(body)

    def bulk_load_run(self, name, **kwargs):
        """
        TODO: add docstring
        """
        with self.post("/v3/bulk_loads/%s" % (urlquote(str(name)),), kwargs) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("BulkLoadSession: %s job create failed" % (name,), res, body)
            return json.loads(body)
