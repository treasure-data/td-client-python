#!/usr/bin/env python

from urllib.parse import quote as urlquote


class ExportAPI:
    ####
    ## Export API
    ##

    def export_data(self, db, table, storage_type, params=None):
        """
        TODO: add docstring
        => jobId:str
        """
        params = {} if params is None else params
        params["storage_type"] = storage_type
        with self.post(
            "/v3/export/run/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Export failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
