#!/usr/bin/env python

from urllib.parse import quote as urlquote


class PartialDeleteAPI:
    ####
    ## Partial delete API
    ##

    def partial_delete(self, db, table, to, _from, params=None):
        """
        TODO: add docstring
        """
        params = {} if params is None else params
        params["to"] = str(to)
        params["from"] = str(_from)
        with self.post("/v3/table/partialdelete/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Partial delete failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
