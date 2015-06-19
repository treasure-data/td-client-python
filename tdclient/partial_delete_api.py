#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class PartialDeleteAPI(object):
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
