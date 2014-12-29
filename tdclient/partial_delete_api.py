#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

class PartialDeleteAPI(object):
    ####
    ## Partial delete API
    ##

    def partial_delete(self, db, table, to, _from, params={}):
        params["to"] = str(to)
        params["from"] = str(_from)
        code, body, res = self.post("/v3/table/partialdelete/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params)
        if code != 200:
            self.raise_error("Partial delete failed", res, body)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])
