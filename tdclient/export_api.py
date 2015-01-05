#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class ExportAPI(object):
    ####
    ## Export API
    ##

    # => jobId:String
    def export_data(self, db, table, storage_type, params={}):
        params["storage_type"] = storage_type
        code, body, res = self.post("/v3/export/run/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params)
        if code != 200:
            self.raise_error("Export failed", res, body)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])
