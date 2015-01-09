#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    import urllib.parse as urlparse # >=3.0
except ImportError:
    import urlparse
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class ImportAPI(object):
    ####
    ## Import API
    ##

    # => time:Float
    def import_data(self, db, table, format, stream, size, unique_id=None):
        if unique_id is not None:
            path = "/v3/table/import_with_id/%s/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(unique_id)), urlquote(str(format)))
        else:
            path = "/v3/table/import/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(format)))

        kwargs = {}
        if self._endpoint == self.DEFAULT_ENDPOINT:
            kwargs["endpoint"] = self.DEFAULT_IMPORT_ENDPOINT
        with self.put(path, stream, size, **kwargs) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Import failed", res, body)
            js = self.checked_json(body, ["elapsed_time"])
            time = float(js["elapsed_time"])
            return time
