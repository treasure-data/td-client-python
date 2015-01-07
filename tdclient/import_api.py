#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib.parse import urlparse # >=3.0
except ImportError:
    from urlparse import urlparse
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
        opts = {}

        default_endpoint = urlparse(self.DEFAULT_ENDPOINT)
        if self._host == default_endpoint.hostname:
            import_endpoint = urlparse(self.DEFAULT_IMPORT_ENDPOINT)
            opts["host"] = import_endpoint.hostname
            if import_endpoint.port is None:
                if import_endpoint.scheme == "http":
                    opts["port"] = 80
                elif import_endpoint.scheme == "https":
                    opts["port"] = 443
                else:
                    raise ValueError("Invalid endpoint: %s" % (self.DEFAULT_IMPORT_ENDPOINT))
            else:
                opts["port"] = import_endpoint.port
        with self.put(path, stream, size, opts) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Import failed", res, body)
            js = self.checked_json(body, ["elapsed_time"])
            time = float(js["elapsed_time"])
            return time
