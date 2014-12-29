#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

class ImportAPI(object):
    ####
    ## Import API
    ##

    # TODO: `import` is not available as Python method name
#   # => time:Float
#   def import(self, db, table, _format, stream, size, unique_id=None):
#       if unique_id is not None:
#           path = "/v3/table/import_with_id/%s/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(unique_id)), urlquote(str(format)))
#       else:
#           path = "/v3/table/import/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(forat)))
#       opts = {}
#       if self._host == DEFAULT_ENDPOINT
#           uri = urlparse.urlparse(DEFAULT_IMPORT_ENDPOINT)
#           opts["host"] = uri.host
#           opts["port"] = uri.port
#       code, body, res = self.put(path, stream, size, opts)
#       if code / 100 != 2:
#           self.raise_error("Import failed", res, body)
#       js = self.checked_json(body, [])
#       time = float(js["elapsed_time"])
#       return time
    pass
