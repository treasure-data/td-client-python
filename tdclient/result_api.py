#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote

class ResultAPI(object):
    ####
    ## Result API
    ##

    def list_result(self):
        code, body, res = self.get("/v3/result/list")
        if code != 200:
          self.raise_error("List result table failed", res)
        js = self.checked_json(body, ["results"])
        return [ [m["name"], m["url"], None] for m in js["result"] ] # same as database

    # => true
    def create_result(self, name, url, params={}):
        params.update({"url": url})
        code, body, res = self.post("/v3/result/create/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Create result table failed", res)
        return True

    # => true
    def delete_result(self, name):
        code, body, res = self.post("/v3/result/delete/%s" % (urlquote(str(name))))
        if code != 200:
            self.raise_error("Delete result table failed", res)
        return True


