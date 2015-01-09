#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class ResultAPI(object):
    ####
    ## Result API
    ##

    def list_result(self):
        with self.get("/v3/result/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List result table failed", res, body)
            js = self.checked_json(body, ["results"])
            return [ [m["name"], m["url"], None] for m in js["result"] ] # same as database

    # => true
    def create_result(self, name, url, params={}):
        params.update({"url": url})
        with self.post("/v3/result/create/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create result table failed", res, body)
            return True

    # => true
    def delete_result(self, name):
        with self.post("/v3/result/delete/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete result table failed", res, body)
            return True
