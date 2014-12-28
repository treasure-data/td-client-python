#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

class ServerStatusAPI(object):
    ####
    ## Server Status API
    ##

    # => status:String
    def server_status(self):
        code, body, res = self.get("/v3/system/server_status")
        if code != 200:
            return "Server is down (%d)" % (code,)
        js = self.checked_json(body, ["status"])
        status = js["status"]
        return status
