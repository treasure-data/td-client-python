#!/usr/bin/env python


class ServerStatusAPI:
    ####
    ## Server Status API
    ##

    def server_status(self):
        """
        TODO: add docstring
        => status:str
        """
        with self.get("/v3/system/server_status") as res:
            code, body = res.status, res.read()
            if code != 200:
                return "Server is down (%d)" % (code,)
            js = self.checked_json(body, ["status"])
            status = js["status"]
            return status
