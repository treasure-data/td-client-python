#!/usr/bin/env python


class ServerStatusAPI:
    """Access to Server Status API

    This class is inherited by :class:`tdclient.api.API`.
    """

    def server_status(self):
        """Show the status of Treasure Data

        Returns:
            str: status
        """
        with self.get("/v3/system/server_status") as res:
            code, body = res.status, res.read()
            if code != 200:
                return "Server is down (%d)" % (code,)
            js = self.checked_json(body, ["status"])
            status = js["status"]
            return status
