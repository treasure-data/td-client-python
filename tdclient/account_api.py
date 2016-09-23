#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import time

class AccountAPI(object):
    ####
    ## Account API
    ##

    def show_account(self):
        """
        TODO: add docstring
        """
        with self.get("/v3/account/show") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show account failed", res, body)
            js = self.checked_json(body, ["account"])
            a = js["account"]
            a["id"] = int(a["id"])
            a["plan"] = int(a["plan"])
            a["storage_size"] = int(a["storage_size"])
            a["guaranteed_cores"] = int(a["guaranteed_cores"])
            a["maximum_cores"] = int(a["maximum_cores"])
            a["created_at"] = self._parsedate(a["created_at"], "%Y-%m-%d")
            return a

    def account_core_utilization(self, _from, to):
        """
        TODO: add docstring
        """
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        with self.get("/v3/account/core_utilization", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show account failed", res, body)
            js = self.checked_json(body, ["from", "to", "interval", "history"])
            _from = self._parsedate(js["from"], "%Y-%m-%d %H:%M:%S %Z")
            to = self._parsedate(js["to"], "%Y-%m-%d %H:%M:%S %Z")
            interval = int(js["interval"])
            history = js["history"]
            return [_from, to, interval, history]
