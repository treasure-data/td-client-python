#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

class AccountAPI(object):
    ####
    ## Account API
    ##

    def show_account(self):
        code, body, res = self.get("/v3/account/show")
        if code != 200:
            self.raise_error("Show account failed", res, body)
        js = self.checked_json(body, ["account"])
        a = js["account"]
        account_id = int(a["id"])
        plan = int(a["plan"])
        storage_size = int(a["storage_size"])
        guaranteed_cores = int(a["guaranteed_cores"])
        maximum_cores = int(a["maximum_cores"])
        created_at = a["created_at"]
        return [account_id, plan, storage_size, guaranteed_cores, maximum_cores, created_at]

    def account_core_utilization(self, _from, to):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        code, body, res = get("/v3/account/core_utilization", params)
        if code != 200:
            self.raise_error("Show account failed", res, body)
        js = self.checked_json(body, ["from", "to", "interval", "history"])
        _from = time.strptime(js["from"], "%Y-%m-%d %H:%M:%S %Z")
        to = time.strptime(js["to"], "%Y-%m-%d %H:%M:%S %Z")
        interval = int(js["interval"])
        history = js["history"]
        return [_from, to, interval, history]
