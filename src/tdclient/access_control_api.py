#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

class AccessControlAPI(object):
    ####
    ## Access Control API
    ##
    def grant_access_control(self, subject, action, scope, grant_option):
        """
        TODO: add docstring
        """
        params = {"subject": subject, "action": action, "scope": scope, "grant_option": str(grant_option)}
        with self.post("/v3/acl/grant", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Granting access control failed", res, body)
            return True

    def revoke_access_control(self, subject, action, scope):
        """
        TODO: add docstring
        """
        params = {"subject": subject, "action": action, "scope": scope}
        with self.post("/v3/acl/revoke", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Revoking access control failed", res, body)
            return True

    def test_access_control(self, user, action, scope):
        """
        TODO: add docstring
        [True, [{subject:str,action:str,scope:str}]]
        """
        params = {"user": user, "action": action, "scope": scope}
        with self.get("/v3/acl/test", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Testing access control failed", res, body)
            js = self.checked_json(body, ["permission", "access_controls"])
            perm = js["permission"]
            acl = [ [roleinfo["subject"], roleinfo["action"], roleinfo["scope"]] for roleinfo in js["access_controls"] ]
            return (perm, acl)

    def list_access_controls(self):
        """
        TODO: add docstring
        [{subject:str,action:str,scope:str}]
        """
        with self.get("/v3/acl/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Listing access control failed", res, body)
            js = self.checked_json(body, ["access_controls"])
            acl = [ [roleinfo["subject"], roleinfo["action"], roleinfo["scope"], roleinfo["grant_option"]] for roleinfo in js["access_controls"] ]
            return acl
