#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class UserAPI(object):
    ####
    ## User API
    ##

    def authenticate(self, user, password):
        """
        TODO: add docstring
        apikey:str
        """
        with self.post("/v3/user/authenticate", {"user": user, "password": password}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Authentication failed", res, body)
            js = self.checked_json(body, ["apikey"])
            apikey = js["apikey"]
            return apikey

    def list_users(self):
        """
        TODO: add docstring
        => [[name:str,organization:str,[user:str]]
        """
        with self.get("/v3/user/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List users failed", res, body)
            js = self.checked_json(body, ["users"])
            def user(roleinfo):
                name = roleinfo["name"]
                email = roleinfo["email"]
                return (name, None, None, email) # set None to org and role for API compatibility
            return [ user(roleinfo) for roleinfo in js["users"] ]

    def add_user(self, name, org, email, password):
        """
        TODO: add docstring
        => True
        """
        params = {"organization": org, "email": email, "password": password}
        with self.post("/v3/user/add/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding user failed", res, body)
            return True

    def remove_user(self, name):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/user/remove/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing user failed", res, body)
            return True

    def change_email(self, name, email):
        """
        TODO: add docstring
        => True
        """
        params = {"email": email}
        with self.post("/v3/user/email/change/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing email failed", res, body)
            return True

    def list_apikeys(self, name):
        """
        TODO: add docstring
        => [apikey:str]
        """
        with self.get("/v3/user/apikey/list/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List API keys failed", res, body)
            js = self.checked_json(body, ["apikeys"])
            return js["apikeys"]

    def add_apikey(self, name):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/user/apikey/add/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding API key failed", res, body)
            return True

    def remove_apikey(self, name, apikey):
        """
        TODO: add docstring
        => True
        """
        params = {"apikey": apikey}
        with self.post("/v3/user/apikey/remove/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing API key failed", res, body)
            return True

    def change_password(self, name, password):
        """
        TODO: add docstring
        => True
        """
        params = {"password": password}
        with self.post("/v3/user/password/change/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True

    def change_my_password(self, old_password, password):
        """
        TODO: add docstring
        => True
        """
        params = {"old_password": old_password, "password": password}
        with self.post("/v3/user/password/change", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True
