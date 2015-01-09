#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class UserAPI(object):
    ####
    ## User API
    ##

    # apikey:String
    def authenticate(self, user, password):
        with self.post("/v3/user/authenticate", {"user": user, "password": password}) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Authentication failed", res, body)
            js = self.checked_json(body, ["apikey"])
            apikey = js["apikey"]
            return apikey

    # => [[name:String,organization:String,[user:String]]
    def list_users(self):
        with self.get("/v3/user/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List users failed", res, body)
            js = self.checked_json(body, ["users"])
            def user(roleinfo):
                name = roleinfo["name"]
                email = roleinfo["email"]
                return [name, None, None, email] # set nil to org and role for API compatibility
            return [ user(roleinfo) for roleinfo in js["users"] ]

    # => true
    def add_user(self, name, org, email, password):
        params = {"organization": org, "email": email, "password": password}
        with self.post("/v3/user/add/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding user failed", res, body)
            return True

    # => true
    def remove_user(self, user):
        with self.post("/v3/user/remove/%s" % (urlquote(str(user)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing user failed", res, body)
            return True

    # => true
    def change_email(self, user, email):
        params = {"email": email}
        with self.post("/v3/user/email/change/%s" % (urlquote(str(user))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing email failed", res, body)
            return True

    # => [apikey:String]
    def list_apikeys(self, user):
        with self.get("/v3/user/apikey/list/%s" % (urlquote(str(user)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List API keys failed", res, body)
            js = self.checked_json(body, ["apikeys"])
            return js["apikeys"]

    # => true
    def add_apikey(self, user):
        with self.post("/v3/user/apikey/add/%s" % (urlquote(str(user)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding API key failed", res, body)
            return True

    # => true
    def remove_apikey(self, user, apikey):
        params = {"apikey": apikey}
        with self.post("/v3/user/apikey/remove/%s" % (urlquote(str(user))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing API key failed", res, body)
            return True

    # => true
    def change_password(self, user, password):
        params = {"password": password}
        with self.post("/v3/user/password/change/%s" % (urlquote(str(user))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True

    # => true
    def change_my_password(self, old_password, password):
        params = {"old_password": old_password, "password": password}
        with self.post("/v3/user/password/change", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True
