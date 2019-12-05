#!/usr/bin/env python

from .util import create_url


class UserAPI:
    def authenticate(self, user, password):
        with self.post(
            "/v3/user/authenticate", {"user": user, "password": password}
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Authentication failed", res, body)
            js = self.checked_json(body, ["apikey"])
            apikey = js["apikey"]
            return apikey

    def list_users(self):
        with self.get("/v3/user/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List users failed", res, body)
            js = self.checked_json(body, ["users"])

            return [user_to_tuple(roleinfo) for roleinfo in js["users"]]

    def add_user(self, name, org, email, password):
        params = {"organization": org, "email": email, "password": password}
        with self.post(create_url("/v3/user/add/{name}", name=name), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding user failed", res, body)
            return True

    def remove_user(self, name):
        with self.post(create_url("/v3/user/remove/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing user failed", res, body)
            return True

    def list_apikeys(self, name):
        with self.get(create_url("/v3/user/apikey/list/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List API keys failed", res, body)
            js = self.checked_json(body, ["apikeys"])
            return js["apikeys"]

    def add_apikey(self, name):
        with self.post(create_url("/v3/user/apikey/add/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding API key failed", res, body)
            return True

    def remove_apikey(self, name, apikey):
        params = {"apikey": apikey}
        with self.post(
            create_url("/v3/user/apikey/remove/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing API key failed", res, body)
            return True


def user_to_tuple(roleinfo):
    name = roleinfo["name"]
    email = roleinfo["email"]
    return (name, None, None, email)  # set None to org and role for API compatibility
