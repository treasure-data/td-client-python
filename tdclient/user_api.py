#!/usr/bin/env python

from contextlib import AbstractContextManager
from typing import Any

import urllib3

from tdclient.util import create_url


class UserAPI:
    # Methods from API class
    def get(
        self,
        path: str,
        params: dict[str, Any] | bytes | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def post(
        self,
        path: str,
        params: dict[str, Any] | bytes | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def raise_error(
        self, msg: str, res: urllib3.BaseHTTPResponse, body: bytes
    ) -> None: ...
    def checked_json(self, body: bytes, required: list[str]) -> dict[str, Any]: ...

    def authenticate(self, user: str, password: str) -> str:
        with self.post(
            "/v3/user/authenticate", {"user": user, "password": password}
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Authentication failed", res, body)
            js = self.checked_json(body, ["apikey"])
            apikey = js["apikey"]
            return apikey

    def list_users(self) -> list[tuple[str, None, None, str]]:
        with self.get("/v3/user/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List users failed", res, body)
            js = self.checked_json(body, ["users"])

            return [user_to_tuple(roleinfo) for roleinfo in js["users"]]

    def add_user(self, name: str, org: str, email: str, password: str) -> bool:
        params = {"organization": org, "email": email, "password": password}
        with self.post(create_url("/v3/user/add/{name}", name=name), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding user failed", res, body)
            return True

    def remove_user(self, name: str) -> bool:
        with self.post(create_url("/v3/user/remove/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing user failed", res, body)
            return True

    def list_apikeys(self, name: str) -> list[str]:
        with self.get(create_url("/v3/user/apikey/list/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List API keys failed", res, body)
            js = self.checked_json(body, ["apikeys"])
            return js["apikeys"]

    def add_apikey(self, name: str) -> bool:
        with self.post(create_url("/v3/user/apikey/add/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding API key failed", res, body)
            return True

    def remove_apikey(self, name: str, apikey: str) -> bool:
        params = {"apikey": apikey}
        with self.post(
            create_url("/v3/user/apikey/remove/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing API key failed", res, body)
            return True


def user_to_tuple(roleinfo: dict[str, Any]) -> tuple[str, None, None, str]:
    name = roleinfo["name"]
    email = roleinfo["email"]
    return (name, None, None, email)  # set None to org and role for API compatibility
