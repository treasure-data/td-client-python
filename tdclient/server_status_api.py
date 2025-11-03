#!/usr/bin/env python

from contextlib import AbstractContextManager
from typing import Any

import urllib3


class ServerStatusAPI:
    """Access to Server Status API

    This class is inherited by :class:`tdclient.api.API`.
    """

    # Methods from API class
    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[urllib3.BaseHTTPResponse]: ...
    def checked_json(self, body: bytes, required: list[str]) -> dict[str, Any]: ...

    def server_status(self) -> str:
        """Show the status of Treasure Data

        Returns:
            str: status
        """
        with self.get("/v3/system/server_status") as res:
            code, body = res.status, res.read()
            if code != 200:
                return f"Server is down ({code})"
            js = self.checked_json(body, ["status"])
            status = js["status"]
            return status
