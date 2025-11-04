#!/usr/bin/env python

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tdclient.client import Client


class Model:
    def __init__(self, client: "Client") -> None:
        self._client = client

    @property
    def client(self) -> "Client":
        """
        Returns: a :class:`tdclient.client.Client` instance
        """
        return self._client
