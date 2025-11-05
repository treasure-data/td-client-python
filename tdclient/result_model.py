#!/usr/bin/env python

from typing import TYPE_CHECKING

from tdclient.model import Model

if TYPE_CHECKING:
    from tdclient.client import Client


class Result(Model):
    """Result on Treasure Data Service"""

    def __init__(
        self, client: "Client", name: str, url: str, org_name: str | None
    ) -> None:
        super().__init__(client)
        self._name = name
        self._url = url
        self._org_name = org_name

    @property
    def name(self) -> str:
        """str: a name for a authentication"""
        return self._name

    @property
    def url(self) -> str:
        """str: a result output URL"""
        return self._url

    @property
    def org_name(self) -> str | None:
        """str: organization name"""
        return self._org_name
