#!/usr/bin/env python

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tdclient.model import Model

if TYPE_CHECKING:
    from tdclient.client import Client


class User(Model):
    """User on Treasure Data Service"""

    def __init__(
        self,
        client: Client,
        name: str,
        org_name: str,
        role_names: list[str],
        email: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(client)
        self._name = name
        self._org_name = org_name
        self._role_names = role_names
        self._email = email

    @property
    def name(self) -> str:
        """
        Returns: name of the user
        """
        return self._name

    @property
    def org_name(self) -> str:
        """
        Returns: organization name
        """
        return self._org_name

    @property
    def role_names(self) -> list[str]:
        """
        TODO: add docstring
        """
        return self._role_names

    @property
    def email(self) -> str:
        """
        Returns: e-mail address
        """
        return self._email
