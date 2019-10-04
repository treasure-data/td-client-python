#!/usr/bin/env python

from .util import create_url


class ResultAPI:
    """Access to Result API.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def list_result(self):
        """Get the list of all the available authentications.

        Returns:
            [(str, str, None)]: The list of tuple of name, Result output url, and
                 organization name (always `None` for api compatibility).
        """
        with self.get("/v3/result/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List result table failed", res, body)
            js = self.checked_json(body, ["results"])
            return [
                (m["name"], m["url"], None) for m in js["results"]
            ]  # same as database

    def create_result(self, name, url, params=None):
        """Create a new authentication with the specified name.

        Args:
            name (str): Authentication name.
            url (str):  Url of the authentication to be created. e.g. "ftp://test.com/"
            params (dict, optional): Extra parameters.
        Returns:
            bool: True if succeeded.
        """
        params = {} if params is None else params
        params.update({"url": url})
        with self.post(
            create_url("/v3/result/create/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create result table failed", res, body)
            return True

    def delete_result(self, name):
        """Delete the authentication having the specified name.

        Args:
            name (str): Authentication name.
        Returns:
            bool: True if succeeded.
        """
        with self.post(create_url("/v3/result/delete/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete result table failed", res, body)
            return True
