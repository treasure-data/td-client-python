#!/usr/bin/env python

from tdclient.model import Model


class Result(Model):
    """Result on Treasure Data Service
    """

    def __init__(self, client, name, url, org_name):
        super(Result, self).__init__(client)
        self._name = name
        self._url = url
        self._org_name = org_name

    @property
    def name(self):
        """str: a name for a authentication
        """
        return self._name

    @property
    def url(self):
        """str: a result output URL
        """
        return self._url

    @property
    def org_name(self):
        """str: organization name
        """
        return self._org_name
