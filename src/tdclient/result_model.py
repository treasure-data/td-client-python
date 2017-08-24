#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

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
        """
        TODO: add docstring
        """
        return self._name

    @property
    def url(self):
        """
        TODO: add docstring
        """
        return self._url

    @property
    def org_name(self):
        """
        TODO: add docstring
        """
        return self._org_name
