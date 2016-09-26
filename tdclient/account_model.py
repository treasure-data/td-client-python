#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from tdclient.model import Model

class Account(Model):
    """Account on Treasure Data Service
    """

    def __init__(self, client, *args, **kwargs):
        super(Account, self).__init__(client)
        if 0 < len(args):
            self._account_id = args[0]
            self._plan = args[1]
        else:
            self._account_id = kwargs.get("id")
            self._plan = kwargs.get("plan")
        self._storage_size = kwargs.get("storage_size")
        self._guaranteed_cores = kwargs.get("guaranteed_cores")
        self._maximum_cores = kwargs.get("maximum_cores")
        self._created_at = kwargs.get("created_at")

    @property
    def account_id(self):
        """
        TODO: add docstring
        """
        return self._account_id

    @property
    def plan(self):
        """
        TODO: add docstring
        """
        return self._plan

    @property
    def storage_size(self):
        """
        TODO: add docstring
        """
        return self._storage_size

    @property
    def guaranteed_cores(self):
        """
        TODO: add docstring
        """
        return self._guaranteed_cores

    @property
    def maximum_cores(self):
        """
        TODO: add docstring
        """
        return self._maximum_cores

    @property
    def created_at(self):
        """
        TODO: add docstring
        """
        return self._created_at

    @property
    def storage_size_string(self):
        """
        TODO: add docstring
        """
        if self._storage_size <= 1024 * 1024:
            return "0.0 GB"
        elif self._storage_size <= 60 * 1024 * 1024:
            return "0.01 GB"
        elif self._storage_size <= 60 * 1024 * 1024 * 1024:
            return "%.1f GB" % (float(self._storage_size) / (1024 * 1024 * 1024))
        else:
            return "%d GB" % int(float(self._storage_size) / (1024 * 1024 * 1024))
