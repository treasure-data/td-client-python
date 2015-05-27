#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from tdclient.model import Model

class Account(Model):
    """Account on Treasure Data Service
    """

    def __init__(self, client, account_id, plan, storage_size=None, guaranteed_cores=None, maximum_cores=None, created_at=None):
        super(Account, self).__init__(client)
        self._account_id = account_id
        self._plan = plan
        self._storage_size = storage_size
        self._guaranteed_cores = guaranteed_cores
        self._maximum_cores = maximum_cores
        self._created_at = created_at

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
