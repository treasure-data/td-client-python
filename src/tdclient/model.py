#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

class Model(object):
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """
        Returns: a :class:`tdclient.Client` instance
        """
        return self._client
