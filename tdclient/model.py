#!/usr/bin/env python


class Model:
    def __init__(self, client):
        self._client = client

    @property
    def client(self):
        """
        Returns: a :class:`tdclient.client.Client` instance
        """
        return self._client
