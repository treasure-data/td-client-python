#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from tdclient import api
from tdclient import cursor

class Connection(object):
    def __init__(self, type=None, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        cursor_kwargs = dict()
        if type is not None:
            cursor_kwargs["type"] = type
        if db is not None:
            cursor_kwargs["db"] = db
        if result_url is not None:
            cursor_kwargs["result_url"] = result_url
        if priority is not None:
            cursor_kwargs["priority"] = priority
        if retry_limit is not None:
            cursor_kwargs["retry_limit"] = retry_limit
        self._api = api.API(**kwargs)
        self._cursor_kwargs = cursor_kwargs

    def close(self):
        self._api.close()

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def cursor(self):
        return cursor.Cursor(self._api, **self._cursor_kwargs)
