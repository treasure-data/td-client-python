#!/usr/bin/env python

from tdclient import api, cursor, errors


class Connection:
    def __init__(
        self,
        type=None,
        db=None,
        result_url=None,
        priority=None,
        retry_limit=None,
        wait_interval=None,
        wait_callback=None,
        **kwargs
    ):
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
        if wait_interval is not None:
            cursor_kwargs["wait_interval"] = wait_interval
        if wait_callback is not None:
            cursor_kwargs["wait_callback"] = wait_callback
        self._api = api.API(**kwargs)
        self._cursor_kwargs = cursor_kwargs

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._api.close()

    @property
    def api(self):
        return self._api

    def close(self):
        self._api.close()

    def commit(self):
        raise errors.NotSupportedError

    def rollback(self):
        raise errors.NotSupportedError

    def cursor(self):
        return cursor.Cursor(self._api, **self._cursor_kwargs)
