#!/usr/bin/env python

from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any

from tdclient import api, cursor, errors
from tdclient.types import Priority

if TYPE_CHECKING:
    from tdclient.cursor import Cursor


class Connection:
    def __init__(
        self,
        type: str | None = None,
        db: str | None = None,
        result_url: str | None = None,
        priority: Priority | None = None,
        retry_limit: int | None = None,
        wait_interval: int | None = None,
        wait_callback: Callable[["Cursor"], None] | None = None,
        **kwargs: Any,
    ) -> None:
        cursor_kwargs: dict[str, Any] = dict()
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

    def __enter__(self) -> "Connection":
        return self

    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._api.close()

    @property
    def api(self) -> api.API:
        return self._api

    def close(self) -> None:
        self._api.close()

    def commit(self) -> None:
        raise errors.NotSupportedError

    def rollback(self) -> None:
        raise errors.NotSupportedError

    def cursor(self) -> "Cursor":
        return cursor.Cursor(self._api, **self._cursor_kwargs)
