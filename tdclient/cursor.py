#!/usr/bin/env python

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from tdclient import errors

if TYPE_CHECKING:
    from tdclient.api import API


class Cursor:
    def __init__(
        self,
        api: API,
        wait_interval: int = 5,
        wait_callback: Callable[[Cursor], None] | None = None,
        **kwargs: Any,
    ) -> None:
        self._api = api
        self._query_kwargs = kwargs
        self._executed: str | None = None  # Job ID
        self._rows: list[Any] | None = None
        self._rownumber = 0
        self._rowcount = -1
        self._description: list[Any] = []
        self.wait_interval = wait_interval
        self.wait_callback = wait_callback

    @property
    def api(self) -> API:
        return self._api

    @property
    def description(self) -> list[Any]:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def callproc(self, procname: str, *parameters: Any) -> None:
        raise errors.NotSupportedError

    def close(self) -> None:
        self._api.close()

    def execute(self, query: str, args: dict[str, Any] | None = None) -> str | None:
        if args is not None:
            if isinstance(args, dict):
                query = query.format(**args)
            else:
                raise errors.NotSupportedError
        self._executed = self._api.query(query, **self._query_kwargs)
        self._rows = None
        self._rownumber = 0
        self._rowcount = -1
        self._description = []
        self._do_execute()
        return self._executed

    def executemany(
        self, operation: str, seq_of_parameters: list[dict[str, Any]]
    ) -> list[str | None]:
        return [
            self.execute(operation, args=parameter) for parameter in seq_of_parameters
        ]

    def _check_executed(self) -> None:
        if self._executed is None:
            raise errors.ProgrammingError("execute() first")

    def _do_execute(self) -> None:
        self._check_executed()
        assert self._executed is not None
        if self._rows is None:
            status = self._api.job_status(self._executed)
            if status == "success":
                self._rows = self._api.job_result(self._executed)
                self._rownumber = 0
                self._rowcount = len(self._rows)
                job = self._api.show_job(self._executed)
                self._description = self._result_description(
                    job.get("hive_result_schema", [])
                )
            else:
                if status in ["error", "killed"]:
                    raise errors.InternalError(f"job error: {self._executed}: {status}")
                else:
                    time.sleep(self.wait_interval)
                    if callable(self.wait_callback):
                        self.wait_callback(self)
                    return self._do_execute()

    def _result_description(
        self, result_schema: list[Any] | None
    ) -> list[tuple[Any, ...]]:
        if result_schema is None:
            result_schema = []
        return [
            (column[0], None, None, None, None, None, None) for column in result_schema
        ]

    def fetchone(self) -> Any | None:
        """
        Fetch the next row of a query result set, returning a single sequence, or `None` when no more data is available.
        """
        self._check_executed()
        assert self._rows is not None
        if self._rownumber < self._rowcount:
            row = self._rows[self._rownumber]
            self._rownumber += 1
            return row
        else:
            return None

    def fetchmany(self, size: int | None = None) -> list[Any]:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a list of tuples).
        An empty sequence is returned when no more rows are available.
        """
        if size is None:
            return self.fetchall()
        else:
            self._check_executed()
            assert self._rows is not None
            if self._rownumber + size - 1 < self._rowcount:
                rows = self._rows[self._rownumber : self._rownumber + size]
                self._rownumber += size
                return rows
            else:
                raise errors.InternalError(
                    f"index out of bound ({self._rownumber} out of {self._rowcount})"
                )

    def fetchall(self) -> list[Any]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor's arraysize attribute can affect the performance of this operation.
        """
        self._check_executed()
        assert self._rows is not None
        if self._rownumber < self._rowcount:
            rows = self._rows[self._rownumber :]
            self._rownumber = self._rowcount
            return rows
        else:
            return []

    def nextset(self) -> None:
        raise errors.NotSupportedError

    def setinputsizes(self, sizes: Any) -> None:
        raise errors.NotSupportedError

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        raise errors.NotSupportedError

    def show_job(self) -> dict[str, Any]:
        """Returns detailed information of a Job

        Returns:
             :class:`dict`: Detailed information of a job
        """
        self._check_executed()
        assert self._executed is not None
        return self._api.show_job(self._executed)

    def job_status(self) -> str:
        """Show job status

        Returns:
             The status information of the given job id at last execution.
        """
        self._check_executed()
        assert self._executed is not None
        return self._api.job_status(self._executed)

    def job_result(self) -> list[dict[str, Any]]:
        """Fetch job results

        Returns:
             Job result in :class:`list`
        """
        self._check_executed()
        assert self._executed is not None
        return self._api.job_result(self._executed)
