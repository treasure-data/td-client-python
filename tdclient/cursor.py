#!/usr/bin/env python

import time

from tdclient import errors


class Cursor:
    def __init__(self, api, wait_interval=5, wait_callback=None, **kwargs):
        self._api = api
        self._query_kwargs = kwargs
        self._executed = None
        self._rows = None
        self._rownumber = 0
        self._rowcount = -1
        self._description = []
        self.wait_interval = wait_interval
        self.wait_callback = wait_callback

    @property
    def api(self):
        return self._api

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return self._rowcount

    def callproc(self, procname, *parameters):
        raise errors.NotSupportedError

    def close(self):
        self._api.close()

    def execute(self, query, args=None):
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

    def executemany(self, operation, seq_of_parameters):
        return [
            self.execute(operation, args=parameter) for parameter in seq_of_parameters
        ]

    def _check_executed(self):
        if self._executed is None:
            raise errors.ProgrammingError("execute() first")

    def _do_execute(self):
        self._check_executed()
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
                    raise errors.InternalError(
                        "job error: %s: %s" % (self._executed, status)
                    )
                else:
                    time.sleep(self.wait_interval)
                    if callable(self.wait_callback):
                        self.wait_callback(self)
                    return self._do_execute()

    def _result_description(self, result_schema):
        if result_schema is None:
            result_schema = []
        return [
            (column[0], None, None, None, None, None, None) for column in result_schema
        ]

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence, or `None` when no more data is available.
        """
        self._check_executed()
        if self._rownumber < self._rowcount:
            row = self._rows[self._rownumber]
            self._rownumber += 1
            return row
        else:
            return None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a list of tuples).
        An empty sequence is returned when no more rows are available.
        """
        if size is None:
            return self.fetchall()
        else:
            self._check_executed()
            if self._rownumber + size - 1 < self._rowcount:
                rows = self._rows[self._rownumber : self._rownumber + size]
                self._rownumber += size
                return rows
            else:
                raise errors.InternalError(
                    "index out of bound (%d out of %d)"
                    % (self._rownumber, self._rowcount)
                )

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor's arraysize attribute can affect the performance of this operation.
        """
        self._check_executed()
        if self._rownumber < self._rowcount:
            rows = self._rows[self._rownumber :]
            self._rownumber = self._rowcount
            return rows
        else:
            return []

    def nextset(self):
        raise errors.NotSupportedError

    def setinputsizes(self, sizes):
        raise errors.NotSupportedError

    def setoutputsize(self, size, column=None):
        raise errors.NotSupportedError

    def show_job(self):
        """Returns detailed information of a Job

        Returns:
             :class:`dict`: Detailed information of a job
        """
        self._check_executed()
        return self._api.show_job(self._executed)

    def job_status(self):
        """Show job status

        Returns:
             The status information of the given job id at last execution.
        """
        self._check_executed()
        return self._api.job_status(self._executed)

    def job_result(self):
        """Fetch job results

        Returns:
             Job result in :class:`list`
        """
        self._check_executed()
        return self._api.job_result(self._executed)
