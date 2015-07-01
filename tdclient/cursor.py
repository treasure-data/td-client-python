#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import time

class Cursor(object):
    def __init__(self, api, wait_interval=3, wait_callback=None, **kwargs):
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
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return self._rowcount

    def callproc(self, procname, *parameters):
        raise NotImplementedError

    def close(self):
        self._api.close()

    def execute(self, query, args=None):
        if args is not None:
            if isinstance(args, dict):
                query = query.format(dict)
            else:
                raise NotImplementedError
        self._executed = self._api.query(query, **self._query_kwargs)
        self._do_execute()
        return self._executed

    def executemany(self, operation, seq_of_parameters):
        return [ self.execute(operation, *parameter) for parameter in seq_of_parameters ]

    def _check_executed(self):
        if self._executed is None:
            raise RuntimeError("execute() first")

    def _do_execute(self):
        if self._rows is None:
            status = self._api.job_status(self._executed)
            if status == "success":
                self._rows = self._api.job_result(self._executed)
                self._rownumber = 0
                self._rowcount = len(self._rows)
                job = self._api.show_job(self._executed)
                self._description = self._result_description(job.get("hive_result_schema", []))
            else:
                if status in ["error", "killed"]:
                    raise RuntimeError("job error: %s: %s" % (self._executed, status))
                else:
                    time.sleep(self.wait_interval)
                    if callable(self.wait_callback):
                        self.wait_callback(self._executed)
                    return self._do_execute()

    def _result_description(self, result_schema):
        if result_schema is None:
            result_schema = []
        return [ (name, None, None, None, None, None, None) for name, t in result_schema ]

    def fetchone(self):
        self._check_executed()
        if self._rownumber < self._rowcount:
            row = self._rows[self._rownumber]
            self._rownumber += 1
            return row
        else:
            raise IndexError("index out of bound (%d out of %d)" % (self._rownumber, self._rowcount))

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        else:
            self._check_executed()
            if self._rownumber + size - 1 < self._rowcount:
                rows = self._rows[self._rownumber:self._rownumber+size]
                self._rownumber += size
                return rows
            else:
                raise IndexError("index out of bound (%d out of %d)" % (self._rownumber, self._rowcount))

    def fetchall(self):
        self._check_executed()
        if self._rownumber < self._rowcount:
            rows = self._rows[self._rownumber:]
            self._rownumber = self._rowcount
            return rows
        else:
            raise IndexError("row index out of bound (%d out of %d)" % (self._rownumber, self._rowcount))

    def nextset(self):
        raise NotImplementedError

    def setinputsizes(self, sizes):
        raise NotImplementedError

    def setoutputsize(self, size, column=None):
        raise NotImplementedError