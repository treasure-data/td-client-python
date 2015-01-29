#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import gzip
import json
import msgpack
import os
import tempfile
try:
    import urllib.parse as urlparse # >=3.0
except ImportError:
    import urlparse
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote
import warnings

class ImportAPI(object):
    ####
    ## Import API
    ##

    def import_data(self, db, table, format, stream, size, unique_id=None):
        """Import data into Treasure Data Service

        This method expects data from a file-like object formatted with "msgpack.gz".

        Params:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack.gz")
            stream (file-like): a file-like object contains the data
            size (int): the size of the part
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        if unique_id is not None:
            path = "/v3/table/import_with_id/%s/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(unique_id)), urlquote(str(format)))
        else:
            path = "/v3/table/import/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(format)))

        kwargs = {}
        if self._endpoint == self.DEFAULT_ENDPOINT:
            kwargs["endpoint"] = self.DEFAULT_IMPORT_ENDPOINT
        with self.put(path, stream, size, **kwargs) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Import failed", res, body)
            js = self.checked_json(body, ["elapsed_time"])
            time = float(js["elapsed_time"])
            return time

    def import_file(self, db, table, format, file, unique_id=None):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        Params:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack", "json")
            file (str or file-like): a name of a file, or a file-like object contains the data
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        if hasattr(file, "read"):
            if format.endswith(".gz"):
                return self._import_file(db, table, format[0:len(format)-len(".gz")], gzip.GzipFile(fileobj=file), unique_id=unique_id)
            else:
                return self._import_file(db, table, format, file, unique_id=unique_id)
        else:
            with open(file) as fp:
                if format.endswith(".gz"):
                    return self._import_file(db, table, format, gzip.GzipFile(fileobj=fp), unique_id=unique_id)
                else:
                    return self._import_file(db, table, format, fp, unique_id=unique_id)

    def _import_file(self, db, table, format, file, unique_id=None):
        if format == "msgpack":
            return self._import_items(db, table, self._parse_msgpack_file(file), unique_id=unique_id)
        elif format == "json":
            return self._import_items(db, table, self._parse_json_file(file), unique_id=unique_id)
        else:
            raise TypeError("unknown format: %s" % (format,))

    def _import_items(self, db, table, items, unique_id=None):
        with tempfile.TemporaryFile() as fp:
            with gzip.GzipFile(fileobj=fp) as gz:
                packer = msgpack.Packer()
                for record in items:
                    gz.write(packer.pack(record))
            fp.seek(0)
            size = os.fstat(fp.fileno()).st_size
            return self.import_data(db, table, "msgpack.gz", fp, size, unique_id=unique_id)

    def _parse_msgpack_file(self, file):
        unpacker = msgpack.Unpacker(file)
        for record in unpacker:
            yield record

    def _parse_json_file(self, file):
        for s in file:
            try:
                record = json.loads(s)
                yield record
            except ValueError as error:
                warnings.warn("skipped: %s: %s" % (error, repr(s)))
