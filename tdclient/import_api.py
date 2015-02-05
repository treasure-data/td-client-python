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

class ImportAPI(object):
    ####
    ## Import API
    ##

    def import_data(self, db, table, format, bytes_or_stream, size, unique_id=None):
        """Import data into Treasure Data Service

        This method expects data from a file-like object formatted with "msgpack.gz".

        Params:
            db (str): name of a database
            table (str): name of a table
            format (str): format of data type (e.g. "msgpack.gz")
            bytes_or_stream (str or file-like): a byte string or a file-like object contains the data
            size (int): the length of the data
            unique_id (str): a unique identifier of the data

        Returns: float represents the elapsed time to import data
        """
        if unique_id is not None:
            path = "/v3/table/import_with_id/%s/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(unique_id)), urlquote(str(format)))
        else:
            path = "/v3/table/import/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(format)))

        kwargs = {}
        with self.put(path, bytes_or_stream, size, **kwargs) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Import failed", res, body)
            js = self.checked_json(body, ["elapsed_time"])
            time = float(js["elapsed_time"])
            return time

    def import_file(self, db, table, format, file, unique_id=None):
        """Import data into Treasure Data Service, from an existing file on filesystem.

        This method will decompress/deserialize records from given file, and then
        convert it into format acceptable from Treasure Data Service ("msgpack.gz").

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
                    return self._import_file(db, table, format[0:len(format)-len(".gz")], gzip.GzipFile(fileobj=fp), unique_id=unique_id)
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
            with gzip.GzipFile(mode="wb", fileobj=fp) as gz:
                packer = msgpack.Packer()
                for record in items:
                    gz.write(packer.pack(record))
            fp.seek(0)
            size = os.fstat(fp.fileno()).st_size
            return self.import_data(db, table, "msgpack.gz", fp, size, unique_id=unique_id)

    def _parse_msgpack_file(self, file):
        # current impl doesn't torelate any unpack error
        unpacker = msgpack.Unpacker(file)
        for record in unpacker:
            yield record

    def _parse_json_file(self, file):
        # current impl doesn't torelate any JSON parse error
        for s in file:
            record = json.loads(s.decode("utf-8"))
            yield record
