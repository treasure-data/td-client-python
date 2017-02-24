#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import collections
import contextlib
import gzip
import io
import msgpack
import os
try:
    from urllib.parse import quote as urlquote # >=3.0
except ImportError:
    from urllib import quote as urlquote

class BulkImportAPI(object):
    ####
    ## Bulk import API
    ##

    def create_bulk_import(self, name, db, table, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/create/%s/%s/%s" % (urlquote(str(name)), urlquote(str(db)), urlquote(str(table))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create bulk import failed", res, body)
            return True

    def delete_bulk_import(self, name, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/delete/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete bulk import failed", res, body)
            return True

    def show_bulk_import(self, name):
        """
        TODO: add docstring
        => data:dict
        """
        with self.get("/v3/bulk_import/show/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Show bulk import failed", res, body)
            js = self.checked_json(body, ["status"])
            return js

    def list_bulk_imports(self, params=None):
        """
        TODO: add docstring
        => result:[data:dict]
        """
        params = {} if params is None else params
        with self.get("/v3/bulk_import/list", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List bulk imports failed", res, body)
            js = self.checked_json(body, ["bulk_imports"])
            return js["bulk_imports"]

    def list_bulk_import_parts(self, name, params=None):
        """
        TODO: add docstring
        """
        params = {} if params is None else params
        with self.get("/v3/bulk_import/list_parts/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List bulk import parts failed", res, body)
            js = self.checked_json(body, ["parts"])
            return js["parts"]

    @staticmethod
    def validate_part_name(part_name):
        """Make sure the part_name is valid

        Params:
            part_name (str): The part name the user is trying to use
        """
        # Check for duplicate periods
        d = collections.defaultdict(int)
        for char in part_name:
            d[char] += 1

        if 1 < d['.']:
            raise ValueError("part names cannot contain multiple periods: %s" % (repr(part_name)))

        if 0 < part_name.find("/"):
            raise ValueError("part name must not contain '/': %s" % (repr(part_name)))

    def bulk_import_upload_part(self, name, part_name, stream, size):
        """
        TODO: add docstring
        => None
        """
        self.validate_part_name(part_name)
        with self.put("/v3/bulk_import/upload_part/%s/%s" % (urlquote(str(name)), urlquote(str(part_name))), stream, size) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Upload a part failed", res, body)

    def bulk_import_upload_file(self, name, part_name, format, file, **kwargs):
        """
        TODO: add docstring
        => None
        """
        self.validate_part_name(part_name)
        with contextlib.closing(self._prepare_file(file, format, **kwargs)) as fp:
            size = os.fstat(fp.fileno()).st_size
            return self.bulk_import_upload_part(name, part_name, fp, size)

    def bulk_import_delete_part(self, name, part_name, params=None):
        """
        TODO: add docstring
        => True
        """
        self.validate_part_name(part_name)
        params = {} if params is None else params
        with self.post("/v3/bulk_import/delete_part/%s/%s" % (urlquote(str(name)), urlquote(str(part_name))), params) as res:
            code, body = res.status, res.read()
            if code / 100 != 2:
                self.raise_error("Delete a part failed", res, body)
            return True

    def freeze_bulk_import(self, name, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/freeze/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Freeze bulk import failed", res, body)
            return True

    def unfreeze_bulk_import(self, name, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/unfreeze/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Unfreeze bulk import failed", res, body)
            return True

    def perform_bulk_import(self, name, params=None):
        """
        TODO: add docstring
        => jobId:str
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/perform/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Perform bulk import failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])

    def commit_bulk_import(self, name, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        with self.post("/v3/bulk_import/commit/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Commit bulk import failed", res, body)
            return True

    def bulk_import_error_records(self, name, params=None):
        """
        TODO: add docstring
        => data...
        """
        params = {} if params is None else params
        with self.get("/v3/bulk_import/error_records/%s" % (urlquote(str(name))), params) as res:
            code = res.status
            if code != 200:
                body = res.read()
                self.raise_error("Failed to get bulk import error records", res, body)

            body = io.BytesIO(res.read())
            decompressor = gzip.GzipFile(fileobj=body)

            content_type = res.getheader("content-type", "application/x-msgpack; charset=utf-8")
            type_params = dict([ p.strip().split("=", 2) for p in content_type.split(";") if 0 < p.find("=") ])

            unpacker = msgpack.Unpacker(decompressor, encoding=str(type_params.get("charset", "utf-8")))
            for row in unpacker:
                yield row
