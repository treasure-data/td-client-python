#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from unittest import mock
except ImportError:
    import mock

from tdclient import model
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()

def test_bulk_import():
    client = mock.MagicMock()
    bulk_import = model.BulkImport(client, name="name", database="database", table="table", status="status", upload_frozen="upload_frozen", job_id="job_id", valid_records="valid_records", error_records="error_records", valid_parts="valid_parts", error_parts="error_parts")
    assert bulk_import.name == "name"
    assert bulk_import.database == "database"
    assert bulk_import.table == "table"
    assert bulk_import.status == "status"
    assert bulk_import.job_id == "job_id"
    assert bulk_import.valid_records == "valid_records"
    assert bulk_import.error_records == "error_records"
    assert bulk_import.valid_parts == "valid_parts"
    assert bulk_import.error_parts == "error_parts"
    assert bulk_import.upload_frozen == "upload_frozen"
