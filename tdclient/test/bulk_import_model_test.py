#!/usr/bin/env python

import io
from unittest import mock

from tdclient import models
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_bulk_import():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
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


def test_bulk_import_delete():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.delete()
    client.delete_bulk_import.assert_called_with("name")


def test_bulk_import_freeze():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.freeze()
    client.freeze_bulk_import.assert_called_with("name")
    assert bulk_import.update.called


def test_bulk_import_unfreeze():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.unfreeze()
    client.unfreeze_bulk_import.assert_called_with("name")
    assert bulk_import.update.called


def test_bulk_import_perfom():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.perform()
    client.perform_bulk_import.assert_called_with("name")
    assert bulk_import.update.called


def test_bulk_import_commit():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.commit()
    client.commit_bulk_import.assert_called_with("name")
    assert bulk_import.update.called


def test_bulk_import_upload_part():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.upload_part("part", b"bytes", 5)
    client.bulk_import_upload_part.assert_called_with("name", "part", b"bytes", 5)
    assert bulk_import.update.called


def test_bulk_import_upload_file():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    stream = io.BytesIO(b"")
    bulk_import.upload_file("part", "json", stream)
    client.bulk_import_upload_file.assert_called_with("name", "part", "json", stream)
    assert bulk_import.update.called


def test_bulk_import_delete_part():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.delete_part("part")
    client.bulk_import_delete_part.assert_called_with("name", "part")
    assert bulk_import.update.called


def test_bulk_import_list_parts():
    client = mock.MagicMock()
    bulk_import = models.BulkImport(
        client,
        name="name",
        database="database",
        table="table",
        status="status",
        upload_frozen="upload_frozen",
        job_id="job_id",
        valid_records="valid_records",
        error_records="error_records",
        valid_parts="valid_parts",
        error_parts="error_parts",
    )
    bulk_import.update = mock.MagicMock()
    bulk_import.list_parts()
    client.list_bulk_import_parts.assert_called_with("name")
    assert bulk_import.update.called
