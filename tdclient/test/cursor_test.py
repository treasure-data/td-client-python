#!/usr/bin/env python

from unittest import mock

import pytest

from tdclient import cursor, errors
from tdclient.test.test_helper import *


def setup_function(function):
    unset_environ()


def test_cursor():
    td = cursor.Cursor(mock.MagicMock())
    assert td._rows is None
    assert td._rownumber == 0
    assert td.rowcount == -1
    assert td.description == []


def test_cursor_close():
    td = cursor.Cursor(mock.MagicMock())
    td.close()
    assert td.api.close.called


def test_cursor_execute():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    td.api.query = mock.MagicMock(return_value=42)
    td._do_execute = mock.MagicMock()
    assert td.execute("SELECT 1") == 42
    td.api.query.assert_called_with("SELECT 1", db="sample_datasets")
    assert td._do_execute.called
    assert td._rows is None
    assert td._rownumber == 0
    assert td._rowcount == -1
    assert td._description == []


def test_cursor_execute_format_dict():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    td.api.query = mock.MagicMock(return_value=42)
    td._do_execute = mock.MagicMock()
    assert td.execute("SELECT {i} FROM {t}", args={"i": 13, "t": "dual"}) == 42
    td.api.query.assert_called_with("SELECT 13 FROM dual", db="sample_datasets")
    assert td._do_execute.called
    assert td._rows is None
    assert td._rownumber == 0
    assert td._rowcount == -1
    assert td._description == []


def test_cursor_execute_format_tuple():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    with pytest.raises(errors.NotSupportedError) as error:
        td.execute("SELECT %d FROM %s", args=(13, "dual"))


def test_cursor_executemany():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    td.api.query = mock.MagicMock(side_effect=[1, 1, 2])
    td._do_execute = mock.MagicMock()
    assert td.executemany("SELECT {i}", [{"i": 1}, {"i": 2}, {"i": 3}]) == [1, 1, 2]
    td.api.query.assert_called_with("SELECT 3", db="sample_datasets")
    assert td._do_execute.called
    assert td._rows is None
    assert td._rownumber == 0
    assert td._rowcount == -1
    assert td._description == []


def test_check_executed():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    assert td._executed is None
    with pytest.raises(errors.ProgrammingError) as error:
        td._check_executed()
    td._executed = "42"
    td._check_executed()


def test_do_execute_success():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    td._executed = "42"
    td._check_executed = mock.MagicMock(return_value=True)
    td.api.job_status = mock.MagicMock(return_value="success")
    td.api.job_result = mock.MagicMock(
        return_value=[["foo", 1], ["bar", 1], ["baz", 2]]
    )
    td.api.show_job = mock.MagicMock(
        return_value={"hive_result_schema": [["col0", "varchar"], ["col1", "long"]]}
    )
    td._do_execute()
    assert td._check_executed.called
    td.api.job_status.assert_called_with("42")
    td.api.job_result.assert_called_with("42")
    td.api.show_job.assert_called_with("42")
    assert td._rownumber == 0
    assert td._rowcount == 3
    assert td._description == [
        ("col0", None, None, None, None, None, None),
        ("col1", None, None, None, None, None, None),
    ]


def test_do_execute_error():
    td = cursor.Cursor(mock.MagicMock(), db="sample_datasets")
    td._executed = "42"
    td.api.job_status = mock.MagicMock(side_effect=["error"])
    with pytest.raises(errors.InternalError) as error:
        td._do_execute()


def test_do_execute_wait():
    td = cursor.Cursor(
        mock.MagicMock(),
        db="sample_datasets",
        wait_interval=5,
        wait_callback=mock.MagicMock(),
    )
    td._executed = "42"
    td._check_executed = mock.MagicMock(return_value=True)
    td.api.job_status = mock.MagicMock(side_effect=["queued", "running", "success"])
    td.api.job_result = mock.MagicMock(
        return_value=[["foo", 1], ["bar", 1], ["baz", 2]]
    )
    td.api.show_job = mock.MagicMock(
        return_value={"hive_result_schema": [["col0", "varchar"], ["col1", "long"]]}
    )
    with mock.patch("time.sleep") as t_sleep:
        td._do_execute()
        t_sleep.assert_called_with(5)
        assert td.wait_callback.called
        assert td._check_executed.called
        td.api.job_status.assert_called_with("42")
        td.api.job_result.assert_called_with("42")
        td.api.show_job.assert_called_with("42")
        assert td._rownumber == 0
        assert td._rowcount == 3
        assert td._description == [
            ("col0", None, None, None, None, None, None),
            ("col1", None, None, None, None, None, None),
        ]


def test_result_description():
    td = cursor.Cursor(mock.MagicMock())
    assert td._result_description(None) == []
    assert td._result_description([["col0", "int"]]) == [
        ("col0", None, None, None, None, None, None)
    ]


def test_fetchone():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td._rows = [["foo", 1], ["bar", 1], ["baz", 2]]
    td._rownumber = 0
    td._rowcount = len(td._rows)
    assert td.fetchone() == ["foo", 1]
    assert td.fetchone() == ["bar", 1]
    assert td.fetchone() == ["baz", 2]
    assert td.fetchone() == None


def test_fetchmany():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td._rows = [["foo", 1], ["bar", 1], ["baz", 2]]
    td._rownumber = 0
    td._rowcount = len(td._rows)
    assert td.fetchmany(2) == [["foo", 1], ["bar", 1]]
    assert td.fetchmany() == [["baz", 2]]
    assert td.fetchmany() == []
    with pytest.raises(errors.InternalError) as error:
        td.fetchmany(1)


def test_fetchall():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td._rows = [["foo", 1], ["bar", 1], ["baz", 2]]
    td._rownumber = 0
    td._rowcount = len(td._rows)
    assert td.fetchall() == [["foo", 1], ["bar", 1], ["baz", 2]]
    assert td.fetchall() == []


def test_show_job():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td.show_job()
    td.api.show_job.assert_called_with("42")


def test_job_status():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td.job_status()
    td.api.job_status.assert_called_with("42")


def test_job_result():
    td = cursor.Cursor(mock.MagicMock())
    td._executed = "42"
    td.job_result()
    td.api.job_result.assert_called_with("42")


def test_cursor_callproc():
    td = cursor.Cursor(mock.MagicMock())
    with pytest.raises(errors.NotSupportedError) as error:
        td.callproc("f")


def test_cursor_nextset():
    td = cursor.Cursor(mock.MagicMock())
    with pytest.raises(errors.NotSupportedError) as error:
        td.nextset()


def test_cursor_setinputsizes():
    td = cursor.Cursor(mock.MagicMock())
    with pytest.raises(errors.NotSupportedError) as error:
        td.setinputsizes(42)


def test_cursor_setoutputsize():
    td = cursor.Cursor(mock.MagicMock())
    with pytest.raises(errors.NotSupportedError) as error:
        td.setoutputsize(42)
