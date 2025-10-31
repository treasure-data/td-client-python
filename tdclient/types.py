"""Type definitions for td-client-python."""

from __future__ import annotations

from array import array
from typing import IO, TYPE_CHECKING

from typing_extensions import Literal, TypeAlias, TypedDict

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

# File-like types
FileLike: TypeAlias = "str | bytes | IO[bytes]"
"""Type for file inputs: file path, bytes, or file-like object."""

BytesOrStream: TypeAlias = "bytes | bytearray | IO[bytes]"
"""Type for byte data or streams (excluding file paths)."""

StreamBody: TypeAlias = "bytes | bytearray | memoryview | array[int] | IO[bytes] | None"
"""Type for HTTP request body."""

# Query engine types
QueryEngineType: TypeAlias = 'Literal["presto", "hive"]'
"""Type for query engine selection."""

EngineVersion: TypeAlias = 'Literal["stable", "experimental"]'
"""Type for engine version selection."""

Priority: TypeAlias = (
    'Literal[-2, -1, 0, 1, 2, "VERY LOW", "LOW", "NORMAL", "HIGH", "VERY HIGH"]'
)
"""Type for job priority levels (numeric or string)."""

# Data format types
ExportFileFormat: TypeAlias = 'Literal["jsonl.gz", "tsv.gz", "json.gz"]'
"""Type for export file formats."""

DataFormat: TypeAlias = 'Literal["msgpack", "msgpack.gz", "json", "json.gz", "csv", "csv.gz", "tsv", "tsv.gz"]'
"""Type for data import/export formats."""

ResultFormat: TypeAlias = 'Literal["msgpack", "json", "csv", "tsv"]'
"""Type for query result formats."""

# Utility types for CSV parsing and data processing
CSVValue: TypeAlias = "int | float | str | bool | None"
"""Type for values parsed from CSV files."""

Converter: TypeAlias = "Callable[[str], Any]"
"""Type for converter functions that parse string values."""

Record: TypeAlias = "dict[str, Any]"
"""Type for data records (dictionaries with string keys and any values)."""


# TypedDict classes for structured parameters
class ScheduleParams(TypedDict, total=False):
    """Parameters for schedule operations."""

    type: QueryEngineType  # Query type
    query: str  # SQL query to execute
    database: str  # Target database name
    result: str  # Result output location URL
    cron: str  # Schedule: "@daily", "@hourly", or cron expression
    timezone: str  # Timezone e.g. "UTC"
    delay: int  # Delay in seconds before running
    priority: Priority  # Priority: -2 (very low) to 2 (very high)
    retry_limit: int  # Automatic retry count
    engine_version: EngineVersion  # Engine version
    pool_name: str  # For Presto only: pool name


class ExportParams(TypedDict, total=False):
    """Parameters for export operations."""

    bucket: str  # Bucket name
    access_key_id: str  # ID to access the export destination
    secret_access_key: str  # Password for access_key_id
    file_prefix: str  # Filename prefix for exported file
    file_format: ExportFileFormat  # File format
    from_: int  # Start time in Unix epoch format
    to: int  # End time in Unix epoch format
    assume_role: str  # Assume role ARN
    domain_key: str  # Job domain key
    pool_name: str  # For Presto only: pool name


class BulkImportParams(TypedDict, total=False):
    """Parameters for bulk import operations."""

    name: str
    database: str
    table: str


class ResultParams(TypedDict, total=False):
    """Parameters for result operations."""

    name: str
    url: str
    user: str
    password: str
