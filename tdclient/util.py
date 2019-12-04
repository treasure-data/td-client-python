import csv
import io
import logging
import warnings
from urllib.parse import quote as urlquote

import dateutil.parser
import msgpack

log = logging.getLogger(__name__)


def create_url(tmpl, **values):
    """Create url with values

    Args:
        tmpl (str): url template
        values (dict): values for url
    """
    quoted_values = {k: urlquote(str(v)) for k, v in values.items()}
    return tmpl.format(**quoted_values)


def validate_record(record):
    """Check that `record` contains a key called "time".
    
    Args:
        record (dict): a dictionary representing a data record, where the
        keys name the "columns".

    Returns:
        True if there is a key called "time" (it actually checks for ``"time"``
        (a string) and ``b"time"`` (a binary)). False if there is no key
        called "time".
    """
    if not any(k in record for k in ("time", b"time")):
        warnings.warn(
            'records should have "time" column to import records properly.',
            category=RuntimeWarning,
        )
    return True


def guess_csv_value(s):
    """Determine the most appropriate type for `s` and return it.

    Tries to interpret `s` as a more specific datatype, in the following
    order, and returns the first that succeeds:

    1. As an integer
    2. As a floating point value
    3. If it is "false" or "true" (case insensitive), then as a boolean
    4. If it is "" or "none" or "null" (case insensitive), then as None
    5. As the string itself, unaltered
    
    Args:
        s (str): a string value, assumed to have been read from a CSV file.
    Returns:
        A good guess at a more specific value (int, float, str, bool or None)
    """
    try:
        return int(s)
    except (OverflowError, ValueError):
        try:
            return float(s)
        except (OverflowError, ValueError):
            pass
    lower = s.lower()
    if lower in ("false", "true"):
        return "true" == lower
    elif lower in ("", "none", "null"):
        return None
    else:
        return s


# Convert our dtype names to callables that parse a string into that type
DTYPE_TO_CALLABLE = {
    "bool": bool,
    "float": float,
    "int": int,
    "str": str,
    "guess": guess_csv_value,
}


def merge_dtypes_and_converters(dtypes=None, converters=None):
    """Generate a merged dictionary from those given.
    
    Args:
        dtypes (optional dict): A dictionary mapping column name to "dtype"
          (datatype), where "dtype" may be any of the strings 'bool', 'float',
          'int', 'str' or 'guess'.
        converters (optional dict): A dictionary mapping column name to a
          callable. The callable should take a string as its single argument,
          and return the result of parsing that string.
    
    Internally, the `dtypes` dictionary is converted to a temporary dictionary
    of the same form as `converters` - that is, mapping column names to
    callables. The "data type" string values in `dtypes` are converted to the
    Python builtins of the same name, and the value `"guess"` is converted to
    the `tdclient.util.guess_csv_value`_ callable.

    Example:
        >>> merge_dtypes_and_converters(
        ...    dtypes={'col1': 'int', 'col2': 'float'},
        ...    converters={'col2': int},
        ... )
        {'col1': int, 'col2': int}

    Returns:
        (dict) A dictionary which maps column names to callables.
        If a column name occurs in both input dictionaries, the callable
        specified in `converters` is used.
    """
    our_converters = {}
    if dtypes is not None:
        try:
            for column_name, dtype in dtypes.items():
                our_converters[column_name] = DTYPE_TO_CALLABLE[dtype]
        except KeyError as e:
            raise ValueError(
                "Unrecognized dtype %r, must be one of %s"
                % (dtype, ", ".join(repr(k) for k in sorted(DTYPE_TO_CALLABLE)))
            )
    if converters is not None:
        for column_name, parse_fn in converters.items():
            our_converters[column_name] = parse_fn
    return our_converters


def parse_csv_value(k, s, converters=None):
    """Given a CSV (string) value, work out an actual value.

    Args:
        k (str): The name of the column that the value belongs to.
        s (str): The value as read from the CSV input.
        converters (optional dict): A dictionary mapping column name to callable.
    
    If `converters` is given, and there is a key matching `k` in `converters`,
    then ``converters[k](s)`` will be called to work out the return value.
    Otherwise, `tdclient.util.guess_csv_value`_ will be called with `s` as its
    argument.

    .. warning:: No attempt is made to cope with any errors occurring in a
       callable from the `converters` dictionary. So if ``int`` is called
       on the string ``"not-an-int"`` the resulting ``ValueError`` is not
       caught.

    Example:

        >>> repr(parse_csv_value('col1', 'A string'))
        'A string'
        >>> repr(parse_csv_value('col1', '10'))
        10
        >>> repr(parse_csv_value('col1', '10', {'col1': float, 'col2': int}))
        10.0

    Returns:
        The value for the CSV column, after parsing by a callable from
        `converters`, or after parsing by `tdclient.util.guess_csv_value`_.
    """
    if converters is None:
        parse_fn = guess_csv_value
    else:
        parse_fn = converters.get(k, guess_csv_value)
    return parse_fn(s)


def csv_dict_record_reader(file_like, encoding, dialect):
    """Yield records from a CSV input using csv.DictReader.
    
    This is a reader suitable for use by `tdclient.util.read_csv_records`_.
    
    It is used to read CSV data when the column names are read from the first
    row in the CSV data.

    Args:
        file_like: acts like an instance of io.BufferedIOBase. Reading from it
            returns bytes.
        encoding (str): the name of the encoding to use when turning those
            bytes into strings.
        dialect (str): the name of the CSV dialect to use.

    Yields:
        For each row of CSV data read from `file_like`, yields a dictionary
        whose keys are column names (determined from the first row in the CSV
        data) and whose values are the column values.
    """
    reader = csv.DictReader(io.TextIOWrapper(file_like, encoding), dialect=dialect)
    for row in reader:
        yield row


def csv_text_record_reader(file_like, encoding, dialect, columns):
    """Yield records from a CSV input using csv.reader and explicit column names.
    
    This is a reader suitable for use by `tdclient.util.read_csv_records`_.
    
    It is used to read CSV data when the column names are supplied as an
    explicit `columns` parameter.

    Args:
        file_like: acts like an instance of io.BufferedIOBase. Reading from it
            returns bytes.
        encoding (str): the name of the encoding to use when turning those
            bytes into strings.
        dialect (str): the name of the CSV dialect to use.

    Yields:
        For each row of CSV data read from `file_like`, yields a dictionary
        whose keys are column names (determined by `columns`) and whose values
        are the column values.
    """
    reader = csv.reader(io.TextIOWrapper(file_like, encoding), dialect=dialect)
    for row in reader:
        yield dict(zip(columns, row))


def read_csv_records(csv_reader, dtypes=None, converters=None, **kwargs):
    """Read records using csv_reader and yield the results.

    """
    our_converters = merge_dtypes_and_converters(dtypes, converters)

    for row in csv_reader:
        record = {k: parse_csv_value(k, v, our_converters) for (k, v) in row.items()}
        validate_record(record)
        yield record


def create_msgpack(items):
    """Create msgpack streaming bytes from list

    Args:
        items (list of dict): target list

    Returns:
        Converted msgpack streaming (bytes)

    Examples:

        >>> t1 = int(time.time())
        >>> l1 = [{"a": 1, "b": 2, "time": t1}, {"a":3, "b": 6, "time": t1}]
        >>> create_msgpack(l1)
        b'\\x83\\xa1a\\x01\\xa1b\\x02\\xa4time\\xce]\\xa5X\\xa1\\x83\\xa1a\\x03\\xa1b\\x06\\xa4time\\xce]\\xa5X\\xa1'
    """
    stream = io.BytesIO()
    packer = msgpack.Packer()
    for item in items:
        try:
            mp = packer.pack(item)
        except (OverflowError, ValueError):
            packer.reset()
            mp = packer.pack(normalized_msgpack(item))
        stream.write(mp)

    return stream.getvalue()


def normalized_msgpack(value):
    """Recursively convert int to str if the int "overflows".

    Args:
        value (list, dict, int, float, str, bool or None): value to be normalized

    If `value` is a list, then all elements in the list are (recursively)
    normalized.

    If `value` is a dictionary, then all the dictionary keys and values are
    (recursively) normalized.
    
    If `value` is an integer, and outside the range ``-(1 << 63)`` to
    ``(1 << 64)``, then it is converted to a string.

    Otherwise, `value` is returned unchanged.

    Returns:
        Normalized value
    """
    if isinstance(value, (list, tuple)):
        return [normalized_msgpack(v) for v in value]
    elif isinstance(value, dict):
        return dict(
            [(normalized_msgpack(k), normalized_msgpack(v)) for (k, v) in value.items()]
        )

    if isinstance(value, int):
        if -(1 << 63) < value < (1 << 64):
            return value
        else:
            return str(value)
    else:
        return value


def get_or_else(hashmap, key, default_value=None):
    """ Get value or default value
    
    It differs from the standard dict ``get`` method in its behaviour when
    `key` is present but has a value that is an empty string or a string of
    only spaces.

    Args:
        hashmap (dict): target
        key (Any): key
        default_value (Any): default value
    
    Example:

        >>> get_or_else({'k': 'nonspace'}, 'k', 'default')
        'nonspace'
        >>> get_or_else({'k': ''}, 'k', 'default')
        'default'
        >>> get_or_else({'k': '    '}, 'k', 'default')
        'default'

    Returns:
        The value of `key` or `default_value`
    """
    value = hashmap.get(key)
    if value is None:
        return default_value
    else:
        if 0 < len(value.strip()):
            return value
        else:
            return default_value


def parse_date(s):
    """Parse date from str to datetime

    TODO: parse datetime using an optional format string
    
    For now, this does not use a format string since API may return date in ambiguous format :(

    Args:
       s (str): target str

    Returns:
       datetime
    """
    try:
        return dateutil.parser.parse(s)
    except ValueError:
        log.warning("Failed to parse date string: %s", s)
        return None


def normalize_connector_config(config):
    """Normalize connector config

    This is porting of TD CLI's ConnectorConfigNormalizer#normalized_config.
    see also: https://github.com/treasure-data/td/blob/15495f12d8645a7b3f6804098f8f8aca72de90b9/lib/td/connector_config_normalizer.rb#L7-L30

    Args:
        config (dict): A config to be normalized

    Returns:
        dict: Normalized configuration

    Examples:
        Only with ``in`` key in a config.
        >>> config = {"in": {"type": "s3"}}
        >>> normalize_connector_config(config)
        {'in': {'type': 's3'}, 'out': {}, 'exec': {}, 'filters': []}

        With ``in``, ``out``, ``exec``, and ``filters`` in a config.
        >>> config =  {
        ...     "in": {"type": "s3"},
        ...     "out": {"mode": "append"},
        ...     "exec": {"guess_plugins": ["json"]},
        ...     "filters": [{"type": "speedometer"}],
        ... }
        >>> normalize_connector_config(config)
        {'in': {'type': 's3'},
        'out': {'mode': 'append'},
        'exec': {'guess_plugins': ['json']},
        'filters': [{'type': 'speedometer'}]}
    """
    if "in" in config:
        return {
            "in": config["in"],
            "out": config.get("out", {}),
            "exec": config.get("exec", {}),
            "filters": config.get("filters", []),
        }
    elif "config" in config:
        if len(config) != 1:
            raise ValueError(
                "Setting sibling keys with 'config' key isn't support. "
                "Set within the 'config' key, or put all the settings without 'config'"
                "key."
            )

        return normalize_connector_config(config["config"])
    else:
        return {"in": config, "out": {}, "exec": {}, "filters": []}
