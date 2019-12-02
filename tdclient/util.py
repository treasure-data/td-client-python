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
    """All input records should contain a column named "time".

    Since we represent records internally as dictionaries, this means
    all record *dictionaries* should contain a key called "time".
    """
    if not any(k in record for k in ("time", b"time")):
        warnings.warn(
            'records should have "time" column to import records properly.',
            category=RuntimeWarning,
        )
    return True


def guess_csv_value(s):
    """Given a (string) CSV value, try to guess its type and return it

    Tries to interpret s. It tries each interpretation in turn, and returns
    the first that succeeds:

    1. As an integer
    2. As a floating point value
    3. If it is "false" or "true" (case insensitive), then as a boolean
    4. If it is "" or "none" or "null" (case insensitive), then as None
    5. As the string itself, unaltered

    Args:
        s (str): value on csv
    Returns:
        Suitable value (int, float, str, bool or None)
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
    """Generate a merged converters dictionary from the user supplied dicts.
    """
    our_converters = {}
    if dtypes is not None:
        try:
            for column_name, dtype in dtypes.items():
                our_converters[column_name] = DTYPE_TO_CALLABLE[dtype]
        except KeyError as e:
            raise ValueError('Unrecognized dtype %r, must be one of %s' % (
                dtype, ", ".join(repr(k) for k in sorted(DTYPE_TO_CALLABLE))))
    if converters is not None:
        for column_name, parse_fn in converters.items():
            our_converters[column_name] = parse_fn
    return our_converters


def parse_csv_value(k, s, converters=None):
    """
    """
    if converters is None:
        parse_fn = guess_csv_value
    else:
        parse_fn = converters.get(k, guess_csv_value)
    return parse_fn(s)


def csv_dict_record_reader(file_like, encoding, dialect):
    """Yield records from a CSV "file" using csv.DictReader.

    Args:
        file_like: acts like an instance of io.BufferedIOBase, returning
            bytes when it is read from.
        encoding (str): then name of the encoding to use when turning those
            bytes into strings.
        dialect (str or None): the name of the CSV dialect to use, or None.

    Yields:
        For each CSV row, yields a dictionary whose keys are column names
        (determined by the first row in the CSV data) and whose values are
        column values.
    """
    reader = csv.DictReader(
        io.TextIOWrapper(file_like, encoding), dialect=dialect
    )
    for row in reader:
        yield row


def csv_text_record_reader(file_like, encoding, dialect, columns):
    """Yield records from a CSV "file" using csv.reader and given column names.

    Args:
        file_like: acts like an instance of io.BufferedIOBase, returning
            bytes when it is read from.
        encoding (str): then name of the encoding to use when turning those
            bytes into strings.
        dialect (str or None): the name of the CSV dialect to use, or None.

    Yields:
        For each CSV row, yields a dictionary whose keys are column names
        (determined by `columns`) and whose values are column values.
    """
    reader = csv.reader(
        io.TextIOWrapper(file_like, encoding), dialect=dialect
    )
    for row in reader:
        yield dict(zip(columns, row))


def read_csv_records(csv_reader, dtypes=None, converters=None, **kwargs):
    """Read records using csv_reader and yield the results.

    """
    our_converters = merge_dtypes_and_converters(dtypes, converters)

    for row in csv_reader:
        record = {
            k: parse_csv_value(k, v, our_converters) for (k, v) in row.items()
        }
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
        ``b'\x83\xa1a\x01\xa1b\x02\xa4time\xce]\xa5X\xa1\x83\xa1a\x03\xa1b\x06\xa4time\xce]\xa5X\xa1'``
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
    """Convert int to str if overflow

    Args:
        value (int, float, str, bool or None): value to be normalized

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
    Args:
        hashmap (dict): target
        key (Any): key
        default_value (Any): default value
    Returns:
        value of key or default_value
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
    """Parse date from str to datetime using fmt

    TODO: parse datetime with using format string
    for now, this ignores given format string since API may return date in ambiguous format :(

    Args:
       s (str): target str
       fmt (str): format for datetime
    Returns:
       datetime
    """
    try:
        return dateutil.parser.parse(s)
    except ValueError:
        log.warning("Failed to parse date string: %s", s)
        return None
