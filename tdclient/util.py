import io
from urllib.parse import quote as urlquote

import msgpack


def create_url(tmpl, **values):
    """Create url with values

    Args:
        tmpl (str): url template
        values (dict): values for url
    """
    quoted_values = {k: urlquote(str(v)) for k, v in values.items()}
    return tmpl.format(**quoted_values)


def parse_csv_value(s):
    """Parse and convert value to suitable types

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
    from tdclient.api import normalized_msgpack

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
