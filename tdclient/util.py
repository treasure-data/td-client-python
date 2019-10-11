from urllib.parse import quote as urlquote


def create_url(tmpl, **values):
    """Create url with values

    Args:
        tmpl (str): url template
        values (dict): values for url
    """
    quoted_values = {k: urlquote(str(v)) for k, v in values.items()}
    return tmpl.format(**quoted_values)


def csv_value(s):
    """Convert value from csv suitably

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
