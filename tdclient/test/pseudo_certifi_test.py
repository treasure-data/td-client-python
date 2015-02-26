#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

try:
    from unittest import mock
except ImportError:
    import mock
import functools

from tdclient import pseudo_certifi as certifi

def exists(platform, path):
    if platform == "debian":
        if path == "/etc/ssl/certs/ca-certificates.crt":
            return True
    elif platform == "redhat":
        if path == "/etc/ssl/certs/ca-bundle.crt":
            return True
    return False

def test_certifi_debian():
    with mock.patch("os.path.exists") as os_path_exists:
        os_path_exists.side_effect = functools.partial(exists, "debian")
        assert certifi.where() == "/etc/ssl/certs/ca-certificates.crt"

def test_certifi_redhat():
    with mock.patch("os.path.exists") as os_path_exists:
        os_path_exists.side_effect = functools.partial(exists, "redhat")
        assert certifi.where() == "/etc/ssl/certs/ca-bundle.crt"

def test_certifi_unknown():
    with mock.patch("os.path.exists") as os_path_exists:
        os_path_exists.side_effect = functools.partial(exists, "unknown")
        assert certifi.where() is None
