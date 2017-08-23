#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import codecs
import contextlib
import csv
import gzip
import io
import json
import msgpack
try:
    from unittest import mock
except ImportError:
    import mock
import os
from tdclient.api import normalized_msgpack
import zlib

def unset_environ():
    try:
        del os.environ["TD_API_KEY"]
    except KeyError:
        pass
    try:
        del os.environ["TD_API_SERVER"]
    except KeyError:
        pass
    try:
        del os.environ["HTTP_PROXY"]
    except KeyError:
        pass

def make_raw_response(status, body, headers={}):
    response = mock.MagicMock()
    response.status = status
    response.pos = 0
    response.body = body
    def read(size=None):
        if response.pos < len(response.body):
            if size is None:
                s = body[response.pos:]
                response.pos = len(response.body)
            else:
                s = response.body[response.pos:response.pos+size]
                response.pos += size
            return s
        else:
            return b""
    response.read.side_effect = read
    return response

def make_response(*args, **kwargs):
    response = make_raw_response(*args, **kwargs)
    return contextlib.closing(response)

def msgpackb(lis):
    """list -> bytes"""
    stream = io.BytesIO()
    packer = msgpack.Packer()
    for item in lis:
        try:
            mp = packer.pack(item)
        except (OverflowError, msgpack.PackValueError):
            packer.reset()
            mp = packer.pack(normalized_msgpack(item))
        stream.write(mp)
    return stream.getvalue()

def msgunpackb(bytes):
    """bytes -> list"""
    unpacker = msgpack.Unpacker(io.BytesIO(bytes), encoding=str("utf-8"))
    return list(unpacker)

def jsonb(lis):
    """list -> bytes"""
    stream = io.BytesIO()
    for item in lis:
        stream.write(json.dumps(item).encode("utf-8"))
        stream.write(b"\n")
    return stream.getvalue()

def unjsonb(bytes):
    """bytes -> list"""
    return [ json.loads(s.decode("utf-8")) for s in bytes.splitlines() ]

def value(s):
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

def csvb(lis, columns=[], dialect=csv.excel, encoding="utf-8"):
    """list -> bytes"""
    stream = io.BytesIO()
    writer = csv.writer(codecs.getwriter(encoding)(stream), dialect=dialect)
    for item in lis:
        writer.writerow([ item.get(column) for column in columns ])
    return stream.getvalue()

def uncsvb(bytes, columns=[], dialect=csv.excel, encoding="utf-8"):
    """bytes -> list"""
    stream = bytes
    reader = csv.reader(io.StringIO(bytes.decode(encoding)), dialect=dialect)
    return [ dict(zip(columns, [ value(column) for column in row ])) for row in reader ]

def dcsvb(lis, dialect=csv.excel, encoding="utf-8"):
    """list -> bytes"""
    cols = lis[0].keys()
    stream = io.BytesIO()
    writer = csv.DictWriter(codecs.getwriter(encoding)(stream), cols, dialect=dialect)
    if hasattr(writer, "writeheader"):
        writer.writeheader()
    else:
        writer.writerow(dict(zip(cols, cols)))
    for item in lis:
        writer.writerow(item)
    return stream.getvalue()

def undcsvb(bytes, dialect=csv.excel, encoding="utf-8"):
    """bytes -> list"""
    stream = bytes
    reader = csv.DictReader(io.StringIO(bytes.decode(encoding)), dialect=dialect)
    return [ dict([ (k, value(v)) for (k, v) in row.items() ]) for row in reader ]

def tsvb(lis, columns=[], encoding="utf-8"):
    """bytes -> list"""
    return csvb(lis, columns=columns, dialect=csv.excel_tab, encoding=encoding)

def untsvb(bytes, columns=[], encoding="utf-8"):
    """bytes -> list"""
    return uncsvb(bytes, columns=columns, dialect=csv.excel_tab, encoding=encoding)

def dtsvb(lis, encoding="utf-8"):
    """bytes -> list"""
    return dcsvb(lis, dialect=csv.excel_tab, encoding=encoding)

def undtsvb(bytes, encoding="utf-8"):
    """bytes -> list"""
    return undcsvb(bytes, dialect=csv.excel_tab, encoding=encoding)

def gzipb(bytes):
    """bytes -> bytes"""
    compress = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compress.compress(bytes) + compress.flush()

def gunzipb(bytes):
    """bytes -> bytes"""
    decompress = zlib.decompressobj(zlib.MAX_WBITS | 16)
    return decompress.decompress(bytes) + decompress.flush()
