#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import gzip
import io
import json
import msgpack
try:
    from unittest import mock
except ImportError:
    import mock
import os
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

def msgpackb(list):
    """list -> bytes"""
    stream = io.BytesIO()
    packer = msgpack.Packer()
    for item in list:
        stream.write(packer.pack(item))
    return stream.getvalue()

def msgunpackb(bytes):
    """bytes -> list"""
    unpacker = msgpack.Unpacker(io.BytesIO(bytes), encoding=str("utf-8"))
    return list(unpacker)

def jsonb(list):
    """list -> bytes"""
    stream = io.BytesIO()
    for item in list:
        stream.write(json.dumps(item).encode("utf-8"))
        stream.write(b"\n")
    return stream.getvalue()

def unjsonb(bytes):
    """bytes -> list"""
    return [ json.loads(s.decode("utf-8")) for s in bytes.splitlines() ]
    return list(unpacker)

def gzipb(bytes):
    """bytes -> bytes"""
    compress = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    return compress.compress(bytes) + compress.flush()

def gunzipb(bytes):
    """bytes -> bytes"""
    decompress = zlib.decompressobj(zlib.MAX_WBITS | 16)
    return decompress.decompress(bytes) + decompress.flush()
