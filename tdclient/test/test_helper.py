#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import os

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

class response(object):
    def __init__(self, code, body, headers={}):
        self.status = code
        self.body = body
        self.headers = { key.lower(): value for (key, value) in headers.items() }
        self.pos = 0

    def read(self, amt=None):
        if amt is None:
            self.pos = len(self.body)
            return self.body
        else:
            body = self.body[self.pos:self.pos+amt]
            self.pos += amt
            return body

    def getheader(self, header):
        return self.headers.get(header.lower())

    def getheaders(self):
        return self.headers

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass
