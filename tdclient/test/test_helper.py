#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

class Response(object):
    def __init__(self, status, body, headers):
        self.status = status
        self.body = body.encode("utf-8")
        self.headers = headers
        self.request_method = None
        self.request_path = None
        self.request_headers = None

def get(response, url, params={}):
    response.request_method = "GET"
    response.request_path = url
    response.request_headers = params
    return (response.status, response.body, response)
