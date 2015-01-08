#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import contextlib
import email.utils
import json
import os
import socket
import sys
import time
try:
    from urllib.parse import urlencode # >=3.0
except ImportError:
    from urllib import urlencode
try:
    import urllib.parse as urlparse # >=3.0
except ImportError:
    import urlparse
import urllib3
import zlib

from tdclient.access_control_api import AccessControlAPI
from tdclient.account_api import AccountAPI
from tdclient.bulk_import_api import BulkImportAPI
from tdclient.database_api import DatabaseAPI
from tdclient.export_api import ExportAPI
from tdclient.import_api import ImportAPI
from tdclient.job_api import JobAPI
from tdclient.partial_delete_api import PartialDeleteAPI
from tdclient.result_api import ResultAPI
from tdclient.schedule_api import ScheduleAPI
from tdclient.server_status_api import ServerStatusAPI
from tdclient.table_api import TableAPI
from tdclient.user_api import UserAPI
from tdclient import version

class ParameterValidationError(Exception):
    pass

# Generic API error
class APIError(Exception):
    pass

# 401 API errors
class AuthError(APIError):
    pass

# 403 API errors, used for database permissions
class ForbiddenError(APIError):
    pass

# 409 API errors
class AlreadyExistsError(APIError):
    pass

# 404 API errors
class NotFoundError(APIError):
    pass

class API(AccessControlAPI, AccountAPI, BulkImportAPI, DatabaseAPI, ExportAPI, ImportAPI,
          JobAPI, PartialDeleteAPI, ResultAPI, ScheduleAPI, ServerStatusAPI, TableAPI, UserAPI):
    DEFAULT_ENDPOINT = "https://api.treasuredata.com/"
    DEFAULT_IMPORT_ENDPOINT = "https://api-import.treasuredata.com/"

    def __init__(self, apikey=None, user_agent=None, endpoint=None, headers={}, **kwargs):
        if apikey is not None:
            self._apikey = apikey
        elif os.getenv("TD_API_KEY"):
            self._apikey = os.getenv("TD_API_KEY")
        else:
            raise(RuntimeError("no API key given"))

        if user_agent is not None:
            self._user_agent = user_agent
        else:
            self._user_agent = "TD-Client-Python: %s" % (version.__version__)

        if endpoint is not None:
            self._endpoint = urlparse.urlparse(endpoint)
        elif os.getenv("TD_API_SERVER"):
            self._endpoint = urlparse.urlparse(os.getenv("TD_API_SERVER"))
        else:
            self._endpoint = urlparse.urlparse(self.DEFAULT_ENDPOINT)

        self._connect_timeout = kwargs.get("connect_timeout", 60)
        self._read_timeout = kwargs.get("read_timeout", 600)
        self._send_timeout = kwargs.get("send_timeout", 600)
        self._retry_post_requests = kwargs.get("retry_post_requests", False)
        self._max_cumul_retry_delay = kwargs.get("max_cumul_retry_delay", 600)

        http_proxy = kwargs.get("http_proxy", os.getenv("HTTP_PROXY"))
        if http_proxy is None:
            self.http = urllib3.PoolManager()
        else:
            if http_proxy.startswith("http://"):
                self.http = urllib3.ProxyManager(http_proxy)
            else:
                self.http = urllib3.ProxyManager("http://%s" % http_proxy)

        self._headers = { key.lower(): value for (key, value) in headers.items() }

    @property
    def apikey(self):
        return self._apikey

    def get(self, request_path, params={}):
        url = self.request_url(request_path)
        headers = self.request_headers({})
        headers["accept-encoding"] = "deflate, gzip"

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                response = self.http.request("GET", url, fields=params, headers=headers)
                if response.status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay), file=sys.stderr)
            except ( urllib3.TimeoutStateError, urllib3.TimeoutError, urllib3.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        return contextlib.closing(response)


    def post(self, request_path, params={}):
        url = self.request_url(request_path)
        headers = self.request_headers({})

        if len(params) < 1:
            headers["content-length"] = 0

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                response = self.http.request("POST", url, fields=params, headers=headers)
                if response.status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay), file=sys.stderr)
            except ( urllib3.TimeoutStateError, urllib3.TimeoutError, urllib3.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        return contextlib.closing(response)

    def put(self, request_path, stream, size):
        url = self.request_url(request_path)
        headers = self.request_headers({})

        header["content-type"] = "application/octet-stream"
        header["content-length"] = str(size)

        body = stream.read() if hasattr(stream, "read") else stream

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                response = self.http.urlopen("PUT", url, body=body, headers=headers)
                if response.status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay), file=sys.stderr)
            except ( urllib3.TimeoutStateError, urllib3.TimeoutError, urllib3.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        return contextlib.closing(response)

    def request_url(self, path=""):
        url = urlparse.ParseResult(self._endpoint.scheme, self._endpoint.netloc, os.path.join(self._endpoint.path, path),
                                   self._endpoint.params, self._endpoint.query, self._endpoint.fragment)
        return urlparse.urlunparse(url)

    def request_headers(self, headers={}):
        # use default headers first
        hs = dict(self._headers)
        # add default headers
        hs["authorization"] = "TD1 %s" % (self._apikey,)
        hs["date"] = email.utils.formatdate(time.time())
        hs["user-agent"] = self._user_agent
        # override given headers
        hs.update({ key.lower(): value for (key, value) in headers.items() })
        return hs

    def raise_error(self, msg, res, body):
        status_code = res.status
        s = body.decode("utf-8")
        if status_code == 404:
            raise NotFoundError("%s: %s" % (msg, s))
        elif status_code == 409:
            raise AlreadyExistsError("%s: %s" % (msg, s))
        elif status_code == 401:
            raise AuthError("%s: %s" % (msg, s))
        elif status_code == 403:
            raise ForbiddenError("%s: %s" % (msg, s))
        else:
            raise APIError("%d: %s: %s" % (status_code, msg, s))

    def checked_json(self, body, required):
        js = None
        try:
            js = json.loads(body.decode("utf-8"))
        except ValueError as error:
            raise RuntimeError("Unexpected API response: %s" % (error))
        js = dict(js)
        if 0 < [ k in js for k in required ].count(False):
            raise RuntimeError("Unexpected API response: %s" % (body))
        return js
