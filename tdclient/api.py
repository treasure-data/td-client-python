#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import email.utils
try:
    import http.client as httplib # >=3.0
except ImportError:
    import httplib
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

    def __init__(self, apikey=None, user_agent=None, endpoint=None, **kwargs):
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
            endpoint = endpoint
        elif os.getenv("TD_API_SERVER"):
            endpoint = os.getenv("TD_API_SERVER")
        else:
            endpoint = self.DEFAULT_ENDPOINT

        uri = urlparse.urlparse(endpoint)

        self._connect_timeout = kwargs.get("connect_timeout", 60)
        self._read_timeout = kwargs.get("read_timeout", 600)
        self._send_timeout = kwargs.get("send_timeout", 600)
        self._retry_post_requests = kwargs.get("retry_post_requests", False)
        self._max_cumul_retry_delay = kwargs.get("max_cumul_retry_delay", 600)

        if uri.scheme == "http" or uri.scheme == "https":
            self._host = uri.hostname
            if uri.port is not None:
                self._port = uri.port
            else:
                self._port = 443 if uri.scheme == "https" else 80
            # the opts[:ssl] option is ignored here, it's value
            #   overridden by the scheme of the endpoint URI
            self._ssl = (uri.scheme == "https")
            self._base_path = uri.path

        else:
            if uri.port:
                # invalid URI
                raise ValueError("Invalid endpoint: %s" % (endpoint))

            # generic URI
            port = None
            if 0 < endpoint.find(":"):
                host, _port = endpoint.split(":", 2)
                port = int(_port)
            else:
                host = endpoint
            if "ssl" in kwargs:
                if port is None:
                    port = 443
                self._ssl = True
            else:
                if port is None:
                    port = 80
                self._ssl = False
            self._host = host
            self._port = port
            self._base_path = ""

        self._http_proxy = kwargs.get("http_proxy", os.getenv("HTTP_PROXY"))
        if self._http_proxy is not None:
            http_proxy = self._http_proxy
            if http_proxy.startswith("http://"):
                http_proxy = http_proxy[7:]
            if http_proxy.endswith("/"):
                http_proxy = http_proxy[0:len(http_proxy)-1]
            self._http_proxy = http_proxy

        self._headers = kwargs.get("headers", {})

    @property
    def apikey(self):
        return self._apikey

    def get(self, url, params={}):
        http, header = self.new_http()

        path = os.path.join(self._base_path, url)
        if 0 < len(params):
            path += "?" + urlencode(params)

        header["Accept-Encoding"] = "deflate, gzip"

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                http.request("GET", path, headers=header)
                response = http.getresponse()
                status = response.status
                if status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (status, response.reason, retry_delay), file=sys.stderr)
            except ( httplib.NotConnected, httplib.IncompleteRead, httplib.CannotSendRequest, httplib.CannotSendHeader,
                     httplib.ResponseNotReady, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        body = response.read()
        ce = response.getheader("content-encoding")
        if ce is not None:
            if ce == "gzip":
                body = zlib.decompress(body, zlib.MAX_WBITS + 16)
            else:
                body = zlib.decompress(body)

        try:
            http.close()
        except:
            pass

        return (response.status, body, response)


    def post(self, url, params={}):
        http, header = self.new_http()

        path = os.path.join(self._base_path, url)
        if len(params) < 1:
            header["Content-Length"] = 0
        data = urlencode(params)

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                http.request("POST", path, data, headers=header)
                response = http.getresponse()
                status = response.status
                if status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (status, response.reason, retry_delay), file=sys.stderr)
            except ( httplib.NotConnected, httplib.IncompleteRead, httplib.CannotSendRequest, httplib.CannotSendHeader,
                     httplib.ResponseNotReady, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        body = response.read()

        try:
            http.close()
        except:
            pass

        return (response.status, body, response)

    def put(self, url, stream, size):
        http, header = self.new_http()

        header["Content-Type"] = "application/octet-stream"
        header["Content-Length"] = str(size)

        if hasattr(stream, "read"):
            data = stream.read()
        else:
            data = stream

        path = os.path.join(self._base_path, url)

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                http.request("PUT", path, data, headers=header)
                response = http.getresponse()
                status = response.status
                if status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (status, response.reason, retry_delay), file=sys.stderr)
            except ( httplib.NotConnected, httplib.IncompleteRead, httplib.CannotSendRequest, httplib.CannotSendHeader,
                     httplib.ResponseNotReady, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                print("Retrying after %d seconds..." % (retry_delay), file=sys.stderr)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(RuntimeError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        body = response.read()

        try:
            http.close()
        except:
            pass

        return (response.status, body, response)


    def new_http(self, host=None, **kwargs):
        if host is None:
            host = self._host
        if self._ssl:
            http = httplib.HTTPSConnection(host, self._port, timeout=60)
        else:
            http = httplib.HTTPConnection(host, self._port, timeout=60)
            if self._http_proxy is not None:
                proxy_host, _proxy_port = self._http_proxy.split(":", 2)
                proxy_port = int(_proxy_port) if _proxy_port is not None else 80
                http.set_tunnel(proxy_host, proxy_port)
        headers = {}
        if self._apikey is not None:
            headers["Authorization"] = "TD1 %s" % (self._apikey,)
        headers["Date"] = email.utils.formatdate(time.time())
        headers["User-Agent"] = self._user_agent
        headers.update(self._headers)
        return (http, headers)

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
