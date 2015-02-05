#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from array import array
try:
    import certifi
    ca_certs = certifi.where
except ImportError:
    def ca_certs():
        certs = [
            "/etc/ssl/certs/ca-certificates.crt", # debian
            "/etc/ssl/certs/ca-bundle.crt", # redhat
        ]
        for cert in certs:
            if os.path.exists(cert):
                return cert
        return None
import contextlib
import dateutil.parser
import email.utils
import io
import json
import logging
import os
import socket
import ssl
import sys
import time
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

log = logging.getLogger(__name__)

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
    """Internal API class

    Args:
        apikey (str): the API key of Treasure Data Service. If `None` is given, `TD_API_KEY` will be used if available.
        user_agent (str): custom User-Agent.
        endpoint (str): custom endpoint URL. If `None` is given, `TD_API_SERVER` will be used if available.
        headers (dict): custom HTTP headers.
        retry_post_requests (bool): Specify whether allowing API client to retry POST requests. `False` by default.
        max_cumul_retry_delay (int): maximum retry limit in seconds. 600 seconds by default.
        http_proxy (str): HTTP proxy setting. if `None` is given, `HTTP_PROXY` will be used if available.
    """

    DEFAULT_ENDPOINT = "https://api.treasuredata.com/"
    DEFAULT_IMPORT_ENDPOINT = "https://api-import.treasuredata.com/"

    def __init__(self, apikey=None, user_agent=None, endpoint=None, headers={}, retry_post_requests=False,
                 max_cumul_retry_delay=600, http_proxy=None, **kwargs):
        if apikey is not None:
            self._apikey = apikey
        elif "TD_API_KEY" in os.environ:
            self._apikey = os.getenv("TD_API_KEY")
        else:
            raise(ValueError("no API key given"))

        if user_agent is not None:
            self._user_agent = user_agent
        else:
            self._user_agent = "TD-Client-Python: %s" % (version.__version__)

        if endpoint is not None:
            self._endpoint = endpoint
        elif os.getenv("TD_API_SERVER"):
            self._endpoint = os.getenv("TD_API_SERVER")
        else:
            self._endpoint = self.DEFAULT_ENDPOINT

        pool_options = dict(kwargs)
        certs = pool_options.get("ca_certs", ca_certs())
        if certs is not None:
            pool_options["ca_certs"] = certs
            pool_options["cert_reqs"] = ssl.CERT_REQUIRED

        if "connect_timeout" in pool_options or "read_timeout" in pool_options or "send_timeout" in pool_options:
            connect_timeout = pool_options.pop("connect_timeout") if "connect_timeout" in pool_options else 0
            read_timeout = pool_options.pop("read_timeout") if "read_timeout" in pool_options else 0
            send_timeout = pool_options.pop("send_timeout") if "send_timeout" in pool_options else 0
            pool_options["timeout"] = connect_timeout + read_timeout + send_timeout

        if http_proxy is None and "HTTP_PROXY" in os.environ:
            http_proxy = os.getenv("HTTP_PROXY")

        if http_proxy is None:
            self.http = urllib3.PoolManager(**pool_options)
        else:
            if http_proxy.startswith("http://"):
                self.http = urllib3.ProxyManager(http_proxy, **pool_options)
            else:
                self.http = urllib3.ProxyManager("http://%s" % http_proxy, **pool_options)

        self._retry_post_requests = retry_post_requests
        self._max_cumul_retry_delay = max_cumul_retry_delay
        self._headers = { key.lower(): value for (key, value) in headers.items() }

    @property
    def apikey(self):
        return self._apikey

    @property
    def endpoint(self):
        return self._endpoint

    def get(self, path, params={}, **kwargs):
        headers = {
            "accept-encoding": "deflate, gzip",
        }
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST GET call:\n  headers: %s\n  path: %s\n  params: %s" % (repr(headers), repr(path), repr(params)))

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        # for both exceptions and 500+ errors retrying is enabled by default.
        # The total number of retries cumulatively should not exceed 10 minutes / 600 seconds
        response = None
        while True:
            try:
                response = self.http.request("GET", url, fields=params, headers=headers, decode_content=True, preload_content=False)
                # retry if the HTTP error code is 500 or higher and we did not run out of retrying attempts
                if response.status < 500:
                    break
                else:
                    log.warn("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay))
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds..." % (retry_delay))
                self.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        log.debug("REST GET response:\n  headers: %s\n  status: %d\n  body: <omitted>" % (repr(dict(response.getheaders())), response.status))

        return contextlib.closing(response)

    def post(self, path, params={}, **kwargs):
        url, headers = self.build_request(path=path, headers={}, **kwargs)

        log.debug("REST POST call:\n  headers: %s\n  path: %s\n  params: %s" % (repr(headers), repr(path), repr(params)))

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        # for both exceptions and 500+ errors retrying can be enabled by initialization
        # parameter 'retry_post_requests'. The total number of retries cumulatively
        # should not exceed 10 minutes / 600 seconds
        response = None
        while True:
            try:
                response = self.http.request("POST", url, fields=params, headers=headers, decode_content=True, preload_content=False)
                # if the HTTP error code is 500 or higher and the user requested retrying
                # on post request, attempt a retry
                if response.status < 500:
                    break
                else:
                    if not self._retry_post_requests:
                        raise(APIError("Retrying stopped by retry_post_requests == False"))
                    log.warn("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay))
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                if self._retry_post_requests:
                    raise(APIError("Retrying stopped by retry_post_requests == False"))

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds..." % (retry_delay))
                self.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        log.debug("REST POST response:\n  headers: %s\n  status: %d\n  body: <omitted>" % (repr(dict(response.getheaders())), response.status))

        return contextlib.closing(response)

    def put(self, path, bytes_or_stream, size, **kwargs):
        headers = {}
        headers["content-length"] = str(size)
        headers["content-type"] = "application/octet-stream"
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST PUT call:\n  headers: %s\n  path: %s\n  body: <omitted>" % (repr(headers), repr(path)))

        if hasattr(bytes_or_stream, "read"):
            # file-like must support `read` and `fileno` to work with `httplib`
            fileno_supported = hasattr(bytes_or_stream, "fileno")
            if fileno_supported:
                try:
                    bytes_or_stream.fileno()
                except io.UnsupportedOperation:
                    # `io.BytesIO` doesn't support `fileno`
                    fileno_supported = False
            if fileno_supported:
                stream = bytes_or_stream
            else:
                stream = array(str("b"), bytes_or_stream.read())
            
        else:
            # send request body as an `array.array` since `httplib` requires the request body to be a unicode string
            stream = array(str("b"), bytes_or_stream)

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                response = self.http.urlopen("PUT", url, body=stream, headers=headers, decode_content=True, preload_content=False)
                if response.status < 500:
                    break
                else:
                    log.warn("Error %d: %s. Retrying after %d seconds..." % (response.status, response.data, retry_delay))
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds..." % (retry_delay))
                self.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds." % (self._max_cumul_retry_delay)))

        log.debug("REST PUT response:\n  headers: %s\n  status: %d\n  body: <omitted>" % (repr(dict(response.getheaders())), response.status))

        return contextlib.closing(response)

    def build_request(self, path=None, headers={}, endpoint=None):
        if endpoint is None:
            endpoint = self._endpoint
        if path is None:
            url = endpoint
        else:
            p = urlparse.urlparse(endpoint)
            url = urlparse.urlunparse(urlparse.ParseResult(p.scheme, p.netloc, os.path.join(p, path), p.params, p.query, p.fragment))
        # use default headers first
        _headers = dict(self._headers)
        # add default headers
        _headers["authorization"] = "TD1 %s" % (self._apikey,)
        _headers["date"] = email.utils.formatdate(time.time())
        _headers["user-agent"] = self._user_agent
        # override given headers
        _headers.update({ key.lower(): value for (key, value) in headers.items() })
        return (url, _headers)

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
            raise APIError("Unexpected API response: %s: %s" % (error, repr(body)))
        js = dict(js)
        if 0 < [ k in js for k in required ].count(False):
            missing = [ k for k in required if k not in js ]
            raise APIError("Unexpected API response: %s: %s" % (repr(missing), repr(body)))
        return js

    def sleep(self, secs):
        time.sleep(secs)

    def parsedate(self, s):
        # TODO: parse datetime with specific format
        # for now, this parses string without format since API may return ambiguous value :(
        return dateutil.parser.parse(s)

    def get_or_else(self, hashmap, key, default_value=None):
        value = hashmap.get(key)
        return default_value if value is None else value

    def close(self):
        # urllib3 doesn't allow to close all connections immediately.
        # all connections in pool will be closed eventually during gc.
        self.http.clear()
