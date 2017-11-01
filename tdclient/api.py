#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from array import array
try:
    import certifi
except ImportError:
    # on py27 on windows, it tries to import pseudo "certifi" module instead of original one.
    # to avoid the weird behavior, the pseudo "certifi" module should be named differently.
    from tdclient import pseudo_certifi as certifi
import codecs
import contextlib
import csv
import dateutil.parser
import email.utils
import gzip
import io
import json
import logging
import msgpack
import os
import six
import socket
import ssl
import tempfile
import time
try:
    import urllib.parse as urlparse # >=3.0
except ImportError:
    import urlparse
import urllib3
import warnings

from tdclient.access_control_api import AccessControlAPI
from tdclient.account_api import AccountAPI
from tdclient.bulk_import_api import BulkImportAPI
from tdclient.connector_api import ConnectorAPI
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
from tdclient import errors
from tdclient import version

log = logging.getLogger(__name__)

APIError = errors.APIError
AuthError = errors.AuthError
ForbiddenError = errors.ForbiddenError
AlreadyExistsError = errors.AlreadyExistsError
NotFoundError = errors.NotFoundError

class API(AccessControlAPI, AccountAPI, BulkImportAPI, ConnectorAPI, DatabaseAPI, ExportAPI, ImportAPI,
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

    def __init__(self, apikey=None, user_agent=None, endpoint=None, headers=None, retry_post_requests=False,
                 max_cumul_retry_delay=600, http_proxy=None, **kwargs):
        headers = {} if headers is None else headers
        if apikey is not None:
            self._apikey = apikey
        elif "TD_API_KEY" in os.environ:
            self._apikey = os.getenv("TD_API_KEY")
        else:
            raise(ValueError("no API key given"))

        if user_agent is not None:
            self._user_agent = user_agent
        else:
            self._user_agent = "TD-Client-Python/%s" % (version.__version__)

        if endpoint is not None:
            self._endpoint = endpoint
        elif os.getenv("TD_API_SERVER"):
            self._endpoint = os.getenv("TD_API_SERVER")
        else:
            self._endpoint = self.DEFAULT_ENDPOINT

        pool_options = dict(kwargs)
        certs = pool_options.get("ca_certs", certifi.where())
        if certs is not None:
            pool_options["ca_certs"] = certs
            pool_options["cert_reqs"] = ssl.CERT_REQUIRED

        if "connect_timeout" in pool_options or "read_timeout" in pool_options or "send_timeout" in pool_options:
            if "connect_timeout" in pool_options:
                warnings.warn("connect_timeout will be removed from future release. Please use timeout instead.", category=DeprecationWarning)
                connect_timeout = pool_options.pop("connect_timeout")
            else:
                connect_timeout = 0
            if "read_timeout" in pool_options:
                warnings.warn("read_timeout will be removed from future release. Please use timeout instead.", category=DeprecationWarning)
                read_timeout = pool_options.pop("read_timeout")
            else:
                read_timeout = 0
            if "send_timeout" in pool_options:
                warnings.warn("send_timeout will be removed from future release. Please use timeout instead.", category=DeprecationWarning)
                send_timeout = pool_options.pop("send_timeout")
            else:
                send_timeout = 0
            pool_options["timeout"] = max(connect_timeout, read_timeout, send_timeout)

        if "timeout" not in pool_options:
            pool_options["timeout"] = 60

        self.http = self._init_http(http_proxy if http_proxy else os.getenv("HTTP_PROXY"), **pool_options)
        self._retry_post_requests = retry_post_requests
        self._max_cumul_retry_delay = max_cumul_retry_delay
        self._headers = dict([ (key.lower(), value) for (key, value) in headers.items() ])

    @property
    def apikey(self):
        return self._apikey

    @property
    def endpoint(self):
        return self._endpoint

    def _init_http(self, http_proxy=None, **kwargs):
        if http_proxy is None:
            return urllib3.PoolManager(**kwargs)
        else:
            if http_proxy.startswith("http://"):
                return self._init_http_proxy(http_proxy, **kwargs)
            else:
                return self._init_http_proxy("http://%s" % (http_proxy,), **kwargs)

    def _init_http_proxy(self, http_proxy, **kwargs):
        pool_options = dict(kwargs)
        p = urlparse.urlparse(http_proxy)
        scheme = p.scheme
        netloc = p.netloc
        if "@" in netloc:
            auth, netloc = netloc.split("@", 2)
            pool_options["proxy_headers"] = urllib3.make_headers(proxy_basic_auth=auth)
        return urllib3.ProxyManager("%s://%s" % (scheme, netloc), **pool_options)

    def get(self, path, params=None, headers=None, **kwargs):
        headers = {} if headers is None else dict(headers)
        headers["accept-encoding"] = "deflate, gzip"
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST GET call:\n  headers: %s\n  path: %s\n  params: %s", repr(headers), repr(path), repr(params))

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        # for both exceptions and 500+ errors retrying is enabled by default.
        # The total number of retries cumulatively should not exceed 10 minutes / 600 seconds
        response = None
        while True:
            try:
                response = self.send_request("GET", url, fields=params, headers=headers, decode_content=True, preload_content=False)
                # retry if the HTTP error code is 500 or higher and we did not run out of retrying attempts
                if response.status < 500:
                    break
                else:
                    log.warn("Error %d: %s. Retrying after %d seconds... (cumulative: %d/%d)", response.status, response.data, retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds... (cumulative: %d/%d)", retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds. (cumulative: %d/%d)" % (self._max_cumul_retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)))

        log.debug("REST GET response:\n  headers: %s\n  status: %d\n  body: <omitted>", repr(dict(response.getheaders())), response.status)

        return contextlib.closing(response)

    def post(self, path, params=None, headers=None, **kwargs):
        headers = {} if headers is None else dict(headers)
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST POST call:\n  headers: %s\n  path: %s\n  params: %s", repr(headers), repr(path), repr(params))

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        # for both exceptions and 500+ errors retrying can be enabled by initialization
        # parameter 'retry_post_requests'. The total number of retries cumulatively
        # should not exceed 10 minutes / 600 seconds

        # use `params` as request parameter if it is a `dict`.
        # otherwise, use it as byte string of request body.
        body = fields = None
        if isinstance(params, dict):
            fields = params
        else:
            body = params

        response = None
        while True:
            try:
                response = self.send_request("POST", url, fields=fields, body=body, headers=headers, decode_content=True, preload_content=False)
                # if the HTTP error code is 500 or higher and the user requested retrying
                # on post request, attempt a retry
                if response.status < 500:
                    break
                else:
                    if not self._retry_post_requests:
                        raise(APIError("Retrying stopped by retry_post_requests == False"))
                    log.warn("Error %d: %s. Retrying after %d seconds... (cumulative: %d/%d)", response.status, response.data, retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                if not self._retry_post_requests:
                    raise(APIError("Retrying stopped by retry_post_requests == False"))

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds... (cumulative: %d/%d)", retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds. (cumulative: %d/%d)" % (self._max_cumul_retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)))

        log.debug("REST POST response:\n  headers: %s\n  status: %d\n  body: <omitted>", repr(dict(response.getheaders())), response.status)

        return contextlib.closing(response)

    def put(self, path, bytes_or_stream, size, headers=None, **kwargs):
        headers = {} if headers is None else dict(headers)
        headers["content-length"] = str(size)
        if "content-type" not in headers:
            headers["content-type"] = "application/octet-stream"
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST PUT call:\n  headers: %s\n  path: %s\n  body: <omitted>", repr(headers), repr(path))

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

        response = None
        try:
            response = self.send_request("PUT", url, body=stream, headers=headers, decode_content=True, preload_content=False)
            if response.status < 500:
                pass
            else:
                raise(APIError("Error %d: %s", response.status, response.data))
        except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
            raise(APIError("Error: %s" % (repr(response))))

        log.debug("REST PUT response:\n  headers: %s\n  status: %d\n  body: <omitted>", repr(dict(response.getheaders())), response.status)

        return contextlib.closing(response)

    def delete(self, path, params=None, headers=None, **kwargs):
        headers = {} if headers is None else dict(headers)
        url, headers = self.build_request(path=path, headers=headers, **kwargs)

        log.debug("REST DELETE call:\n  headers: %s\n  path: %s\n  params: %s", repr(headers), repr(path), repr(params))

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        # for both exceptions and 500+ errors retrying is enabled by default.
        # The total number of retries cumulatively should not exceed 10 minutes / 600 seconds
        response = None
        while True:
            try:
                response = self.send_request("DELETE", url, fields=params, headers=headers, decode_content=True, preload_content=False)
                # retry if the HTTP error code is 500 or higher and we did not run out of retrying attempts
                if response.status < 500:
                    break
                else:
                    log.warn("Error %d: %s. Retrying after %d seconds... (cumulative: %d/%d)", response.status, response.data, retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
            except ( urllib3.exceptions.TimeoutStateError, urllib3.exceptions.TimeoutError, urllib3.exceptions.PoolError, socket.error ):
                pass

            if cumul_retry_delay <= self._max_cumul_retry_delay:
                log.warn("Retrying after %d seconds... (cumulative: %d/%d)", retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)
                time.sleep(retry_delay)
                cumul_retry_delay += retry_delay
                retry_delay *= 2
            else:
                raise(APIError("Retrying stopped after %d seconds. (cumulative: %d/%d)" % (self._max_cumul_retry_delay, cumul_retry_delay, self._max_cumul_retry_delay)))

        log.debug("REST DELETE response:\n  headers: %s\n  status: %d\n  body: <omitted>", repr(dict(response.getheaders())), response.status)

        return contextlib.closing(response)

    def build_request(self, path=None, headers=None, endpoint=None):
        headers = {} if headers is None else headers
        if endpoint is None:
            endpoint = self._endpoint
        if path is None:
            url = endpoint
        else:
            p = urlparse.urlparse(endpoint)
            # should not use `os.path.join` since it returns path string like "/foo\\bar"
            request_path = path if p.path == "/" else "/".join([p.path, path])
            url = urlparse.urlunparse(urlparse.ParseResult(p.scheme, p.netloc, request_path, p.params, p.query, p.fragment))
        # use default headers first
        _headers = dict(self._headers)
        # add default headers
        _headers["authorization"] = "TD1 %s" % (self._apikey,)
        _headers["date"] = email.utils.formatdate(time.time())
        _headers["user-agent"] = self._user_agent
        # override given headers
        _headers.update(dict([ (key.lower(), value) for (key, value) in headers.items() ]))
        return (url, _headers)

    def send_request(self, method, url, fields=None, body=None, headers=None, **kwargs):
        def as_bytes(s, encoding):
            return s.encode(encoding) if isinstance(s, six.text_type) else s

        if six.PY2:
            # FIXME: Ugly workaround for `UnicodeDecodeError` from `httplib.HTTPConnection._send_output` when sending multi-byte payloads on Python 2.x (#27)
            method = as_bytes(method, 'utf-8')
            url = as_bytes(url, 'utf-8')
            if fields is not None:
                fields = dict([ (as_bytes(k, 'utf-8'), as_bytes(v, 'utf-8')) for k, v in fields.items() ])
            if headers is not None:
                headers = dict([ (as_bytes(k, 'utf-8'), as_bytes(v, 'utf-8')) for k, v in headers.items() ])
        if body is None:
            return self.http.request(method, url, fields=fields, headers=headers, **kwargs)
        else:
            return self.http.urlopen(method, url, body=body, headers=headers, **kwargs)

    def raise_error(self, msg, res, body):
        status_code = res.status
        s = body.decode("utf-8")
        if status_code == 404:
            raise errors.NotFoundError("%s: %s" % (msg, s))
        elif status_code == 409:
            raise errors.AlreadyExistsError("%s: %s" % (msg, s))
        elif status_code == 401:
            raise errors.AuthError("%s: %s" % (msg, s))
        elif status_code == 403:
            raise errors.ForbiddenError("%s: %s" % (msg, s))
        else:
            raise errors.APIError("%d: %s: %s" % (status_code, msg, s))

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
        warnings.warn("sleep(secs) will be removed from future release. Please use time.sleep(secs)", category=DeprecationWarning)
        time.sleep(secs)

    def parsedate(self, s):
        warnings.warn("parsedate(secs) will be removed from future release. Please use datetime.strptime(date_string, fmt) or other.", category=DeprecationWarning)
        try:
            return self._parsedate(s, None)
        except ValueError:
            log.warn("Failed to parse date string: %s" % (s,))
            return None

    def _parsedate(self, s, fmt):
        # TODO: parse datetime with using format string
        # for now, this ignores given format string since API may return date in ambiguous format :(
        try:
            return dateutil.parser.parse(s)
        except ValueError:
            log.warn("Failed to parse date string: %s as %s" % (s, fmt))
            return None

    def get_or_else(self, hashmap, key, default_value=None):
        value = hashmap.get(key)
        if value is None:
            return default_value
        else:
            if 0 < len(value.strip()):
                return value
            else:
                return default_value

    def close(self):
        # urllib3 doesn't allow to close all connections immediately.
        # all connections in pool will be closed eventually during gc.
        self.http.clear()

    def _prepare_file(self, file_like, fmt, **kwargs):
        fp = tempfile.TemporaryFile()
        with contextlib.closing(gzip.GzipFile(mode="wb", fileobj=fp)) as gz:
            packer = msgpack.Packer()
            with contextlib.closing(self._read_file(file_like, fmt, **kwargs)) as items:
                for item in items:
                    try:
                        mp = packer.pack(item)
                    except (OverflowError, msgpack.PackValueError):
                        packer.reset()
                        mp = packer.pack(normalized_msgpack(item))
                    gz.write(mp)
        fp.seek(0)
        return fp

    def _read_file(self, file_like, fmt, **kwargs):
        compressed = fmt.endswith(".gz")
        if compressed:
            fmt = fmt[0:len(fmt)-len(".gz")]
        reader_name = "_read_%s_file" % (fmt,)
        if hasattr(self, reader_name):
            reader = getattr(self, reader_name)
        else:
            raise TypeError("unknown format: %s" % (fmt,))
        if hasattr(file_like, "read"):
            if compressed:
                file_like = gzip.GzipFile(fileobj=file_like)
            return reader(file_like, **kwargs)
        else:
            if compressed:
                file_like = gzip.GzipFile(fileobj=open(file_like, "rb"))
            else:
                file_like = open(file_like, "rb")
            return reader(file_like, **kwargs)

    def _validate_record(self, record):
        if not any (k in record for k in ("time", b"time")):
            warnings.warn("records should have \"time\" column to import records properly.", category=RuntimeWarning)
        return True

    def _read_msgpack_file(self, file_like, **kwargs):
        # current impl doesn't torelate any unpack error
        unpacker = msgpack.Unpacker(file_like)
        for record in unpacker:
            self._validate_record(record)
            yield record

    def _read_json_file(self, file_like, **kwargs):
        # current impl doesn't torelate any JSON parse error
        for s in file_like:
            record = json.loads(s.decode("utf-8"))
            self._validate_record(record)
            yield record

    def _read_csv_file(self, file_like, dialect=csv.excel, columns=None, encoding="utf-8", **kwargs):
        try:
            unicode
            py2k = True
        except NameError:
            py2k = False
        # `csv` module bundled with py2k doesn't support `unicode` :(
        # https://docs.python.org/2/library/csv.html#examples
        def getreader(file_like):
            for s in codecs.getreader(encoding)(file_like):
                yield s.encode(encoding) if py2k else s
        def value(s):
            s = s.decode(encoding) if py2k else s
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
        if columns is None:
            reader = csv.DictReader(getreader(file_like), dialect=dialect)
            for row in reader:
                record = dict([ (k, value(v)) for (k, v) in row.items() ])
                self._validate_record(record)
                yield record
        else:
            reader = csv.reader(getreader(file_like), dialect=dialect)
            for row in reader:
                record = dict(zip(columns, [ value(col) for col in row ]))
                self._validate_record(record)
                yield record

    def _read_tsv_file(self, file_like, **kwargs):
        return self._read_csv_file(file_like, dialect=csv.excel_tab, **kwargs)

def normalized_msgpack(value):
    if isinstance(value, (list, tuple)):
        return [ normalized_msgpack(v) for v in value ]
    elif isinstance(value, dict):
        return dict([ (normalized_msgpack(k), normalized_msgpack(v)) for (k, v) in value.items() ])
    try:
        long
        py2k = True
    except NameError:
        py2k = False
    if py2k:
        if isinstance(value, long):
            return str(value)
        else:
            return value
    else:
        if isinstance(value, int):
            if -(1<<63) < value < (1<<64):
                return value
            else:
                return str(value)
        else:
            return value
