#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import email.utils
try:
    import httplib
except ImportError:
    import http.client as httplib
try:
    import json
except ImportError:
    import simplejson as json
import msgpack
import os
import socket
import sys
import time
import urllib
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
try:
    from urllib import quote as urlquote
except ImportError:
    from urllib.parse import quote as urlquote
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse
import zlib

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


class API(object):
    DEFAULT_ENDPOINT = "api.treasure-data.com"
    DEFAULT_IMPORT_ENDPOINT = "api-import.treasure-data.com"

    NEW_DEFAULT_ENDPOINT = "api.treasuredata.com"
    NEW_DEFAULT_IMPORT_ENDPOINT = "api-import.treasuredata.com"

    def __init__(self, apikey, user_agent=None, endpoint=None, **kwargs):
        self._apikey = apikey
        if user_agent is not None:
            self._user_agent = user_agent
        else:
            self._user_agent = "TD-Client-Python: 0.0.1" # FIXME: display proper version
        
        if endpoint is not None:
            endpoint = endpoint
        elif os.getenv("TD_API_SERVER"):
            endpoint = os.getenv("TD_API_SERVER")
        else:
            endpoint = DEFAULT_ENDPOINT
        
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
            host, port = endpoint.split(":", 2)
            if kwargs.has_key("ssl"):
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

    ####
    ## Database API
    ##

    # => [name:String]
    def list_databases(self):
        code, body, res = self.get("/v3/database/list")
        if code != 200:
            self.raise_error("List databases failed", res)
        js = self.checked_json(body, ["databases"])
        result = {}
        for m in js["databases"]:
            name = m.get("name")
            count = m.get("count")
            created_at = m.get("created_at")
            updated_at = m.get("updated_at")
            permission = m.get("permission")
            result[name] = [count, created_at, updated_at, None, permission] # set nil to org for API copatibility
        return result

    ####
    ## Table API
    ##

    # => {name:String => [type:Symbol, count:Integer]}
    def list_tables(self, db):
        code, body, res = self.get("/v3/table/list/%s" % (urlquote(str(db))))
        if code != 200:
            self.raise_error("List tables failed", res)
        js = self.checked_json(body, ["tables"])
        result = {}
        for m in js["tables"]:
            name = m.get("name")
            _type = m.get("type", "?")
            count = int(m.get("count", 0))
            created_at = m.get("created_at")
            updated_at = m.get("updated_at")
            last_import = m.get("counter_updated_at")
            last_log_timestamp = m.get("last_log_timestamp")
            estimated_storage_size = int(m.get("estimated_storage_size", 0))
            schema = json.loads(m.get("schema", "[]"))
            expire_days = m.get("expire_days")
            primary_key = m.get("primary_key")
            primary_key_type = m.get("primary_key_type")
            result[name] = [_type, schema, count, created_at, updated_at, estimated_storage_size, last_import, last_log_timestamp, expire_days, primary_key, primary_key_type]
        return result

    ####
    ## Job API
    ##

    # => [(jobId:String, type:Symbol, status:String, start_at:String, end_at:String, result_url:String)]
    def list_jobs(self, _from=0, to=None, status=None, conditions=None):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        if status is not None:
            params["status"] = str(status)
        if conditions is not None:
            params.update(conditions)
        code, body, res = self.get("/v3/job/list", params)
        if code != 200:
            self.raise_error("List jobs failed", res)
        js = self.checked_json(body, ["jobs"])
        result = []
        for m in js["jobs"]:
            job_id = m.get("job_id")
            _type = m.get("type", "?")
            database = m.get("database")
            status = m.get("status")
            query = m.get("query")
            start_at = m.get("start_at")
            end_at = m.get("end_at")
            cpu_time = m.get("cpu_time")
            result_size = m.get("result_size") # compressed result size in msgpack.gz format
            result_url = m.get("result")
            priority = m.get("priority")
            retry_limit = m.get("retry_limit")
            result.append([job_id, _type, status, query, start_at, end_at, cpu_time,
                 result_size, result_url, priority, retry_limit, None, database])
        return result

    # => (type:Symbol, status:String, result:String, url:String, result:String)
    def show_job(self, job_id):
        # use v3/job/status instead of v3/job/show to poll finish of a job
        code, body, res = self.get("/v3/job/show/%s" % (urlquote(str(job_id))))
        if code != 200:
            self.raise_error("Show job failed", res)
        js = self.checked_json(body, ["status"])
        _type = js.get("type", "?")
        database = js.get("database")
        query = js.get("query")
        status = js.get("status")
        debug = js.get("debug")
        url = js.get("url")
        start_at = js.get("start_at")
        end_at = js.get("end_at")
        cpu_time = js.get("cpu_time")
        result_size = js.get("result_size") # compressed result size in msgpack.gz format
        result = js.get("result") # result target URL
        hive_result_schema = js.get("hive_result_schema", "")
        if hive_result_schema is None or len(str(hive_result_schema)) < 1:
            hive_result_schema = None
        else:
            hive_result_schema = json.loads(hive_result_schema)
        priority = js.get("priority")
        retry_limit = js.get("retry_limit")
        return [_type, query, status, url, debug, start_at, end_at, cpu_time,
                result_size, result, hive_result_schema, priority, retry_limit, None, database]

    def job_status(self, job_id):
        code, body, res = self.get("/v3/job/status/%s" % (urlquote(str(job_id))))
        if code != 200:
            self.raise_error("Get job status failed", res)

        js = self.checked_json(body, ["status"])
        return js["status"]

    def job_result(self, job_id):
        code, body, res = self.get("/v3/job/result/%s" % (urlquote(str(job_id))), {"format": "msgpack"})
        if code != 200:
            self.raise_error("Get job result failed", res)
        result = []
        unpacker = msgpack.Unpacker(body)
        for row in unpacker:
            result.append(row)
        return result

    def job_result_raw(self, job_id, _format):
        code, body, res = self.get("/v3/job/result/%s" % (urlquote(str(job_id))), {"format": _format})
        if code != 200:
            self.raise_error("Get job result failed", res)
        return body

    def kill(self, job_id):
        code, body, res = post("/v3/job/kill/%s" % (urlquote(str(job_id))))
        if code != 200:
            self.raise_error("Kill job failed", res)
        js = self.checked_json(body, [])
        former_status = js.get("former_status")
        return former_status

    # => jobId:String
    def hive_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        return self.query(q, "hive", db, result_url, priority, retry_limit, **kwargs)

    # => jobId:String
    def pig_query(self, q, db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        return self.query(q, "pig", db, result_url, priority, retry_limit, **kwargs)

    # => jobId:String
    def query(self, q, _type="hive", db=None, result_url=None, priority=None, retry_limit=None, **kwargs):
        params = {"query": q}
        params.update(kwargs)
        if result_url is not None:
            params["result"] = result_url
        if priority is not None:
            params["priority"] = priority
        if retry_limit is not None:
            params["retry_limit"] = retry_limit
        code, body, res = self.post("/v3/job/issue/%s/%s" % (urlquote(str(_type)), urlquote(str(db))), params)
        if code != 200:
            self.raise_error("Query failed", res)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])

    ####
    ## Server Status API
    ##

    # => status:String
    def server_status(self):
        code, body, res = self.get("/v3/system/server_status")
        if code != 200:
            return "Server is down (%d)" % (code,)
        js = self.checked_json(body, ["status"])
        status = js["status"]
        return status

    def get(self, url, params={}):
        http, header = self.new_http()

        path = self._base_path + url
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
            except socket.error: # FIXME: need to handle sort of errors
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

        path = self._base_path + url
        if len(params) < 1:
            header["Content-Length"] = 0
        data = urlencode(params)

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                http.request("POST", path, data, header)
                response = http.getresponse()
                status = response.status
                if status < 500:
                    break
                else:
                    print("Error %d: %s. Retrying after %d seconds..." % (status, response.reason, retry_delay), file=sys.stderr)
            except socket.error: # FIXME: need to handle sort of errors
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
        if status_code == 404:
            raise NotFoundError("%s: %s", (msg, body))
        elif status_code == 409:
            raise AlreadyExistsError("%s: %s", (msg, body))
        elif status_code == 401:
            raise AuthError("%s: %s", (msg, body))
        elif status_code == 403:
            raise ForbiddenError("%s: %s", (msg, body))
        else:
            raise APIError("%d: %s: %s", (status_code, msg, body))

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
