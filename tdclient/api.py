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


class API(object):
    DEFAULT_ENDPOINT = "https://api.treasuredata.com/"
    DEFAULT_IMPORT_ENDPOINT = "https://api-import.treasuredata.com/"

    def __init__(self, apikey, user_agent=None, endpoint=None, **kwargs):
        self._apikey = apikey
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

    ####
    ## Access Control API
    ##
    def grant_access_control(self, subject, action, scope, grant_option):
        params = {"subject": subject, "action": action, "scope": scope, "grant_option": str(grant_option)}
        code, body, res = self.post("/v3/acl/grant", params)
        if code != 200:
            self.raise_error("Granting access control failed", res)
        return True

    def revoke_access_control(self, subject, action, scope):
        params = {"subject": subject, "action": action, "scope": scope}
        code, body, res = self.post("/v3/acl/revoke", params)
        if code != 200:
            self.raise_error("Revoking access control failed", res)
        return True

    # [true, [{subject:String,action:String,scope:String}]]
    def test_access_control(self, user, action, scope):
        params = {"user": user, "action": action, "scope": scope}
        code, body, res = self.get("/v3/acl/test", params)
        if code != 200:
            self.raise_error("Testing access control failed", res)
        js = self.checked_json(body, ["permission", "access_controls"])
        perm = js["permission"]
        acl = [ [roleinfo["subject"], roleinfo["action"], roleinfo["scope"]] for roleinfo in js["access_controls"] ]
        return (perm, acl)

    # [{subject:String,action:String,scope:String}]
    def list_access_controls(self):
        code, body, res = self.get("/v3/acl/list")
        if code != 200:
            self.raise_error("Listing access control failed", res)
        js = self.checked_json(body, ["access_controls"])
        acl = [ [roleinfo["subject"], roleinfo["action"], roleinfo["scope"], roleinfo["grant_option"]] for roleinfo in js["access_controls"] ]
        return acl

    ####
    ## Account API
    ##

    def show_account(self):
        code, body, res = self.get("/v3/account/show")
        if code != 200:
            self.raise_error("Show account failed", res)
        js = self.checked_json(body, ["account"])
        a = js["account"]
        account_id = int(a["id"])
        plan = int(a["plan"])
        storage_size = int(a["storage_size"])
        guaranteed_cores = int(a["guaranteed_cores"])
        maximum_cores = int(a["maximum_cores"])
        created_at = a["created_at"]
        return [account_id, plan, storage_size, guaranteed_cores, maximum_cores, created_at]
    def account_core_utilization(self, _from, to):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        code, body, res = get("/v3/account/core_utilization", params)
        if code != 200:
            self.raise_error("Show account failed", res)
        js = self.checked_json(body, ["from", "to", "interval", "history"])
        _from = time.strptime(js["from"], "%Y-%m-%d %H:%M:%S %Z")
        to = time.strptime(js["to"], "%Y-%m-%d %H:%M:%S %Z")
        interval = int(js["interval"])
        history = js["history"]
        return [_from, to, interval, history]

    ####
    ## Bulk import API
    ##

    # => nil
    def create_bulk_import(self, name, db, table, params={}):
        code, body, res = self.post("/v3/bulk_import/create/%s/%s/%s" % (urlquote(str(name)), urlquote(str(db)), urlquote(str(table))), params)
        if code != 200:
            self.raise_error("Create bulk import failed", res)
        end
        return None

    # => nil
    def delete_bulk_import(self, name, params={}):
        code, body, res = self.post("/v3/bulk_import/delete/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Delete bulk import failed", res)
        end
        return None

    # => data:Hash
    def show_bulk_import(self, name):
        code, body, res = self.get("/v3/bulk_import/show/%s" % (urlquote(str(name))))
        if code != 200:
            self.raise_error("Show bulk import failed", res)
        end
        js = self.checked_json(body, ["status"])
        return js

    # => result:[data:Hash]
    def list_bulk_imports(self, params={}):
        code, body, res = self.get("/v3/bulk_import/list", params)
        if code != 200:
            self.raise_error("List bulk imports failed", res)
        js = self.checked_json(body, ["bulk_imports"])
        return js["bulk_imports"]

    def list_bulk_import_parts(self, name, params={}):
        code, body, res = self.get("/v3/bulk_import/list_parts/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("List bulk import parts failed", res)
        js = self.checked_json(body, ["parts"])
        return js["parts"]

    # => nil
    def bulk_import_upload_part(self, name, part_name, stream, size):
        code, body, res = self.put("/v3/bulk_import/upload_part/%s/%s" % (urlquote(str(name)), urlquote(str(part_name))), stream, size)
        if code / 100 != 2:
            self.raise_error("Upload a part failed", res)
        return None

    # => nil
    def bulk_import_delete_part(self, name, part_name, parms={}):
        code, body, res = self.post("/v3/bulk_import/delete_part/%s/%s" % (urlquote(str(name)), urlquote(str(part_name))), params)
        if code / 100 != 2:
            self.raise_error("Delete a part failed", res)
        return None

    # => nil
    def freeze_bulk_import(self, name, params={}):
        code, body, res = self.post("/v3/bulk_import/freeze/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Freeze bulk import failed", res)
        return None

    # => nil
    def unfreeze_bulk_import(self, name, params={}):
        code, body, res = self.post("/v3/bulk_import/unfreeze/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Unfreeze bulk import failed", res)
        return None

    # => jobId:String
    def perform_bulk_import(self, name, params={}):
        code, body, res = self.post("/v3/bulk_import/perform/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Perform bulk import failed", res)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])

    # => nil
    def commit_bulk_import(self, name, params={}):
        code, body, res = self.post("/v3/bulk_import/commit/%s" % (urlquote(str(name))), params)
        if code != 200:
            eslf.raise_error("Commit bulk import failed", res)
        return None

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

    # => true
    def delete_database(self, db):
        code, body, res = self.post("/v3/database/delete/%s" % urlquote(str(db)))
        if code != 200:
            self.raise_error("Delete database failed", res)
        return True

    # => true
    def create_database(self, db, params):
        code, body, res = self.post("/v3/database/create/%s" % urlquote(str(db)), params)
        if code != 200:
            self.raise_error("Create database failed", res)
        return True

    ####
    ## Export API
    ##

    # => jobId:String
    def export(self, db, table, storage_type, params={}):
        params["storage_type"] = storage_type
        code, body, res = self.post("/v3/export/run/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params)
        if code != 200:
            self.raise_error("Export failed", res)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])

  ####
    ## Import API
    ##

    # TODO: `import` is not available as Python method name
#   # => time:Float
#   def import(self, db, table, _format, stream, size, unique_id=None):
#       if unique_id is not None:
#           path = "/v3/table/import_with_id/%s/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(unique_id)), urlquote(str(format)))
#       else:
#           path = "/v3/table/import/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(forat)))
#       opts = {}
#       if self._host == DEFAULT_ENDPOINT
#           uri = urlparse.urlparse(DEFAULT_IMPORT_ENDPOINT)
#           opts["host"] = uri.host
#           opts["port"] = uri.port
#       code, body, res = self.put(path, stream, size, opts)
#       if code / 100 != 2:
#           self.raise_error("Import failed", res)
#       js = self.checked_json(body, [])
#       time = float(js["elapsed_time"])
#       return time

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
    ## Partial delete API
    ##

    def partial_delete(self, db, table, to, _from, params={}):
        params["to"] = str(to)
        params["from"] = str(_from)
        code, body, res = self.post("/v3/table/partialdelete/%s/%s" % (urlquote(str(db)), urlquote(str(table))), params)
        if code != 200:
            self.raise_error("Partial delete failed", res)
        js = self.checked_json(body, ["job_id"])
        return str(js["job_id"])

    ####
    ## Result API
    ##

    def list_result(self):
        code, body, res = self.get("/v3/result/list")
        if code != 200:
          self.raise_error("List result table failed", res)
        js = self.checked_json(body, ["results"])
        return [ [m["name"], m["url"], None] for m in js["result"] ] # same as database

    # => true
    def create_result(self, name, url, params={}):
        params.update({"url": url})
        code, body, res = self.post("/v3/result/create/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Create result table failed", res)
        return True

    # => true
    def delete_result(self, name):
        code, body, res = self.post("/v3/result/delete/%s" % (urlquote(str(name))))
        if code != 200:
            self.raise_error("Delete result table failed", res)
        return True

    ####
    ## Schedule API
    ##

    # => start:String
    def create_schedule(self, name, params={}):
        params.update({"type": params.get("type", "hive")})
        code, body, res = self.post("/v3/schedule/create/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Create schedule failed", res)
        js = self.checked_json(body, ["start"])
        return js["start"]

    # => cron:String, query:String
    def delete_schedule(self, name):
        code, body, res = self.post("/v3/schedule/delete/%s" % (urlquote(str(name))))
        if code != 200:
            self.raise_error("Delete schedule failed", res)
        js = self.checked_json(body, [])
        return (js["cron"], js["query"])

    # => [(name:String, cron:String, query:String, database:String, result_url:String)]
    def list_schedules(self):
        code, body, res = self.get("/v3/schedule/list")
        if code != 200:
            self.raise_error("List schedules failed", res)
        js = self.checked_json(body, ["schedules"])
        def schedule(m):
            name = m.get("name")
            cron = m.get("cron")
            query = m.get("query")
            database = m.get("database")
            result_url = m.get("result")
            timezone = m.get("timezone")
            delay = m.get("delay")
            next_time = m.get("next_time")
            priority = m.get("priority")
            retry_limit = m.get("retry_limit")
            return [name, cron, query, database, result_url, timezone, delay, next_time, priority, retry_limit, None] # same as database
        return [ schedule(m) for m in js["schedules"] ]

    def update_schedule(self, name, params):
      code, body, res = post("/v3/schedule/update/%s" % (urlquote(str(name))), params)
      if code != 200:
          self.raise_error("Update schedule failed", res)
      return None

    def history(self, name, _from=0, to=None):
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        code, body, res = self.get("/v3/schedule/history/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("List history failed", res)
        js = self.checked_json(body, ["history"])
        def history(m):
            job_id = m.get("job_id")
            _type = m.get("type", "?")
            database = m.get("database")
            status = m.get("status")
            query = m.get("query")
            start_at = m.get("start_at")
            end_at = m.get("end_at")
            scheduled_at = m.get("scheduled_at")
            result_url = m.get("result")
            priority = m.get("priority")
            return [scheduled_at, job_id, _type, status, query, start_at, end_at, result_url, priority, database]
        return [ history(m) for m in js["history"] ]

    def run_schedule(self, name, time, num):
        params = {}
        if num is not None:
            params = {"num": num}
        code, body, res = self.post("/v3/schedule/run/%s/%s" % (urlquote(str(name)), urlquote(str(time))), params)
        if code != 200:
            self.raise_error("Run schedule failed", res)
        js = self.checked_json(body, ["jobs"])
        def job(m):
            job_id = m.get("job_id")
            scheduled_at = m.get("scheduled_at")
            _type = m.get("type", "?")
            return [job_id, _type, scheduled_at]
        return [ job(m) for m in js["jobs"] ]

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

    def _create_log_or_item_table(self, db, table, _type):
        code, body, res = self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(_type))))
        if code != 200:
            self.raise_error("Create #{type} table failed", res)
        return True

    # => true
    def create_log_table(self, db, table):
        return self.create_table(db, table, "log")

    # => true
    def create_item_table(self, db, table, primary_key, primary_key_type):
        params = {"primary_key": primary_key, "primary_key_type": primary_key_type}
        return self.create_table(db, table, "item", params)

    def _create_table(self, db, table, _type, params={}):
        code, body, res = self.post("/v3/table/create/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table)), urlquote(str(_type))), params)
        if code != 200:
            self.raise_error("Create %s table failed" % (_type), res)
        return True

    # => true
    def swap_table(self, db, table1, table2):
        code, body, res = self.post("/v3/table/swap/%s/%s/%s" % (urlquote(str(db)), urlquote(str(table1), urlquote(str(table2)))))
        if code != 200:
            self.raise_error("Swap tables failed", res)
        return True

    # => true
    def update_schema(self, db, table, schema_json):
        code, body, res = self.post("/v3/table/update-schema/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"schema": schema_json})
        if code != 200:
            self.raise_error("Create schema table failed", res)
        return True

    def update_expire(self, db, table, expire_days):
        code, body, res = self.post("/v3/table/update/%s/%s" % (urlquote(str(db)), urlquote(str(table))), {"expire_days": expire_days})
        if code != 200:
            self.raise_error("Update table expiration failed", res)
        return True

    # => type:Symbol
    def delete_table(self, db, table):
        code, body, res = self.post("/v3/table/delete/%s/%s" % (urlquote(str(db)), urlquote(str(table))))
        if code != 200:
            self.raise_error("Delete table failed", res)
        js = self.checked_json(body, [])
        _type = js.get("type", "?")
        return _type

    ####
    ## User API
    ##

    # apikey:String
    def authenticate(self, user, password):
        code, body, res = self.post("/v3/user/authenticate", {"user": user, "password": password})
        if code != 200:
            self.raise_error("Authentication failed", res)
        js = self.checked_json(body, ["apikey"])
        apikey = js["apikey"]
        return apikey

    # => [[name:String,organization:String,[user:String]]
    def list_users(self):
        code, body, res = self.get("/v3/user/list")
        if code != 200:
            self.raise_error("List users failed", res)
        js = self.checked_json(body, ["users"])
        def user(roleinfo):
            name = roleinfo["name"]
            email = roleinfo["email"]
            return [name, None, None, email] # set nil to org and role for API compatibility
        return [ user(roleinfo) for roleinfo in js["users"] ]

    # => true
    def add_user(self, name, org, email, password):
        params = {"organization": org, "email": email, "password": password}
        code, body, res = self.post("/v3/user/add/%s" % (urlquote(str(name))), params)
        if code != 200:
            self.raise_error("Adding user failed", res)
        return True

    # => true
    def remove_user(self, user):
        code, body, res = self.post("/v3/user/remove/%s" % (urlquote(str(user))))
        if code != 200:
            self.raise_error("Removing user failed", res)
        return True

    # => true
    def change_email(self, user, email):
        params = {"email": email}
        code, body, res = self.post("/v3/user/email/change/%s" % (urlquote(str(user))), params)
        if code != 200:
            self.raise_error("Changing email failed", res)
        return True

    # => [apikey:String]
    def list_apikeys(self, user):
        code, body, res = self.get("/v3/user/apikey/list/%s" % (urlquote(str(user))))
        if code != 200:
            self.raise_error("List API keys failed", res)
        js = self.checked_json(body, ["apikeys"])
        return js["apikeys"]

    # => true
    def add_apikey(self, user):
        code, body, res = self.post("/v3/user/apikey/add/%s" % (urlquote(str(user))))
        if code != 200:
            self.raise_error("Adding API key failed", res)
        return True

    # => true
    def remove_apikey(self, user, apikey):
        params = {"apikey": apikey}
        code, body, res = self.post("/v3/user/apikey/remove/%s" % (urlquote(str(user))), params)
        if code != 200:
            self.raise_error("Removing API key failed", res)
        return True

    # => true
    def change_password(self, user, password):
        params = {"password": password}
        code, body, res = self.post("/v3/user/password/change/%s" % (urlquote(str(user))), params)
        if code != 200:
            self.raise_error("Changing password failed", res)
        return True

    # => true
    def change_my_password(self, old_password, password):
        params = {"old_password": old_password, "password": password}
        code, body, res = self.post("/v3/user/password/change", params)
        if code != 200:
            self.raise_error("Changing password failed", res)
        return True

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

        path = self._base_path + url

        # up to 7 retries with exponential (base 2) back-off starting at 'retry_delay'
        retry_delay = 5
        cumul_retry_delay = 0

        response = None
        while True:
            try:
                http.request("PUT", path, headers=header)
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
