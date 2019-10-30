#!/usr/bin/env python
from .util import create_url, get_or_else, parse_date


class ScheduleAPI:
    """Access to Schedule API

    This class is inherited by :class:`tdclient.api.API`.
    """

    def create_schedule(self, name, params=None):
        """Create a new scheduled query with the specified name.

        Args:
            name (str): Scheduled query name.
            params (dict, optional): Extra parameters.

                - type (str):
                    Query type. {"presto", "hive"}. Default: "hive"
                - database (str):
                    Target database name.
                - timezone (str):
                    Scheduled query's timezone. e.g. "UTC"
                    For details, see also: https://gist.github.com/frsyuki/4533752
                - cron (str, optional):
                    Schedule of the query.
                    {``"@daily"``, ``"@hourly"``, ``"10 * * * *"`` (custom cron)}
                    See also: https://support.treasuredata.com/hc/en-us/articles/360001451088-Scheduled-Jobs-Web-Console
                - delay (int, optional):
                    A delay ensures all buffered events are imported
                    before running the query. Default: 0
                - query (str):
                    Is a language used to retrieve, insert, update and modify
                    data. See also: https://support.treasuredata.com/hc/en-us/articles/360012069493-SQL-Examples-of-Scheduled-Queries
                - priority (int, optional):
                    Priority of the query.
                    Range is from -2 (very low) to 2 (very high). Default: 0
                - retry_limit (int, optional):
                    Automatic retry count. Default: 0
                - engine_version (str, optional):
                    Engine version to be used. If none is
                    specified, the account's default engine version would be set.
                    {"stable", "experimental"}
                - pool_name (str, optional):
                    For Presto only. Pool name to be used, if not
                    specified, default pool would be used.
                - result (str, optional):
                    Location where to store the result of the query.
                    e.g. 'tableau://user:password@host.com:1234/datasource'
        Returns:
            datetime.datetime: Start date time.
        """
        params = {} if params is None else params
        params.update({"type": params.get("type", "hive")})
        with self.post(
            create_url("/v3/schedule/create/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create schedule failed", res, body)
            js = self.checked_json(body, ["start"])
            return parse_date(get_or_else(js, "start", "1970-01-01T00:00:00Z"))

    def delete_schedule(self, name):
        """Delete the scheduled query with the specified name.

        Args:
            name (str): Target scheduled query name.
        Returns:
            (str, str): Tuple of cron and query.
        """
        with self.post(create_url("/v3/schedule/delete/{name}", name=name)) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete schedule failed", res, body)
            js = self.checked_json(body, ["cron", "query"])
            return js["cron"], js["query"]

    def list_schedules(self):
        """Get the list of all the scheduled queries.

        Returns:
            [(name:str, cron:str, query:str, database:str, result_url:str)]
        """
        with self.get("/v3/schedule/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List schedules failed", res, body)
            js = self.checked_json(body, ["schedules"])

            return [schedule_to_tuple(m) for m in js["schedules"]]

    def update_schedule(self, name, params=None):
        """Update the scheduled query.

        Args:
            name (str): Target scheduled query name.
            params (dict): Extra parameteres.

                - type (str):
                    Query type. {"presto", "hive"}. Default: "hive"
                - database (str):
                    Target database name.
                - timezone (str):
                    Scheduled query's timezone. e.g. "UTC"
                    For details, see also: https://gist.github.com/frsyuki/4533752
                - cron (str, optional):
                    Schedule of the query.
                    {``"@daily"``, ``"@hourly"``, ``"10 * * * *"`` (custom cron)}
                    See also: https://support.treasuredata.com/hc/en-us/articles/360001451088-Scheduled-Jobs-Web-Console
                - delay (int, optional):
                    A delay ensures all buffered events are imported
                    before running the query. Default: 0
                - query (str):
                    Is a language used to retrieve, insert, update and modify
                    data. See also: https://support.treasuredata.com/hc/en-us/articles/360012069493-SQL-Examples-of-Scheduled-Queries
                - priority (int, optional):
                    Priority of the query.
                    Range is from -2 (very low) to 2 (very high). Default: 0
                - retry_limit (int, optional):
                    Automatic retry count. Default: 0
                - engine_version (str, optional):
                    Engine version to be used. If none is
                    specified, the account's default engine version would be set.
                    {"stable", "experimental"}
                - pool_name (str, optional):
                    For Presto only. Pool name to be used, if not
                    specified, default pool would be used.
                - result (str, optional):
                    Location where to store the result of the query.
                    e.g. 'tableau://user:password@host.com:1234/datasource'
        """
        params = {} if params is None else params
        with self.post(
            create_url("/v3/schedule/update/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Update schedule failed", res, body)

    def history(self, name, _from=0, to=None):
        """Get the history details of the saved query for the past 90days.

        Args:
            name (str): Target name of the scheduled query.
            _from (int, optional): Indicates from which nth record in the run history
                would be fetched.
                Default: 0.
                Note: Count starts from zero. This means that the first record in the
                list has a count of zero.
            to (int, optional): Indicates up to which nth record in the run history
                would be fetched.
                Default: 20
        Returns:
            dict: History of the scheduled query.
        """
        params = {}
        if _from is not None:
            params["from"] = str(_from)
        if to is not None:
            params["to"] = str(to)
        with self.get(
            create_url("/v3/schedule/history/{name}", name=name), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List history failed", res, body)
            js = self.checked_json(body, ["history"])

            return [history_to_tuple(m) for m in js["history"]]

    def run_schedule(self, name, time, num=None):
        """Execute the specified query.

        Args:
            name (str): Target scheduled query name.
            time (int): Time in Unix epoch format that would be set as TD_SCHEDULED_TIME
            num (int, optional): Indicates how many times the query will be executed.
                Value should be 9 or less.
                Default: 1
        Returns:
            list of tuple: [(job_id:int, type:str, scheduled_at:str)]
        """
        params = {}
        if num is not None:
            params = {"num": num}
        with self.post(
            create_url("/v3/schedule/run/{name}/{time}", name=name, time=time), params
        ) as res:
            code, body = res.status, res.read()
        if code != 200:
            self.raise_error("Run schedule failed", res, body)
        js = self.checked_json(body, ["jobs"])

        return [job_to_tuple(m) for m in js["jobs"]]


def job_to_tuple(m):
    job_id = m.get("job_id")
    scheduled_at = parse_date(get_or_else(m, "scheduled_at", "1970-01-01T00:00:00Z"))
    t = m.get("type", "?")
    return job_id, t, scheduled_at


def schedule_to_tuple(m):
    m = dict(m)
    if "timezone" not in m:
        m["timezone"] = "UTC"
    m["created_at"] = parse_date(get_or_else(m, "created_at", "1970-01-01T00:00:00Z"))
    m["next_time"] = parse_date(get_or_else(m, "next_time", "1970-01-01T00:00:00Z"))
    return m


def history_to_tuple(m):
    job_id = m.get("job_id")
    t = m.get("type", "?")
    database = m.get("database")
    status = m.get("status")
    query = m.get("query")
    start_at = parse_date(get_or_else(m, "start_at", "1970-01-01T00:00:00Z"))
    end_at = parse_date(get_or_else(m, "end_at", "1970-01-01T00:00:00Z"))
    scheduled_at = parse_date(get_or_else(m, "scheduled_at", "1970-01-01T00:00:00Z"))
    result_url = m.get("result")
    priority = m.get("priority")
    return (
        scheduled_at,
        job_id,
        t,
        status,
        query,
        start_at,
        end_at,
        result_url,
        priority,
        database,
    )
