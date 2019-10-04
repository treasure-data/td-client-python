#!/usr/bin/env python

from .util import create_url


class PartialDeleteAPI:
    """Create a job to partially delete the contents of the table with the given
    time range.

    This class is inherited by :class:`tdclient.api.API`.
    """

    def partial_delete(self, db, table, to, _from, params=None):
        """Create a job to partially delete the contents of the table with the given
        time range.

        Args:
            db (str): Target database name.
            table (str): Target table name.
            to (int): Time in Unix Epoch format indicating the End date and time of the
                data to be deleted. Should be set only by the hour. Minutes and seconds
                values will not be accepted.
            _from (int): Time in Unix Epoch format indicating the Start date and time of
                the data to be deleted. Should be set only by the hour. Minutes and
                seconds values will not be accepted.
            params (dict, optional): Extra parameters.

                - pool_name (str, optional):
                    Indicates the resource pool to execute this
                    job. If not provided, the account's default resource pool would be
                    used.
                - domain_key (str, optional):
                    Domain key that will be assigned to the
                    partial delete job to be created
        Returns:
            str: Job ID.
        """
        params = {} if params is None else params
        params["to"] = str(to)
        params["from"] = str(_from)
        with self.post(
            create_url("/v3/table/partialdelete/{db}/{table}", db=db, table=table),
            params,
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Partial delete failed", res, body)
            js = self.checked_json(body, ["job_id"])
            return str(js["job_id"])
