#!/usr/bin/env python

from urllib.parse import quote as urlquote


class ResultAPI:
    ####
    ## Result API
    ##

    def list_result(self):
        """
        TODO: add docstring
        """
        with self.get("/v3/result/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List result table failed", res, body)
            js = self.checked_json(body, ["results"])
            return [
                (m["name"], m["url"], None) for m in js["results"]
            ]  # same as database

    def create_result(self, name, url, params=None):
        """
        TODO: add docstring
        => True
        """
        params = {} if params is None else params
        params.update({"url": url})
        with self.post("/v3/result/create/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Create result table failed", res, body)
            return True

    def delete_result(self, name):
        """
        TODO: add docstring
        => True
        """
        with self.post("/v3/result/delete/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Delete result table failed", res, body)
            return True
