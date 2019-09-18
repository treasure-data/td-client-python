#!/usr/bin/env python

from urllib.parse import quote as urlquote


class UserAPI:
    ####
    ## User API
    ##

    def authenticate(self, user, password):
        """Authenticate the indicated email address which is not authenticated via SSO.

        Args:
            user (str): Email of the user to be authenticated.
            password (str): Must contain at least 1 letter, 1 number, and 1 special
                character such as the following:
                    [!#\$%\-_=\+<>0-9a-zA-Z]
        Returns:
            str: API key
        """
        with self.post(
            "/v3/user/authenticate", {"user": user, "password": password}
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Authentication failed", res, body)
            js = self.checked_json(body, ["apikey"])
            apikey = js["apikey"]
            return apikey

    def list_users(self):
        """Get the list of users for the account.

        Returns:
            [[name:str,organization:str,[user:str]]
        """
        with self.get("/v3/user/list") as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List users failed", res, body)
            js = self.checked_json(body, ["users"])

            def user(roleinfo):
                name = roleinfo["name"]
                email = roleinfo["email"]
                return (
                    name,
                    None,
                    None,
                    email,
                )  # set None to org and role for API compatibility

            return [user(roleinfo) for roleinfo in js["users"]]

    def add_user(self, name, org, email, password):
        """Add a new user to the current account and sends invitation.

        Args:
            name (str): User's name
            org (str): Not used
            email (str): User's email address
            password (str): User's temporary password for logging-in
        Returns:
            bool: `True` if succeeded.
        """
        params = {"organization": org, "email": email, "password": password}
        with self.post("/v3/user/add/%s" % (urlquote(str(name))), params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding user failed", res, body)
            return True

    def remove_user(self, name):
        """Remove the specified email in the account and revokes the user's access.

        Args:
            name (str): User's email address
        Returns:
            bool: `True` if succeded
        """
        with self.post("/v3/user/remove/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing user failed", res, body)
            return True

    def change_email(self, name, email):
        """
        TODO: remove
        => True
        """
        params = {"email": email}
        with self.post(
            "/v3/user/email/change/%s" % (urlquote(str(name))), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing email failed", res, body)
            return True

    def list_apikeys(self, name):
        """Get the apikeys of the current user.

        Args:
            name (str): User's email address
        Returns:
            [str]: List of API keys
        """
        with self.get("/v3/user/apikey/list/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("List API keys failed", res, body)
            js = self.checked_json(body, ["apikeys"])
            return js["apikeys"]

    def add_apikey(self, name):
        """Create a new apikey for the specified email address.

        Args:
            name (str): User's email address
        Returns:
            bool: `True` if succeeded.
        """
        with self.post("/v3/user/apikey/add/%s" % (urlquote(str(name)))) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Adding API key failed", res, body)
            return True

    def remove_apikey(self, name, apikey):
        """Delete the apikey for the specified email address.

        Args:
            name (str): User's email address
            apikey (str): User's apikey to be deleted
        Returns:
            bool: `True` if succeeded.
        """
        params = {"apikey": apikey}
        with self.post(
            "/v3/user/apikey/remove/%s" % (urlquote(str(name))), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Removing API key failed", res, body)
            return True

    def change_password(self, name, password):
        """
        TODO: remove
        => True
        """
        params = {"password": password}
        with self.post(
            "/v3/user/password/change/%s" % (urlquote(str(name))), params
        ) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True

    def change_my_password(self, old_password, password):
        """
        TODO: remove
        => True
        """
        params = {"old_password": old_password, "password": password}
        with self.post("/v3/user/password/change", params) as res:
            code, body = res.status, res.read()
            if code != 200:
                self.raise_error("Changing password failed", res, body)
            return True
