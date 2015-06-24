#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

from tdclient.model import Model

class AccessControl(Model):
    """Access control settings of a user on Treasure Data Service
    """

    def __init__(self, client, subject, action, scope, grant_option):
        super(AccessControl, self).__init__(client)
        self._subject = subject
        self._action = action
        self._scope = scope
        self._grant_option = grant_option

    @property
    def subject(self):
        """
        TODO: add docstring
        """
        return self._subject

    @property
    def action(self):
        """
        TODO: add docstring
        """
        return self._action

    @property
    def scope(self):
        """
        TODO: add docstring
        """
        return self._scope

    @property
    def grant_option(self):
        """
        TODO: add docstring
        """
        return self._grant_option

    def __str__(self):
        print_fmt = """\
{class_name}(subject={subject}, action={action}, scope={scope}, grant_option={grant_option})
"""
        access_control = print_fmt.format(
            subject=self._subject,
            action=self._action,
            scope=self._scope,
            grant_option=self._grant_option
            )
