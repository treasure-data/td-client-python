#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from tdclient import user_api
from tdclient.test.test_helper import *

def setup_function(function):
    unset_environ()
