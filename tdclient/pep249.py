#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import datetime
from tdclient import connection
from tdclient import cursor

def connect(*args, **kwargs):
    return connection.Connection(*args, **kwargs)

apilevel = "2.0"

threadsafety = 3

paramstyle = "pyformat"

class Warning(Warning):
    pass

class Error(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

Date = datetime.date

Time = datetime.time

Timestamp = datetime.datetime

def DateFromTicks(ticks):
    return datetime.date(*datetime.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return datetime.time(*datetime.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return datetime.datetime(*datetime.localtime(ticks)[:6])

def Binary(string):
    pass

STRING = "string"

BINARY = "string"

NUMBER = "int"

DATETIME = "int"

ROWID = "long"
