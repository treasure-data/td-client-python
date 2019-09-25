import datetime

from tdclient import client, connection, errors, version

__version__ = version.__version__

Client = client.Client


def connect(*args, **kwargs):
    """Returns a DBAPI compatible connection object

    Args:
        type (str): query engine type. "hive" by default.
        db (str): the name of database on Treasure Data
        result_url (str): result output URL
        priority (str): job priority
        retry_limit (int): job retry limit
        wait_interval (int): job wait interval to check status
        wait_callback (callable): a callback to be called on every ticks of job wait

    Returns:
         :class:`tdclient.connection.Connection`
    """
    return connection.Connection(*args, **kwargs)


apilevel = "2.0"
threadsafety = 3
paramstyle = "pyformat"

Error = errors.Error
InterfaceError = errors.InterfaceError
DatabaseError = errors.DatabaseError
DataError = errors.DataError
OperationalError = errors.OperationalError
IntegrityError = errors.IntegrityError
InternalError = errors.InternalError
ProgrammingError = errors.ProgrammingError
NotSupportedError = errors.NotSupportedError

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
    return bytes(string)


STRING = "string"

BINARY = "string"

NUMBER = "int"

DATETIME = "int"

ROWID = "long"
