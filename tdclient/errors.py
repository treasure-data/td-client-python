#!/usr/bin/env python


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


# PEP 0249 errors
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
