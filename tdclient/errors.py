#!/usr/bin/env python


class ParameterValidationError(Exception):
    """Exception raised when parameter validation fails."""

    pass


# Generic API error
class APIError(Exception):
    """Base exception for API-related errors."""

    pass


# 401 API errors
class AuthError(APIError):
    """Exception raised for authentication errors (HTTP 401)."""

    pass


# 403 API errors, used for database permissions
class ForbiddenError(APIError):
    """Exception raised for forbidden access errors (HTTP 403)."""

    pass


# 409 API errors
class AlreadyExistsError(APIError):
    """Exception raised when a resource already exists (HTTP 409)."""

    pass


# 404 API errors
class NotFoundError(APIError):
    """Exception raised when a resource is not found (HTTP 404)."""

    pass


# PEP 0249 errors
class Error(Exception):
    """Base class for database-related errors (PEP 249)."""

    pass


class InterfaceError(Error):
    """Exception for errors related to the database interface (PEP 249)."""

    pass


class DatabaseError(Error):
    """Exception for errors related to the database (PEP 249)."""

    pass


class DataError(DatabaseError):
    """Exception for errors due to problems with the processed data (PEP 249)."""

    pass


class OperationalError(DatabaseError):
    """Exception for errors related to database operation (PEP 249)."""

    pass


class IntegrityError(DatabaseError):
    """Exception for errors related to relational integrity (PEP 249)."""

    pass


class InternalError(DatabaseError):
    """Exception for internal database errors (PEP 249)."""

    pass


class ProgrammingError(DatabaseError):
    """Exception for programming errors (PEP 249)."""

    pass


class NotSupportedError(DatabaseError):
    """Exception for unsupported operations (PEP 249)."""

    pass
