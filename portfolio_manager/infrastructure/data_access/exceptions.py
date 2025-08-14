"""Data access layer specific exceptions."""

from portfolio_manager.domain.exceptions import DomainError


class DataAccessError(DomainError):
    """Base exception for data access layer errors."""

    pass


class ConnectionError(DataAccessError):
    """Exception raised when database connection fails."""

    pass


class TransactionError(DataAccessError):
    """Exception raised when transaction operations fail."""

    pass


class QueryError(DataAccessError):
    """Exception raised when query execution fails."""

    pass


class ParameterError(DataAccessError):
    """Exception raised when query parameters are invalid."""

    pass


class SchemaError(DataAccessError):
    """Exception raised when schema operations fail."""

    pass


class MigrationError(DataAccessError):
    """Exception raised when migration operations fail."""

    pass


class NotFoundError(DataAccessError):
    """Exception raised when requested data is not found."""

    pass
