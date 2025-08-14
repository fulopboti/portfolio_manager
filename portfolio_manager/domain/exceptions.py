"""Domain-specific exceptions for the Stock Analysis Platform."""


class DomainError(Exception):
    """Base exception for all domain-related errors."""

    pass


class DomainValidationError(DomainError):
    """Raised when domain entity validation fails."""

    pass


class InsufficientFundsError(DomainError):
    """Raised when a portfolio has insufficient funds for an operation."""

    pass


class InvalidTradeError(DomainError):
    """Raised when a trade operation is invalid."""

    pass


class InvalidPositionError(DomainError):
    """Raised when a position operation is invalid."""

    pass


class DataIngestionError(DomainError):
    """Raised when data ingestion fails."""

    pass


class StrategyCalculationError(DomainError):
    """Raised when strategy calculation fails."""

    pass


class RepositoryError(DomainError):
    """Raised when repository operations fail."""

    pass
