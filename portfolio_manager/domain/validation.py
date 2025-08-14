"""
Domain validation utilities.

This module provides reusable validation functions for common domain patterns,
eliminating code duplication across entities and events.
"""

from decimal import Decimal
from typing import Any

from .exceptions import DomainValidationError


def validate_symbol(symbol: str, field_name: str = "Symbol") -> None:
    """
    Validate that a trading symbol is not empty or whitespace.

    Args:
        symbol: The symbol to validate
        field_name: Name of the field being validated (for error messages)

    Raises:
        DomainValidationError: If symbol is empty or whitespace-only
    """
    if not symbol or not symbol.strip():
        raise DomainValidationError(f"{field_name} cannot be empty")


def validate_positive_decimal(
    value: Decimal,
    field_name: str,
    allow_zero: bool = False
) -> None:
    """
    Validate that a decimal value is positive (and optionally allow zero).

    Args:
        value: The decimal value to validate
        field_name: Name of the field being validated
        allow_zero: Whether zero is considered valid

    Raises:
        DomainValidationError: If value is not positive (or zero when allowed)
    """
    if allow_zero and value < 0:
        raise DomainValidationError(f"{field_name} cannot be negative")
    elif not allow_zero and value <= 0:
        raise DomainValidationError(f"{field_name} must be positive")


def validate_non_empty_string(value: str, field_name: str) -> None:
    """
    Validate that a string is not empty or whitespace-only.

    Args:
        value: The string to validate
        field_name: Name of the field being validated

    Raises:
        DomainValidationError: If string is empty or whitespace-only
    """
    if not value or not value.strip():
        raise DomainValidationError(f"{field_name} cannot be empty")


def validate_non_empty_collection(
    collection: list | set | tuple,
    field_name: str
) -> None:
    """
    Validate that a collection is not empty.

    Args:
        collection: The collection to validate
        field_name: Name of the field being validated

    Raises:
        DomainValidationError: If collection is empty
    """
    if not collection:
        raise DomainValidationError(f"{field_name} cannot be empty")


def validate_currency_code(currency: str, valid_currencies: set[str] | None = None) -> None:
    """
    Validate that a currency code is in the allowed set.

    Args:
        currency: The currency code to validate
        valid_currencies: Set of valid currency codes (uses default if None)

    Raises:
        DomainValidationError: If currency is not in valid set
    """
    if valid_currencies is None:
        valid_currencies = {"USD", "EUR", "RON", "GBP", "JPY", "CAD", "AUD", "CHF"}

    if currency not in valid_currencies:
        raise DomainValidationError(
            f"Currency must be one of: {', '.join(sorted(valid_currencies))}"
        )


def validate_price_relationships(
    low: Decimal,
    high: Decimal,
    open_price: Decimal | None = None,
    close: Decimal | None = None
) -> None:
    """
    Validate OHLC price relationships (High >= Low, Open/Close within range).

    Args:
        low: The low price
        high: The high price
        open_price: The opening price (optional)
        close: The closing price (optional)

    Raises:
        DomainValidationError: If price relationships are invalid
    """
    if high < low:
        raise DomainValidationError("High price must be >= low price")

    if open_price is not None:
        if open_price < low:
            raise DomainValidationError("Open price must be >= low price")
        if open_price > high:
            raise DomainValidationError("Open price must be <= high price")

    if close is not None:
        if close < low:
            raise DomainValidationError("Close price must be >= low price")
        if close > high:
            raise DomainValidationError("Close price must be <= high price")


def validate_severity_level(severity: str, valid_levels: set[str] | None = None) -> None:
    """
    Validate that a severity level is in the allowed set.

    Args:
        severity: The severity level to validate
        valid_levels: Set of valid severity levels (uses default if None)

    Raises:
        DomainValidationError: If severity is not in valid set
    """
    if not severity or not severity.strip():
        raise DomainValidationError("Severity cannot be empty")

    if valid_levels is None:
        valid_levels = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}

    if severity.upper() not in valid_levels:
        raise DomainValidationError(f"Severity must be one of: {valid_levels}")


def validate_percentage(value: Decimal, field_name: str, allow_negative: bool = False) -> None:
    """
    Validate that a value represents a valid percentage.

    Args:
        value: The percentage value to validate (as decimal, e.g., 0.05 for 5%)
        field_name: Name of the field being validated
        allow_negative: Whether negative percentages are allowed

    Raises:
        DomainValidationError: If percentage value is invalid
    """
    if not allow_negative and value < 0:
        raise DomainValidationError(f"{field_name} percentage cannot be negative")

    # Reasonable upper bound for percentages (1000% = 10.0)
    if value > 10:
        raise DomainValidationError(f"{field_name} percentage seems unreasonably high")


def validate_quantity(quantity: Decimal, field_name: str = "Quantity") -> None:
    """
    Validate that a quantity is positive (for trading operations).

    Args:
        quantity: The quantity to validate
        field_name: Name of the field being validated

    Raises:
        DomainValidationError: If quantity is not positive
    """
    if quantity <= 0:
        raise DomainValidationError(f"{field_name} must be positive")


def validate_event_id(event_id: str) -> None:
    """
    Validate that an event ID is not empty.

    Args:
        event_id: The event ID to validate

    Raises:
        DomainValidationError: If event ID is empty
    """
    if not event_id or not event_id.strip():
        raise DomainValidationError("Event ID cannot be empty")


class ValidationBuilder:
    """
    Builder pattern for chaining multiple validations.

    Allows for fluent validation syntax:
    ValidationBuilder(symbol).not_empty("Symbol").build()
    """

    def __init__(self, value: Any):
        """Initialize with the value to validate."""
        self.value = value
        self._validations: list[callable] = []

    def not_empty(self, field_name: str) -> 'ValidationBuilder':
        """Add not-empty validation."""
        if isinstance(self.value, str):
            self._validations.append(lambda: validate_non_empty_string(self.value, field_name))
        else:
            self._validations.append(lambda: validate_non_empty_collection(self.value, field_name))
        return self

    def positive(self, field_name: str, allow_zero: bool = False) -> 'ValidationBuilder':
        """Add positive number validation."""
        if isinstance(self.value, Decimal):
            self._validations.append(
                lambda: validate_positive_decimal(self.value, field_name, allow_zero)
            )
        return self

    def symbol(self, field_name: str = "Symbol") -> 'ValidationBuilder':
        """Add symbol validation."""
        self._validations.append(lambda: validate_symbol(self.value, field_name))
        return self

    def currency(self, valid_currencies: set[str] | None = None) -> 'ValidationBuilder':
        """Add currency code validation."""
        self._validations.append(lambda: validate_currency_code(self.value, valid_currencies))
        return self

    def severity(self, valid_levels: set[str] | None = None) -> 'ValidationBuilder':
        """Add severity level validation."""
        self._validations.append(lambda: validate_severity_level(self.value, valid_levels))
        return self

    def build(self) -> None:
        """Execute all accumulated validations."""
        for validation in self._validations:
            validation()


# Convenience function for fluent validation
def validate(value: Any) -> ValidationBuilder:
    """
    Start a validation chain for a value.

    Args:
        value: The value to validate

    Returns:
        ValidationBuilder for chaining validations

    Example:
        validate(symbol).symbol().build()
        validate(price).positive("Price").build()
    """
    return ValidationBuilder(value)
