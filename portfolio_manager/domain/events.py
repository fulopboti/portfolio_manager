"""
Domain events for the stock analysis platform.

This module contains all domain events that represent significant business occurrences
in the system. Events are immutable and carry all necessary data for event handlers
to process them appropriately.
"""

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from .entities import TradeSide
from .exceptions import DomainValidationError
from .validation import (
    validate_event_id,
    validate_non_empty_string,
    validate_positive_decimal,
    validate_severity_level,
    validate_symbol,
)


@dataclass(frozen=True)
class DomainEvent(ABC):
    """Base class for all domain events."""

    event_id: str
    timestamp: datetime

    def __post_init__(self):
        """Validate event data after initialization."""
        validate_event_id(self.event_id)


@dataclass(frozen=True)
class TradeExecutedEvent(DomainEvent):
    """Event published when a trade is executed in the system."""

    trade_id: UUID
    portfolio_id: UUID
    symbol: str
    side: TradeSide
    quantity: Decimal
    price: Decimal

    def __post_init__(self):
        """Validate trade event data."""
        super().__post_init__()

        validate_symbol(self.symbol)
        validate_positive_decimal(self.quantity, "Quantity")
        validate_positive_decimal(self.price, "Price")

    def gross_amount(self) -> Decimal:
        """Calculate the gross trade amount (quantity * price)."""
        return self.quantity * self.price

    def __str__(self) -> str:
        """Return string representation of the trade event."""
        return (f"TradeExecutedEvent(symbol={self.symbol}, side={self.side.value}, "
                f"qty={self.quantity}, price=${self.price})")


@dataclass(frozen=True)
class AssetPriceUpdatedEvent(DomainEvent):
    """Event published when an asset price is updated."""

    symbol: str
    old_price: Decimal
    new_price: Decimal

    def __post_init__(self):
        """Validate price update event data."""
        super().__post_init__()

        validate_symbol(self.symbol)
        validate_positive_decimal(self.old_price, "Old price")
        validate_positive_decimal(self.new_price, "New price")

    def price_change(self) -> Decimal:
        """Calculate the absolute price change."""
        return self.new_price - self.old_price

    def price_change_percent(self) -> Decimal:
        """Calculate the percentage price change."""
        if self.old_price == 0:
            return Decimal("0")
        return (self.new_price - self.old_price) / self.old_price

    def is_price_increase(self) -> bool:
        """Check if the price increased."""
        return self.new_price > self.old_price

    def is_price_decrease(self) -> bool:
        """Check if the price decreased."""
        return self.new_price < self.old_price


@dataclass(frozen=True)
class PositionChange:
    """Value object representing a change in portfolio position."""

    symbol: str
    old_quantity: Decimal
    new_quantity: Decimal
    reason: str

    def __post_init__(self):
        """Validate position change data."""
        validate_symbol(self.symbol)
        validate_non_empty_string(self.reason, "Reason")

        # Quantities can be zero (closing position) but not negative
        validate_positive_decimal(self.old_quantity, "Old quantity", allow_zero=True)
        validate_positive_decimal(self.new_quantity, "New quantity", allow_zero=True)

    def quantity_change(self) -> Decimal:
        """Calculate the quantity change."""
        return self.new_quantity - self.old_quantity


@dataclass(frozen=True)
class PortfolioRebalancedEvent(DomainEvent):
    """Event published when a portfolio is rebalanced."""

    portfolio_id: UUID
    changes: list[PositionChange]

    def __post_init__(self):
        """Validate portfolio rebalanced event data."""
        super().__post_init__()

        if not self.changes:
            raise ValueError("Changes list cannot be empty")

    def get_symbols_affected(self) -> set[str]:
        """Get the set of symbols affected by rebalancing."""
        return {change.symbol for change in self.changes}

    def get_change_for_symbol(self, symbol: str) -> PositionChange | None:
        """Get the position change for a specific symbol."""
        for change in self.changes:
            if change.symbol == symbol:
                return change
        return None


@dataclass(frozen=True)
class RiskThresholdBreachedEvent(DomainEvent):
    """Event published when a portfolio breaches a risk threshold."""

    portfolio_id: UUID
    threshold_type: str
    threshold_value: Decimal
    current_value: Decimal
    severity: str

    def __post_init__(self):
        """Validate risk threshold event data."""
        super().__post_init__()

        validate_non_empty_string(self.threshold_type, "Threshold type")
        validate_severity_level(self.severity)

    def breach_amount(self) -> Decimal:
        """Calculate how much the threshold was breached by."""
        return abs(self.current_value - self.threshold_value)

    def is_critical_breach(self) -> bool:
        """Check if this is a critical risk breach."""
        return self.severity.upper() == "CRITICAL"


@dataclass(frozen=True)
class MarketDataReceivedEvent(DomainEvent):
    """Event published when new market data is received."""

    symbol: str
    price: Decimal
    volume: int
    market_cap: Decimal | None = None

    def __post_init__(self):
        """Validate market data event."""
        super().__post_init__()

        validate_symbol(self.symbol)
        validate_positive_decimal(self.price, "Price")

        if self.volume < 0:
            raise DomainValidationError("Volume cannot be negative")

        if self.market_cap is not None:
            validate_positive_decimal(self.market_cap, "Market cap", allow_zero=True)


@dataclass(frozen=True)
class PortfolioCreatedEvent(DomainEvent):
    """Event published when a new portfolio is created."""

    portfolio_id: UUID
    owner_id: str
    initial_cash: Decimal
    strategy: str

    def __post_init__(self):
        """Validate portfolio created event."""
        super().__post_init__()

        validate_non_empty_string(self.owner_id, "Owner ID")
        validate_positive_decimal(self.initial_cash, "Initial cash", allow_zero=True)
        validate_non_empty_string(self.strategy, "Strategy")
