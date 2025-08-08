"""Unit tests for domain events."""

import pytest
import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4, UUID
from dataclasses import asdict
from typing import List

from portfolio_manager.domain.entities import TradeSide
from portfolio_manager.domain.exceptions import DomainValidationError


# Domain Events - These will be the actual classes to implement
from dataclasses import dataclass


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events."""
    event_id: str
    timestamp: datetime

    def __post_init__(self):
        if not self.event_id or not self.event_id.strip():
            raise DomainValidationError("Event ID cannot be empty")


@dataclass(frozen=True)
class TradeExecutedEvent(DomainEvent):
    """Event published when a trade is executed."""
    trade_id: UUID
    portfolio_id: UUID
    symbol: str
    side: TradeSide
    quantity: Decimal
    price: Decimal

    def __post_init__(self):
        super().__post_init__()

        if not self.symbol or not self.symbol.strip():
            raise DomainValidationError("Symbol cannot be empty")

        if self.quantity <= 0:
            raise DomainValidationError("Quantity must be positive")

        if self.price <= 0:
            raise DomainValidationError("Price must be positive")

    def gross_amount(self) -> Decimal:
        """Calculate gross trade amount (quantity * price)."""
        return self.quantity * self.price

    def __str__(self) -> str:
        return f"TradeExecutedEvent(symbol={self.symbol}, side={self.side.value}, qty={self.quantity}, price=${self.price})"


@dataclass(frozen=True)
class AssetPriceUpdatedEvent(DomainEvent):
    """Event published when an asset price is updated."""
    symbol: str
    old_price: Decimal
    new_price: Decimal

    def __post_init__(self):
        super().__post_init__()

        if not self.symbol or not self.symbol.strip():
            raise DomainValidationError("Symbol cannot be empty")

        if self.old_price <= 0:
            raise DomainValidationError("Old price must be positive")

        if self.new_price <= 0:
            raise DomainValidationError("New price must be positive")

    def price_change(self) -> Decimal:
        """Calculate absolute price change."""
        return self.new_price - self.old_price

    def price_change_percent(self) -> Decimal:
        """Calculate percentage price change."""
        if self.old_price == 0:
            return Decimal("0")
        return (self.new_price - self.old_price) / self.old_price

    def is_price_increase(self) -> bool:
        """Check if price increased."""
        return self.new_price > self.old_price

    def is_price_decrease(self) -> bool:
        """Check if price decreased."""
        return self.new_price < self.old_price


@dataclass(frozen=True)
class PositionChange:
    """Value object representing a position change."""
    symbol: str
    old_quantity: Decimal
    new_quantity: Decimal
    reason: str

    def __post_init__(self):
        if not self.symbol or not self.symbol.strip():
            raise DomainValidationError("Symbol cannot be empty")

        if not self.reason or not self.reason.strip():
            raise DomainValidationError("Reason cannot be empty")

        # Quantities can be zero (closing position) but not negative
        if self.old_quantity < 0 or self.new_quantity < 0:
            raise DomainValidationError("Quantities cannot be negative")

    def quantity_change(self) -> Decimal:
        """Calculate quantity change."""
        return self.new_quantity - self.old_quantity


@dataclass(frozen=True)
class PortfolioRebalancedEvent(DomainEvent):
    """Event published when a portfolio is rebalanced."""
    portfolio_id: UUID
    changes: List[PositionChange]

    def __post_init__(self):
        super().__post_init__()

        if not self.changes:
            raise DomainValidationError("Changes list cannot be empty")

    def get_symbols_affected(self) -> set[str]:
        """Get set of symbols affected by rebalancing."""
        return {change.symbol for change in self.changes}

    def get_change_for_symbol(self, symbol: str) -> PositionChange | None:
        """Get position change for specific symbol."""
        for change in self.changes:
            if change.symbol == symbol:
                return change
        return None


class TestDomainEvent:
    """Test base domain event functionality."""

    def test_domain_event_is_frozen(self):
        """Domain events should be immutable."""
        event = TradeExecutedEvent(
            event_id="test-event-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Should not be able to modify frozen dataclass
        with pytest.raises(AttributeError):
            event.symbol = "MSFT"

    def test_domain_event_equality(self):
        """Events with same data should be equal."""
        trade_id = uuid4()
        portfolio_id = uuid4()
        timestamp = datetime.now(timezone.utc)

        event1 = TradeExecutedEvent(
            event_id="test-123",
            timestamp=timestamp,
            trade_id=trade_id,
            portfolio_id=portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        event2 = TradeExecutedEvent(
            event_id="test-123",
            timestamp=timestamp,
            trade_id=trade_id,
            portfolio_id=portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        assert event1 == event2
        assert hash(event1) == hash(event2)

    def test_domain_event_inequality(self):
        """Events with different data should not be equal."""
        event1 = TradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        event2 = TradeExecutedEvent(
            event_id="test-456",  # Different event ID
            timestamp=event1.timestamp,
            trade_id=event1.trade_id,
            portfolio_id=event1.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        assert event1 != event2

    def test_domain_event_validation(self):
        """Should validate event ID."""
        with pytest.raises(DomainValidationError, match="Event ID cannot be empty"):
            TradeExecutedEvent(
                event_id="",
                timestamp=datetime.now(timezone.utc),
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="AAPL",
                side=TradeSide.BUY,
                quantity=Decimal("10"),
                price=Decimal("150.00")
            )


class TestTradeExecutedEvent:
    """Test trade execution events."""

    def test_valid_trade_executed_event_creation(self):
        """Should create valid trade executed event."""
        trade_id = uuid4()
        portfolio_id = uuid4()
        timestamp = datetime.now(timezone.utc)

        event = TradeExecutedEvent(
            event_id="trade-executed-123",
            timestamp=timestamp,
            trade_id=trade_id,
            portfolio_id=portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        assert event.event_id == "trade-executed-123"
        assert event.timestamp == timestamp
        assert event.trade_id == trade_id
        assert event.portfolio_id == portfolio_id
        assert event.symbol == "AAPL"
        assert event.side == TradeSide.BUY
        assert event.quantity == Decimal("10")
        assert event.price == Decimal("150.00")

    def test_trade_executed_event_validation(self):
        """Should validate trade event data."""
        base_kwargs = {
            "event_id": "trade-123",
            "timestamp": datetime.now(timezone.utc),
            "trade_id": uuid4(),
            "portfolio_id": uuid4(),
            "symbol": "AAPL",
            "side": TradeSide.BUY,
            "quantity": Decimal("10"),
            "price": Decimal("150.00")
        }

        # Valid event should work
        event = TradeExecutedEvent(**base_kwargs)
        assert event.symbol == "AAPL"

        # Empty symbol should fail
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            TradeExecutedEvent(**{**base_kwargs, "symbol": ""})

        # Zero quantity should fail
        with pytest.raises(DomainValidationError, match="Quantity must be positive"):
            TradeExecutedEvent(**{**base_kwargs, "quantity": Decimal("0")})

        # Negative quantity should fail
        with pytest.raises(DomainValidationError, match="Quantity must be positive"):
            TradeExecutedEvent(**{**base_kwargs, "quantity": Decimal("-10")})

        # Zero price should fail
        with pytest.raises(DomainValidationError, match="Price must be positive"):
            TradeExecutedEvent(**{**base_kwargs, "price": Decimal("0")})

        # Negative price should fail
        with pytest.raises(DomainValidationError, match="Price must be positive"):
            TradeExecutedEvent(**{**base_kwargs, "price": Decimal("-150")})

    def test_trade_executed_event_string_representation(self):
        """Should have meaningful string representation."""
        event = TradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        str_repr = str(event)
        assert "TradeExecutedEvent" in str_repr
        assert "AAPL" in str_repr
        assert "BUY" in str_repr
        assert "10" in str_repr

    def test_trade_executed_event_gross_amount(self):
        """Should calculate gross trade amount."""
        event = TradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.50")
        )

        assert event.gross_amount() == Decimal("1505.00")

    def test_trade_executed_event_different_sides(self):
        """Should handle both BUY and SELL trades."""
        buy_event = TradeExecutedEvent(
            event_id="buy-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        sell_event = TradeExecutedEvent(
            event_id="sell-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=buy_event.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("5"),
            price=Decimal("155.00")
        )

        assert buy_event.side == TradeSide.BUY
        assert sell_event.side == TradeSide.SELL
        assert buy_event.gross_amount() == Decimal("1500.00")
        assert sell_event.gross_amount() == Decimal("775.00")


class TestAssetPriceUpdatedEvent:
    """Test asset price update events."""

    def test_valid_price_updated_event_creation(self):
        """Should create valid price update event."""
        timestamp = datetime.now(timezone.utc)

        event = AssetPriceUpdatedEvent(
            event_id="price-update-123",
            timestamp=timestamp,
            symbol="AAPL",
            old_price=Decimal("150.00"),
            new_price=Decimal("155.00")
        )

        assert event.event_id == "price-update-123"
        assert event.timestamp == timestamp
        assert event.symbol == "AAPL"
        assert event.old_price == Decimal("150.00")
        assert event.new_price == Decimal("155.00")

    def test_price_updated_event_validation(self):
        """Should validate price update event data."""
        base_kwargs = {
            "event_id": "price-123",
            "timestamp": datetime.now(timezone.utc),
            "symbol": "AAPL",
            "old_price": Decimal("150.00"),
            "new_price": Decimal("155.00")
        }

        # Valid event should work
        event = AssetPriceUpdatedEvent(**base_kwargs)
        assert event.symbol == "AAPL"

        # Empty symbol should fail
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            AssetPriceUpdatedEvent(**{**base_kwargs, "symbol": ""})

        # Zero old price should fail
        with pytest.raises(DomainValidationError, match="Old price must be positive"):
            AssetPriceUpdatedEvent(**{**base_kwargs, "old_price": Decimal("0")})

        # Zero new price should fail
        with pytest.raises(DomainValidationError, match="New price must be positive"):
            AssetPriceUpdatedEvent(**{**base_kwargs, "new_price": Decimal("0")})

    def test_price_change_calculation(self):
        """Should calculate price change amounts and percentages."""
        event = AssetPriceUpdatedEvent(
            event_id="price-123",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150.00"),
            new_price=Decimal("155.00")
        )

        assert event.price_change() == Decimal("5.00")
        # 5/150 = 0.0333...
        expected_percent = Decimal("5.00") / Decimal("150.00")
        assert abs(event.price_change_percent() - expected_percent) < Decimal("0.0001")
        assert event.is_price_increase() is True
        assert event.is_price_decrease() is False

    def test_price_decrease_calculation(self):
        """Should handle price decreases correctly."""
        event = AssetPriceUpdatedEvent(
            event_id="price-123",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150.00"),
            new_price=Decimal("145.00")
        )

        assert event.price_change() == Decimal("-5.00")
        expected_percent = Decimal("-5.00") / Decimal("150.00")
        assert abs(event.price_change_percent() - expected_percent) < Decimal("0.0001")
        assert event.is_price_increase() is False
        assert event.is_price_decrease() is True

    def test_no_price_change(self):
        """Should handle no price change correctly."""
        event = AssetPriceUpdatedEvent(
            event_id="price-123",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150.00"),
            new_price=Decimal("150.00")
        )

        assert event.price_change() == Decimal("0.00")
        assert event.price_change_percent() == Decimal("0.00")
        assert event.is_price_increase() is False
        assert event.is_price_decrease() is False


class TestPositionChange:
    """Test position change value objects."""

    def test_valid_position_change_creation(self):
        """Should create valid position change."""
        change = PositionChange(
            symbol="AAPL",
            old_quantity=Decimal("10"),
            new_quantity=Decimal("15"),
            reason="TRADE_EXECUTION"
        )

        assert change.symbol == "AAPL"
        assert change.old_quantity == Decimal("10")
        assert change.new_quantity == Decimal("15")
        assert change.reason == "TRADE_EXECUTION"

    def test_position_change_validation(self):
        """Should validate position change data."""
        base_kwargs = {
            "symbol": "AAPL",
            "old_quantity": Decimal("10"),
            "new_quantity": Decimal("15"),
            "reason": "TRADE_EXECUTION"
        }

        # Valid change should work
        change = PositionChange(**base_kwargs)
        assert change.symbol == "AAPL"

        # Empty symbol should fail
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            PositionChange(**{**base_kwargs, "symbol": ""})

        # Empty reason should fail
        with pytest.raises(DomainValidationError, match="Reason cannot be empty"):
            PositionChange(**{**base_kwargs, "reason": ""})

        # Negative quantities should fail
        with pytest.raises(DomainValidationError, match="Quantities cannot be negative"):
            PositionChange(**{**base_kwargs, "old_quantity": Decimal("-5")})

        with pytest.raises(DomainValidationError, match="Quantities cannot be negative"):
            PositionChange(**{**base_kwargs, "new_quantity": Decimal("-5")})

    def test_quantity_change_calculation(self):
        """Should calculate quantity changes correctly."""
        # Increase
        change_increase = PositionChange(
            symbol="AAPL",
            old_quantity=Decimal("10"),
            new_quantity=Decimal("15"),
            reason="BUY"
        )
        assert change_increase.quantity_change() == Decimal("5")

        # Decrease
        change_decrease = PositionChange(
            symbol="AAPL",
            old_quantity=Decimal("15"),
            new_quantity=Decimal("10"),
            reason="SELL"
        )
        assert change_decrease.quantity_change() == Decimal("-5")

        # No change
        change_none = PositionChange(
            symbol="AAPL",
            old_quantity=Decimal("10"),
            new_quantity=Decimal("10"),
            reason="REBALANCE"
        )
        assert change_none.quantity_change() == Decimal("0")

    def test_position_closure(self):
        """Should handle position closure (new quantity = 0)."""
        change = PositionChange(
            symbol="AAPL",
            old_quantity=Decimal("10"),
            new_quantity=Decimal("0"),
            reason="POSITION_CLOSURE"
        )

        assert change.quantity_change() == Decimal("-10")
        assert change.new_quantity == Decimal("0")


class TestPortfolioRebalancedEvent:
    """Test portfolio rebalancing events."""

    def test_valid_portfolio_rebalanced_event_creation(self):
        """Should create valid portfolio rebalanced event."""
        portfolio_id = uuid4()
        timestamp = datetime.now(timezone.utc)
        changes = [
            PositionChange("AAPL", Decimal("10"), Decimal("15"), "BUY"),
            PositionChange("MSFT", Decimal("20"), Decimal("18"), "SELL")
        ]

        event = PortfolioRebalancedEvent(
            event_id="rebalance-123",
            timestamp=timestamp,
            portfolio_id=portfolio_id,
            changes=changes
        )

        assert event.event_id == "rebalance-123"
        assert event.timestamp == timestamp
        assert event.portfolio_id == portfolio_id
        assert len(event.changes) == 2

    def test_portfolio_rebalanced_event_validation(self):
        """Should validate portfolio rebalanced event data."""
        changes = [PositionChange("AAPL", Decimal("10"), Decimal("15"), "BUY")]

        # Valid event should work
        event = PortfolioRebalancedEvent(
            event_id="rebalance-123",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            changes=changes
        )
        assert len(event.changes) == 1

        # Empty changes should fail
        with pytest.raises(DomainValidationError, match="Changes list cannot be empty"):
            PortfolioRebalancedEvent(
                event_id="rebalance-123",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                changes=[]
            )

    def test_get_symbols_affected(self):
        """Should return set of symbols affected by rebalancing."""
        changes = [
            PositionChange("AAPL", Decimal("10"), Decimal("15"), "BUY"),
            PositionChange("MSFT", Decimal("20"), Decimal("18"), "SELL"),
            PositionChange("GOOGL", Decimal("5"), Decimal("5"), "REBALANCE")
        ]

        event = PortfolioRebalancedEvent(
            event_id="rebalance-123",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            changes=changes
        )

        symbols = event.get_symbols_affected()
        assert symbols == {"AAPL", "MSFT", "GOOGL"}

    def test_get_changes_by_symbol(self):
        """Should return changes for specific symbol."""
        changes = [
            PositionChange("AAPL", Decimal("10"), Decimal("15"), "BUY"),
            PositionChange("MSFT", Decimal("20"), Decimal("18"), "SELL"),
        ]

        event = PortfolioRebalancedEvent(
            event_id="rebalance-123",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            changes=changes
        )

        aapl_change = event.get_change_for_symbol("AAPL")
        assert aapl_change is not None
        assert aapl_change.symbol == "AAPL"
        assert aapl_change.quantity_change() == Decimal("5")

        # Non-existent symbol should return None
        non_existent = event.get_change_for_symbol("NVDA")
        assert non_existent is None


@pytest.mark.unit
class TestEventIntegration:
    """Integration tests for event system components."""

    def test_event_serialization_compatibility(self):
        """Events should be serializable for potential future persistence."""
        event = TradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Should be able to convert to dict
        event_dict = asdict(event)
        assert event_dict["symbol"] == "AAPL"
        assert event_dict["side"] == TradeSide.BUY

        # Should handle serialization with custom encoder
        def custom_serializer(obj):
            if isinstance(obj, UUID):
                return str(obj)
            elif isinstance(obj, Decimal):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, TradeSide):
                return obj.value
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        json_str = json.dumps(event_dict, default=custom_serializer)
        assert "AAPL" in json_str
        assert "BUY" in json_str

        # Should be able to deserialize back
        parsed = json.loads(json_str)
        assert parsed["symbol"] == "AAPL"
        assert parsed["side"] == "BUY"

    def test_event_type_hierarchy(self):
        """Should maintain proper type hierarchy."""
        trade_event = TradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        price_event = AssetPriceUpdatedEvent(
            event_id="price-123",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150.00"),
            new_price=Decimal("155.00")
        )

        # Both should be instances of DomainEvent
        assert isinstance(trade_event, DomainEvent)
        assert isinstance(price_event, DomainEvent)

        # Should have different types
        assert type(trade_event) != type(price_event)
        assert trade_event.__class__.__name__ == "TradeExecutedEvent"
        assert price_event.__class__.__name__ == "AssetPriceUpdatedEvent"
