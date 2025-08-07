"""
Unit tests for new domain events.

Tests for RiskThresholdBreachedEvent, MarketDataReceivedEvent, and PortfolioCreatedEvent.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4

from portfolio_manager.domain.events import (
    RiskThresholdBreachedEvent,
    MarketDataReceivedEvent,
    PortfolioCreatedEvent,
)
from portfolio_manager.domain.exceptions import DomainValidationError


class TestRiskThresholdBreachedEvent:
    """Test RiskThresholdBreachedEvent domain event."""

    def test_create_valid_risk_threshold_event(self):
        """Test creating a valid risk threshold breached event."""
        portfolio_id = uuid4()
        event_id = f"risk_breach_{portfolio_id}"
        timestamp = datetime.now(timezone.utc)
        
        event = RiskThresholdBreachedEvent(
            event_id=event_id,
            timestamp=timestamp,
            portfolio_id=portfolio_id,
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("10000.00"),
            current_value=Decimal("12000.00"),
            severity="HIGH"
        )
        
        assert event.event_id == event_id
        assert event.timestamp == timestamp
        assert event.portfolio_id == portfolio_id
        assert event.threshold_type == "MAX_POSITION_SIZE"
        assert event.threshold_value == Decimal("10000.00")
        assert event.current_value == Decimal("12000.00")
        assert event.severity == "HIGH"

    def test_risk_threshold_event_validation_empty_threshold_type(self):
        """Test validation fails with empty threshold type."""
        with pytest.raises(DomainValidationError, match="Threshold type cannot be empty"):
            RiskThresholdBreachedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                threshold_type="",
                threshold_value=Decimal("1000"),
                current_value=Decimal("1200"),
                severity="HIGH"
            )

    def test_risk_threshold_event_validation_empty_severity(self):
        """Test validation fails with empty severity."""
        with pytest.raises(DomainValidationError, match="Severity cannot be empty"):
            RiskThresholdBreachedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                threshold_type="MAX_POSITION_SIZE",
                threshold_value=Decimal("1000"),
                current_value=Decimal("1200"),
                severity=""
            )

    def test_risk_threshold_event_validation_invalid_severity(self):
        """Test validation fails with invalid severity."""
        with pytest.raises(DomainValidationError, match="Severity must be one of"):
            RiskThresholdBreachedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                threshold_type="MAX_POSITION_SIZE",
                threshold_value=Decimal("1000"),
                current_value=Decimal("1200"),
                severity="INVALID"
            )

    def test_risk_threshold_event_valid_severities(self):
        """Test all valid severity values."""
        valid_severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        
        for severity in valid_severities:
            event = RiskThresholdBreachedEvent(
                event_id=f"test_event_{severity}",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                threshold_type="MAX_POSITION_SIZE",
                threshold_value=Decimal("1000"),
                current_value=Decimal("1200"),
                severity=severity
            )
            assert event.severity == severity

    def test_breach_amount_calculation(self):
        """Test breach amount calculation."""
        event = RiskThresholdBreachedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("1000.00"),
            current_value=Decimal("1250.00"),
            severity="HIGH"
        )
        
        assert event.breach_amount() == Decimal("250.00")

    def test_breach_amount_calculation_negative_breach(self):
        """Test breach amount calculation when current is below threshold."""
        event = RiskThresholdBreachedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MIN_CASH_BALANCE",
            threshold_value=Decimal("5000.00"),
            current_value=Decimal("3000.00"),
            severity="MEDIUM"
        )
        
        assert event.breach_amount() == Decimal("2000.00")

    def test_is_critical_breach(self):
        """Test critical breach detection."""
        critical_event = RiskThresholdBreachedEvent(
            event_id="critical_event",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_DRAWDOWN",
            threshold_value=Decimal("0.20"),
            current_value=Decimal("0.35"),
            severity="CRITICAL"
        )
        
        high_event = RiskThresholdBreachedEvent(
            event_id="high_event",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("1000"),
            current_value=Decimal("1200"),
            severity="HIGH"
        )
        
        assert critical_event.is_critical_breach() is True
        assert high_event.is_critical_breach() is False


class TestMarketDataReceivedEvent:
    """Test MarketDataReceivedEvent domain event."""

    def test_create_valid_market_data_event(self):
        """Test creating a valid market data received event."""
        event_id = "market_data_AAPL_123"
        timestamp = datetime.now(timezone.utc)
        
        event = MarketDataReceivedEvent(
            event_id=event_id,
            timestamp=timestamp,
            symbol="AAPL",
            price=Decimal("150.25"),
            volume=1000000,
            market_cap=Decimal("2500000000000")
        )
        
        assert event.event_id == event_id
        assert event.timestamp == timestamp
        assert event.symbol == "AAPL"
        assert event.price == Decimal("150.25")
        assert event.volume == 1000000
        assert event.market_cap == Decimal("2500000000000")

    def test_market_data_event_without_market_cap(self):
        """Test creating market data event without market cap."""
        event = MarketDataReceivedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            price=Decimal("150.25"),
            volume=1000000
        )
        
        assert event.market_cap is None

    def test_market_data_event_validation_empty_symbol(self):
        """Test validation fails with empty symbol."""
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            MarketDataReceivedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                symbol="",
                price=Decimal("150.25"),
                volume=1000000
            )

    def test_market_data_event_validation_zero_price(self):
        """Test validation fails with zero price."""
        with pytest.raises(DomainValidationError, match="Price must be positive"):
            MarketDataReceivedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                symbol="AAPL",
                price=Decimal("0"),
                volume=1000000
            )

    def test_market_data_event_validation_negative_price(self):
        """Test validation fails with negative price."""
        with pytest.raises(DomainValidationError, match="Price must be positive"):
            MarketDataReceivedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                symbol="AAPL",
                price=Decimal("-50.00"),
                volume=1000000
            )

    def test_market_data_event_validation_negative_volume(self):
        """Test validation fails with negative volume."""
        with pytest.raises(DomainValidationError, match="Volume cannot be negative"):
            MarketDataReceivedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                symbol="AAPL",
                price=Decimal("150.25"),
                volume=-1000
            )

    def test_market_data_event_validation_zero_volume_allowed(self):
        """Test validation allows zero volume."""
        event = MarketDataReceivedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            price=Decimal("150.25"),
            volume=0
        )
        assert event.volume == 0

    def test_market_data_event_validation_negative_market_cap(self):
        """Test validation fails with negative market cap."""
        with pytest.raises(DomainValidationError, match="Market cap cannot be negative"):
            MarketDataReceivedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                symbol="AAPL",
                price=Decimal("150.25"),
                volume=1000000,
                market_cap=Decimal("-1000000")
            )

    def test_market_data_event_validation_zero_market_cap_allowed(self):
        """Test validation allows zero market cap."""
        event = MarketDataReceivedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            price=Decimal("150.25"),
            volume=1000000,
            market_cap=Decimal("0")
        )
        assert event.market_cap == Decimal("0")


class TestPortfolioCreatedEvent:
    """Test PortfolioCreatedEvent domain event."""

    def test_create_valid_portfolio_created_event(self):
        """Test creating a valid portfolio created event."""
        portfolio_id = uuid4()
        event_id = f"portfolio_created_{portfolio_id}"
        timestamp = datetime.now(timezone.utc)
        
        event = PortfolioCreatedEvent(
            event_id=event_id,
            timestamp=timestamp,
            portfolio_id=portfolio_id,
            owner_id="user123",
            initial_cash=Decimal("100000.00"),
            strategy="balanced_growth"
        )
        
        assert event.event_id == event_id
        assert event.timestamp == timestamp
        assert event.portfolio_id == portfolio_id
        assert event.owner_id == "user123"
        assert event.initial_cash == Decimal("100000.00")
        assert event.strategy == "balanced_growth"

    def test_portfolio_created_event_validation_empty_owner_id(self):
        """Test validation fails with empty owner ID."""
        with pytest.raises(DomainValidationError, match="Owner ID cannot be empty"):
            PortfolioCreatedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                owner_id="",
                initial_cash=Decimal("100000.00"),
                strategy="balanced_growth"
            )

    def test_portfolio_created_event_validation_negative_initial_cash(self):
        """Test validation fails with negative initial cash."""
        with pytest.raises(DomainValidationError, match="Initial cash cannot be negative"):
            PortfolioCreatedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                owner_id="user123",
                initial_cash=Decimal("-1000.00"),
                strategy="balanced_growth"
            )

    def test_portfolio_created_event_validation_zero_initial_cash_allowed(self):
        """Test validation allows zero initial cash."""
        event = PortfolioCreatedEvent(
            event_id="test_event",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            owner_id="user123",
            initial_cash=Decimal("0.00"),
            strategy="balanced_growth"
        )
        assert event.initial_cash == Decimal("0.00")

    def test_portfolio_created_event_validation_empty_strategy(self):
        """Test validation fails with empty strategy."""
        with pytest.raises(DomainValidationError, match="Strategy cannot be empty"):
            PortfolioCreatedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                owner_id="user123",
                initial_cash=Decimal("100000.00"),
                strategy=""
            )

    def test_portfolio_created_event_validation_whitespace_only_fields(self):
        """Test validation fails with whitespace-only fields."""
        with pytest.raises(DomainValidationError, match="Owner ID cannot be empty"):
            PortfolioCreatedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                owner_id="   ",
                initial_cash=Decimal("100000.00"),
                strategy="balanced_growth"
            )
        
        with pytest.raises(DomainValidationError, match="Strategy cannot be empty"):
            PortfolioCreatedEvent(
                event_id="test_event",
                timestamp=datetime.now(timezone.utc),
                portfolio_id=uuid4(),
                owner_id="user123",
                initial_cash=Decimal("100000.00"),
                strategy="   "
            )
