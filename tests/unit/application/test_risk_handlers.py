"""
Unit tests for risk management event handlers.

Tests for RiskThresholdBreachedEventHandler and RiskMitigationEventHandler.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from portfolio_manager.application.events.risk_handlers import (
    RiskThresholdBreachedEventHandler,
    RiskMitigationEventHandler,
)
from portfolio_manager.domain.events import RiskThresholdBreachedEvent, TradeExecutedEvent
from portfolio_manager.domain.entities import Portfolio


@pytest.fixture
def risk_breach_event():
    """Create a sample risk threshold breached event."""
    return RiskThresholdBreachedEvent(
        event_id="risk_breach_123",
        timestamp=datetime.now(timezone.utc),
        portfolio_id=uuid4(),
        threshold_type="MAX_POSITION_SIZE",
        threshold_value=Decimal("10000.00"),
        current_value=Decimal("12500.00"),
        severity="HIGH"
    )


@pytest.fixture
def critical_risk_breach_event():
    """Create a critical risk threshold breached event."""
    return RiskThresholdBreachedEvent(
        event_id="critical_risk_breach_456",
        timestamp=datetime.now(timezone.utc),
        portfolio_id=uuid4(),
        threshold_type="MAX_DRAWDOWN",
        threshold_value=Decimal("0.20"),
        current_value=Decimal("0.35"),
        severity="CRITICAL"
    )


@pytest.fixture
def mock_repositories():
    """Create mock repositories."""
    portfolio_repo = AsyncMock()
    return portfolio_repo


@pytest.fixture
def mock_services():
    """Create mock services."""
    risk_service = AsyncMock()
    alert_service = AsyncMock()
    trading_service = AsyncMock()
    position_service = AsyncMock()
    notification_service = AsyncMock()
    return risk_service, alert_service, trading_service, position_service, notification_service


class TestRiskThresholdBreachedEventHandler:
    """Test RiskThresholdBreachedEventHandler."""

    @pytest.mark.asyncio
    async def test_can_handle_risk_threshold_breached_event(self, mock_repositories, mock_services):
        """Test handler can handle RiskThresholdBreachedEvent."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        event = RiskThresholdBreachedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("1000"),
            current_value=Decimal("1200"),
            severity="HIGH"
        )

        assert await handler.can_handle(event) is True

    @pytest.mark.asyncio
    async def test_cannot_handle_other_events(self, mock_repositories, mock_services):
        """Test handler cannot handle other event types."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        other_event = TradeExecutedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side="BUY",
            quantity=Decimal("100"),
            price=Decimal("150")
        )

        assert await handler.can_handle(other_event) is False

    @pytest.mark.asyncio
    async def test_handle_standard_risk_breach(self, risk_breach_event, mock_repositories, mock_services):
        """Test handling of standard (non-critical) risk breach."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        # Mock portfolio
        portfolio = MagicMock()
        portfolio.portfolio_id = risk_breach_event.portfolio_id
        portfolio_repo.get_portfolio.return_value = portfolio

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        await handler.handle(risk_breach_event)

        # Verify portfolio lookup
        portfolio_repo.get_portfolio.assert_called_once_with(risk_breach_event.portfolio_id)

        # Verify standard breach handling
        risk_service.schedule_risk_review.assert_called_once_with(
            portfolio_id=risk_breach_event.portfolio_id,
            breach_type="MAX_POSITION_SIZE",
            severity="HIGH",
            review_priority="HIGH"
        )

        # Verify portfolio risk status update
        assert portfolio.risk_status == "HIGH"
        portfolio_repo.save_portfolio.assert_called()

        # Verify alert sent
        alert_service.send_risk_breach_alert.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_critical_risk_breach(self, critical_risk_breach_event, mock_repositories, mock_services):
        """Test handling of critical risk breach."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        # Mock portfolio
        portfolio = MagicMock()
        portfolio.portfolio_id = critical_risk_breach_event.portfolio_id
        portfolio_repo.get_portfolio.return_value = portfolio

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        await handler.handle(critical_risk_breach_event)

        # Verify critical breach handling
        risk_service.trigger_emergency_risk_mitigation.assert_called_once_with(
            portfolio_id=critical_risk_breach_event.portfolio_id,
            breach_type="MAX_DRAWDOWN",
            severity="CRITICAL"
        )

        # Verify critical portfolio status
        assert portfolio.risk_status == "CRITICAL"
        assert portfolio.requires_review is True
        portfolio_repo.save_portfolio.assert_called()

    @pytest.mark.asyncio
    async def test_handle_portfolio_not_found(self, risk_breach_event, mock_repositories, mock_services):
        """Test error handling when portfolio is not found."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        # Mock portfolio not found
        portfolio_repo.get_portfolio.return_value = None

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        with pytest.raises(ValueError, match="Portfolio .* not found"):
            await handler.handle(risk_breach_event)

    @pytest.mark.asyncio
    async def test_handle_risk_breach_history_tracking(self, risk_breach_event, mock_repositories, mock_services):
        """Test that risk breach history is properly tracked."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        # Mock portfolio
        portfolio = MagicMock()
        portfolio.portfolio_id = risk_breach_event.portfolio_id
        portfolio.risk_breaches = []
        portfolio_repo.get_portfolio.return_value = portfolio

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        await handler.handle(risk_breach_event)

        # Verify breach history updated
        assert len(portfolio.risk_breaches) == 1
        breach_record = portfolio.risk_breaches[0]
        assert breach_record['threshold_type'] == "MAX_POSITION_SIZE"
        assert breach_record['severity'] == "HIGH"
        assert breach_record['breach_amount'] == Decimal("2500.00")

    @pytest.mark.asyncio
    async def test_handle_medium_severity_breach(self, mock_repositories, mock_services):
        """Test handling of medium severity breach."""
        portfolio_repo = mock_repositories
        risk_service, alert_service = mock_services[0:2]

        # Create medium severity event
        event = RiskThresholdBreachedEvent(
            event_id="medium_breach",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_VOLATILITY",
            threshold_value=Decimal("0.15"),
            current_value=Decimal("0.18"),
            severity="MEDIUM"
        )

        # Mock portfolio
        portfolio = MagicMock()
        portfolio.portfolio_id = event.portfolio_id
        portfolio_repo.get_portfolio.return_value = portfolio

        handler = RiskThresholdBreachedEventHandler(
            portfolio_repo, risk_service, alert_service
        )

        await handler.handle(event)

        # Verify medium severity handling
        assert portfolio.risk_status == "MEDIUM"
        risk_service.schedule_risk_review.assert_called_once_with(
            portfolio_id=event.portfolio_id,
            breach_type="MAX_VOLATILITY",
            severity="MEDIUM",
            review_priority="MEDIUM"
        )


class TestRiskMitigationEventHandler:
    """Test RiskMitigationEventHandler."""

    @pytest.mark.asyncio
    async def test_can_handle_risk_threshold_breached_event(self, mock_services):
        """Test handler can handle RiskThresholdBreachedEvent."""
        trading_service, position_service, notification_service = mock_services[2:5]

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        event = RiskThresholdBreachedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("1000"),
            current_value=Decimal("1200"),
            severity="HIGH"
        )

        assert await handler.can_handle(event) is True

    @pytest.mark.asyncio
    async def test_handle_non_critical_breach_ignores(self, risk_breach_event, mock_services):
        """Test that non-critical breaches are ignored."""
        trading_service, position_service, notification_service = mock_services[2:5]

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        await handler.handle(risk_breach_event)  # HIGH severity, not CRITICAL

        # Verify no mitigation actions taken
        trading_service.create_hedge_positions.assert_not_called()
        trading_service.activate_stop_losses.assert_not_called()
        position_service.reduce_largest_positions.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_critical_position_size_breach(self, mock_services):
        """Test mitigation for critical position size breach."""
        trading_service, position_service, notification_service = mock_services[2:5]

        # Create critical position size breach event
        event = RiskThresholdBreachedEvent(
            event_id="critical_position",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("10000.00"),
            current_value=Decimal("15000.00"),
            severity="CRITICAL"
        )

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        await handler.handle(event)

        # Verify position reduction action
        position_service.reduce_largest_positions.assert_called_once()
        args = position_service.reduce_largest_positions.call_args[0]
        assert args[0] == str(event.portfolio_id)

        # Verify notification sent
        notification_service.send_risk_mitigation_notification.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_critical_volatility_breach(self, mock_services):
        """Test mitigation for critical portfolio volatility breach."""
        trading_service, position_service, notification_service = mock_services[2:5]

        # Create critical volatility breach event
        event = RiskThresholdBreachedEvent(
            event_id="critical_volatility",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_PORTFOLIO_VOLATILITY",
            threshold_value=Decimal("0.20"),
            current_value=Decimal("0.30"),
            severity="CRITICAL"
        )

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        await handler.handle(event)

        # Verify hedging action
        trading_service.create_hedge_positions.assert_called_once()
        args = trading_service.create_hedge_positions.call_args[0]
        assert args[0] == str(event.portfolio_id)

        # Verify notification sent
        notification_service.send_risk_mitigation_notification.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_critical_drawdown_breach(self, mock_services):
        """Test mitigation for critical drawdown breach."""
        trading_service, position_service, notification_service = mock_services[2:5]

        # Create critical drawdown breach event
        event = RiskThresholdBreachedEvent(
            event_id="critical_drawdown",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_DRAWDOWN",
            threshold_value=Decimal("0.15"),
            current_value=Decimal("0.25"),
            severity="CRITICAL"
        )

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        await handler.handle(event)

        # Verify stop loss activation
        trading_service.activate_stop_losses.assert_called_once()
        args = trading_service.activate_stop_losses.call_args[0]
        assert args[0] == str(event.portfolio_id)

        # Verify notification sent
        notification_service.send_risk_mitigation_notification.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_mitigation_error_does_not_raise(self, critical_risk_breach_event, mock_services):
        """Test that mitigation errors don't break the handler."""
        trading_service, position_service, notification_service = mock_services[2:5]

        # Mock error in position service
        position_service.reduce_largest_positions.side_effect = Exception("Position error")

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        # Should not raise exception
        await handler.handle(critical_risk_breach_event)

    @pytest.mark.asyncio
    async def test_mitigation_action_calculation(self, mock_services):
        """Test mitigation action calculation logic."""
        trading_service, position_service, notification_service = mock_services[2:5]

        # Create event with specific breach amount
        event = RiskThresholdBreachedEvent(
            event_id="test_calculation",
            timestamp=datetime.now(timezone.utc),
            portfolio_id=uuid4(),
            threshold_type="MAX_POSITION_SIZE",
            threshold_value=Decimal("10000.00"),
            current_value=Decimal("12000.00"),  # $2000 breach
            severity="CRITICAL"
        )

        handler = RiskMitigationEventHandler(
            trading_service, position_service, notification_service
        )

        await handler.handle(event)

        # Verify position reduction called with calculated amount
        position_service.reduce_largest_positions.assert_called_once()
        args = position_service.reduce_largest_positions.call_args[0]

        # Should reduce by breach amount * 1.1 = 2000 * 1.1 = 2200
        # But capped at current_value * 0.5 = 12000 * 0.5 = 6000
        # So should be min(2200, 6000) = 2200
        expected_reduction = Decimal("2200.00")
        assert args[1] == expected_reduction
