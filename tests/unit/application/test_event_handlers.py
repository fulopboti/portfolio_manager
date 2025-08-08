"""Unit tests for application event handlers."""

import pytest
import logging
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from unittest.mock import Mock, AsyncMock, call
from abc import ABC, abstractmethod
from dataclasses import dataclass

from portfolio_manager.domain.entities import TradeSide, Portfolio, Position
from portfolio_manager.domain.exceptions import (
    InsufficientFundsError, 
    InvalidPositionError, 
    DomainValidationError
)


# Event Handler Implementation for TDD
@dataclass(frozen=True)
class MockTradeExecutedEvent:
    """Mock trade executed event for testing."""
    event_id: str
    timestamp: datetime
    trade_id: str
    portfolio_id: str
    symbol: str
    side: TradeSide
    quantity: Decimal
    price: Decimal

    def gross_amount(self):
        return self.quantity * self.price


class EventHandler(ABC):
    """Abstract base class for event handlers."""

    @abstractmethod
    async def can_handle(self, event) -> bool:
        """Check if this handler can handle the given event."""
        pass

    @abstractmethod
    async def handle(self, event) -> None:
        """Handle the given event."""
        pass


class TradeExecutedEventHandler(EventHandler):
    """Handler for trade execution events."""

    def __init__(self, portfolio_repository, position_repository, audit_service):
        self.portfolio_repository = portfolio_repository
        self.position_repository = position_repository
        self.audit_service = audit_service
        self._logger = logging.getLogger(__name__)

    async def can_handle(self, event) -> bool:
        """Check if event is a trade execution event."""
        return isinstance(event, MockTradeExecutedEvent)

    async def handle(self, event: MockTradeExecutedEvent) -> None:
        """Handle trade execution event."""
        try:
            # Get portfolio
            portfolio = await self.portfolio_repository.get_portfolio(event.portfolio_id)
            if not portfolio:
                raise ValueError(f"Portfolio {event.portfolio_id} not found")

            # Process trade based on side
            if event.side == TradeSide.BUY:
                await self._handle_buy_trade(event, portfolio)
            elif event.side == TradeSide.SELL:
                await self._handle_sell_trade(event, portfolio)

            # Update portfolio cash
            await self._update_portfolio_cash(event, portfolio)

            # Save updated portfolio
            await self.portfolio_repository.save_portfolio(portfolio)

            # Log audit event
            await self.audit_service.log_trade_execution(event)

            self._logger.info(f"Successfully processed trade {event.trade_id}")

        except Exception as e:
            self._logger.error(f"Failed to process trade {event.trade_id}: {e}")
            raise

    async def _handle_buy_trade(self, event: MockTradeExecutedEvent, portfolio: Portfolio) -> None:
        """Handle buy trade - create or update position."""
        # Check sufficient funds
        trade_cost = event.gross_amount()
        if not portfolio.has_sufficient_cash(trade_cost):
            raise InsufficientFundsError(
                f"Insufficient funds: need {trade_cost}, have {portfolio.cash_balance}"
            )

        # Get existing position
        existing_position = await self.position_repository.get_position(
            event.portfolio_id, event.symbol
        )

        if existing_position:
            # Update existing position
            existing_position.add_shares(event.quantity, event.price)
            existing_position.last_updated = event.timestamp
            await self.position_repository.save_position(existing_position)
        else:
            # Create new position
            new_position = Position(
                portfolio_id=event.portfolio_id,
                symbol=event.symbol,
                qty=event.quantity,
                avg_cost=event.price,
                unit="share",
                price_ccy="USD",
                last_updated=event.timestamp
            )
            await self.position_repository.save_position(new_position)

    async def _handle_sell_trade(self, event: MockTradeExecutedEvent, portfolio: Portfolio) -> None:
        """Handle sell trade - reduce or close position."""
        # Get existing position
        existing_position = await self.position_repository.get_position(
            event.portfolio_id, event.symbol
        )

        if not existing_position:
            raise InvalidPositionError(f"No position found for {event.symbol}")

        if existing_position.qty < event.quantity:
            raise InvalidPositionError(
                f"Insufficient position: have {existing_position.qty}, selling {event.quantity}"
            )

        # Reduce position
        if existing_position.qty == event.quantity:
            # Close position completely
            await self.position_repository.delete_position(event.portfolio_id, event.symbol)
        else:
            # Reduce position
            existing_position.reduce_shares(event.quantity)
            existing_position.last_updated = event.timestamp
            await self.position_repository.save_position(existing_position)

    async def _update_portfolio_cash(self, event: MockTradeExecutedEvent, portfolio: Portfolio) -> None:
        """Update portfolio cash based on trade."""
        trade_amount = event.gross_amount()

        if event.side == TradeSide.BUY:
            portfolio.deduct_cash(trade_amount)
        elif event.side == TradeSide.SELL:
            portfolio.add_cash(trade_amount)


class PortfolioMetricsEventHandler(EventHandler):
    """Handler for portfolio metrics recalculation."""

    def __init__(self, portfolio_metrics_service, risk_service):
        self.portfolio_metrics_service = portfolio_metrics_service
        self.risk_service = risk_service
        self._logger = logging.getLogger(__name__)

    async def can_handle(self, event) -> bool:
        """Handle position change events."""
        return isinstance(event, MockTradeExecutedEvent)

    async def handle(self, event: MockTradeExecutedEvent) -> None:
        """Recalculate portfolio metrics after position changes."""
        try:
            # Recalculate portfolio metrics
            await self.portfolio_metrics_service.recalculate_metrics(event.portfolio_id)

            # Update risk metrics
            await self.risk_service.update_portfolio_risk(event.portfolio_id)

            self._logger.info(f"Updated metrics for portfolio {event.portfolio_id}")

        except Exception as e:
            self._logger.error(f"Failed to update metrics for portfolio {event.portfolio_id}: {e}")
            # Don't re-raise - metrics are not critical for trade processing


class TestTradeExecutedEventHandler:
    """Test trade execution event handler."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Mock portfolio repository."""
        return AsyncMock()

    @pytest.fixture
    def mock_position_repository(self):
        """Mock position repository."""
        return AsyncMock()

    @pytest.fixture
    def mock_audit_service(self):
        """Mock audit service."""
        return AsyncMock()

    @pytest.fixture
    def trade_event_handler(self, mock_portfolio_repository, mock_position_repository, mock_audit_service):
        """Create trade event handler with mocked dependencies."""
        return TradeExecutedEventHandler(
            mock_portfolio_repository,
            mock_position_repository,
            mock_audit_service
        )

    @pytest.fixture
    def sample_portfolio(self):
        """Create sample portfolio for testing."""
        return Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

    @pytest.mark.asyncio
    async def test_can_handle_trade_events(self, trade_event_handler):
        """Should handle trade executed events."""
        trade_event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        can_handle = await trade_event_handler.can_handle(trade_event)
        assert can_handle is True

        # Should not handle other event types
        other_event = {"type": "other"}
        can_handle_other = await trade_event_handler.can_handle(other_event)
        assert can_handle_other is False

    @pytest.mark.asyncio
    async def test_handle_buy_trade_new_position(self, trade_event_handler, mock_portfolio_repository, 
                                                 mock_position_repository, mock_audit_service, sample_portfolio):
        """Should create new position for buy trade when none exists."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Mock portfolio and no existing position
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = None

        await trade_event_handler.handle(event)

        # Verify position creation
        mock_position_repository.save_position.assert_called_once()
        saved_position = mock_position_repository.save_position.call_args[0][0]
        assert saved_position.symbol == "AAPL"
        assert saved_position.qty == Decimal("10")
        assert saved_position.avg_cost == Decimal("150.00")

        # Verify portfolio cash deduction
        expected_cash = Decimal("10000.00") - Decimal("1500.00")  # 10000 - (10 * 150)
        assert sample_portfolio.cash_balance == expected_cash

        # Verify portfolio saved
        mock_portfolio_repository.save_portfolio.assert_called_once_with(sample_portfolio)

        # Verify audit logging
        mock_audit_service.log_trade_execution.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_handle_buy_trade_existing_position(self, trade_event_handler, mock_portfolio_repository,
                                                     mock_position_repository, mock_audit_service, sample_portfolio):
        """Should update existing position for buy trade."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("5"),
            price=Decimal("160.00")
        )

        # Mock existing position
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("10"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = existing_position

        await trade_event_handler.handle(event)

        # Verify position update - should now have 15 shares with weighted average cost
        # Original: 10 @ 150 = 1500
        # New: 5 @ 160 = 800
        # Total: 15 @ 153.33 average
        assert existing_position.qty == Decimal("15")
        expected_avg_cost = (Decimal("10") * Decimal("150.00") + Decimal("5") * Decimal("160.00")) / Decimal("15")
        assert abs(existing_position.avg_cost - expected_avg_cost) < Decimal("0.01")

        mock_position_repository.save_position.assert_called_once_with(existing_position)

    @pytest.mark.asyncio
    async def test_handle_sell_trade_sufficient_position(self, trade_event_handler, mock_portfolio_repository,
                                                        mock_position_repository, mock_audit_service, sample_portfolio):
        """Should reduce position for sell trade when sufficient quantity exists."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("5"),
            price=Decimal("155.00")
        )

        # Mock existing position with sufficient quantity
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("10"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = existing_position

        await trade_event_handler.handle(event)

        # Verify position reduction
        assert existing_position.qty == Decimal("5")  # 10 - 5
        assert existing_position.avg_cost == Decimal("150.00")  # Avg cost unchanged

        # Verify cash addition
        expected_cash = Decimal("10000.00") + Decimal("775.00")  # 10000 + (5 * 155)
        assert sample_portfolio.cash_balance == expected_cash

        mock_position_repository.save_position.assert_called_once_with(existing_position)

    @pytest.mark.asyncio
    async def test_handle_sell_trade_close_position(self, trade_event_handler, mock_portfolio_repository,
                                                   mock_position_repository, mock_audit_service, sample_portfolio):
        """Should close position when selling entire quantity."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("10"),
            price=Decimal("155.00")
        )

        # Mock existing position with exact quantity
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("10"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = existing_position

        await trade_event_handler.handle(event)

        # Verify position deletion
        mock_position_repository.delete_position.assert_called_once_with(
            str(sample_portfolio.portfolio_id), "AAPL"
        )

        # Verify cash addition
        expected_cash = Decimal("10000.00") + Decimal("1550.00")  # 10000 + (10 * 155)
        assert sample_portfolio.cash_balance == expected_cash

    @pytest.mark.asyncio
    async def test_handle_sell_trade_insufficient_position(self, trade_event_handler, mock_portfolio_repository,
                                                          mock_position_repository, sample_portfolio):
        """Should handle error when selling more than available."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("15"),  # More than available
            price=Decimal("155.00")
        )

        # Mock existing position with insufficient quantity
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("10"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = existing_position

        # Should raise InvalidPositionError
        with pytest.raises(InvalidPositionError, match="Insufficient position"):
            await trade_event_handler.handle(event)

        # Should not save anything on error
        mock_position_repository.save_position.assert_not_called()
        mock_portfolio_repository.save_portfolio.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_sell_trade_no_position(self, trade_event_handler, mock_portfolio_repository,
                                                mock_position_repository, sample_portfolio):
        """Should handle error when selling with no position."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("10"),
            price=Decimal("155.00")
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_position_repository.get_position.return_value = None  # No position

        # Should raise InvalidPositionError
        with pytest.raises(InvalidPositionError, match="No position found"):
            await trade_event_handler.handle(event)

    @pytest.mark.asyncio
    async def test_handle_buy_trade_insufficient_funds(self, trade_event_handler, mock_portfolio_repository,
                                                      mock_position_repository):
        """Should handle insufficient funds error."""
        # Portfolio with insufficient cash
        poor_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Poor Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("100.00"),  # Not enough for trade
            created=datetime.now(timezone.utc)
        )

        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(poor_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")  # Needs 1500, only has 100
        )

        mock_portfolio_repository.get_portfolio.return_value = poor_portfolio
        mock_position_repository.get_position.return_value = None

        # Should raise InsufficientFundsError
        with pytest.raises(InsufficientFundsError, match="Insufficient funds"):
            await trade_event_handler.handle(event)

    @pytest.mark.asyncio
    async def test_handle_portfolio_not_found(self, trade_event_handler, mock_portfolio_repository):
        """Should handle portfolio not found error."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id="non-existent-portfolio",
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        mock_portfolio_repository.get_portfolio.return_value = None  # Portfolio not found

        # Should raise ValueError
        with pytest.raises(ValueError, match="Portfolio .* not found"):
            await trade_event_handler.handle(event)

    @pytest.mark.asyncio
    async def test_handler_error_handling(self, trade_event_handler, mock_portfolio_repository, mock_audit_service):
        """Should handle repository errors gracefully."""
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Mock repository error
        mock_portfolio_repository.get_portfolio.side_effect = Exception("Database error")

        # Should re-raise the exception
        with pytest.raises(Exception, match="Database error"):
            await trade_event_handler.handle(event)

        # Audit should not be called if processing fails early
        mock_audit_service.log_trade_execution.assert_not_called()


class TestPortfolioMetricsEventHandler:
    """Test portfolio metrics event handler."""

    @pytest.fixture
    def mock_portfolio_metrics_service(self):
        """Mock portfolio metrics service."""
        return AsyncMock()

    @pytest.fixture
    def mock_risk_service(self):
        """Mock risk calculation service."""
        return AsyncMock()

    @pytest.fixture
    def metrics_handler(self, mock_portfolio_metrics_service, mock_risk_service):
        """Create portfolio metrics event handler."""
        return PortfolioMetricsEventHandler(
            mock_portfolio_metrics_service,
            mock_risk_service
        )

    @pytest.mark.asyncio
    async def test_can_handle_trade_events(self, metrics_handler):
        """Should handle trade events for metrics recalculation."""
        trade_event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        can_handle = await metrics_handler.can_handle(trade_event)
        assert can_handle is True

    @pytest.mark.asyncio
    async def test_recalculate_portfolio_metrics(self, metrics_handler, mock_portfolio_metrics_service, mock_risk_service):
        """Should trigger portfolio metrics recalculation."""
        portfolio_id = str(uuid4())
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        await metrics_handler.handle(event)

        # Verify metrics recalculation
        mock_portfolio_metrics_service.recalculate_metrics.assert_called_once_with(portfolio_id)

        # Verify risk metrics update
        mock_risk_service.update_portfolio_risk.assert_called_once_with(portfolio_id)

    @pytest.mark.asyncio
    async def test_handle_metrics_service_error(self, metrics_handler, mock_portfolio_metrics_service, mock_risk_service):
        """Should handle metrics service errors gracefully."""
        portfolio_id = str(uuid4())
        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Mock metrics service error
        mock_portfolio_metrics_service.recalculate_metrics.side_effect = Exception("Metrics calculation failed")

        # Should not raise exception - metrics failures should not break trade processing
        await metrics_handler.handle(event)

        # Risk service should not be called if metrics calculation fails
        mock_risk_service.update_portfolio_risk.assert_not_called()


@pytest.mark.unit
class TestEventHandlerIntegration:
    """Integration tests for event handlers."""

    @pytest.mark.asyncio
    async def test_complete_trade_processing_flow(self):
        """Should handle complete trade processing workflow."""
        # Create real handler instances with mocks
        portfolio_repo = AsyncMock()
        position_repo = AsyncMock()
        audit_service = AsyncMock()
        metrics_service = AsyncMock()
        risk_service = AsyncMock()

        trade_handler = TradeExecutedEventHandler(portfolio_repo, position_repo, audit_service)
        metrics_handler = PortfolioMetricsEventHandler(metrics_service, risk_service)

        # Create test data
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Mock repository responses
        portfolio_repo.get_portfolio.return_value = portfolio
        position_repo.get_position.return_value = None  # New position

        # Process trade
        await trade_handler.handle(event)

        # Process metrics update
        await metrics_handler.handle(event)

        # Verify complete flow
        portfolio_repo.get_portfolio.assert_called_once()
        position_repo.save_position.assert_called_once()
        portfolio_repo.save_portfolio.assert_called_once()
        audit_service.log_trade_execution.assert_called_once()
        metrics_service.recalculate_metrics.assert_called_once()
        risk_service.update_portfolio_risk.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_recovery_scenarios(self):
        """Should handle various error scenarios gracefully."""
        # Test that metrics handler continues even if trade handler fails
        portfolio_repo = AsyncMock()
        position_repo = AsyncMock()
        audit_service = AsyncMock()
        metrics_service = AsyncMock()
        risk_service = AsyncMock()

        trade_handler = TradeExecutedEventHandler(portfolio_repo, position_repo, audit_service)
        metrics_handler = PortfolioMetricsEventHandler(metrics_service, risk_service)

        event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Mock trade handler failure
        portfolio_repo.get_portfolio.side_effect = Exception("Database error")

        # Trade handler should fail
        with pytest.raises(Exception, match="Database error"):
            await trade_handler.handle(event)

        # Metrics handler should still work independently
        await metrics_handler.handle(event)
        metrics_service.recalculate_metrics.assert_called_once()
