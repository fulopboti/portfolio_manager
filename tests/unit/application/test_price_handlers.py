"""
Unit tests for price update event handlers.

Tests for AssetPriceUpdatedEventHandler and PortfolioRevaluationEventHandler.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from portfolio_manager.application.events.price_handlers import (
    AssetPriceUpdatedEventHandler,
    PortfolioRevaluationEventHandler,
)
from portfolio_manager.domain.events import AssetPriceUpdatedEvent, TradeExecutedEvent
from portfolio_manager.domain.entities import Portfolio, Position


@pytest.fixture
def price_update_event():
    """Create a sample asset price updated event."""
    return AssetPriceUpdatedEvent(
        event_id="price_update_AAPL_123",
        timestamp=datetime.now(timezone.utc),
        symbol="AAPL",
        old_price=Decimal("150.00"),
        new_price=Decimal("155.75")
    )


@pytest.fixture
def mock_repositories():
    """Create mock repositories."""
    portfolio_repo = AsyncMock()
    position_repo = AsyncMock()
    return portfolio_repo, position_repo


@pytest.fixture
def mock_services():
    """Create mock services."""
    market_data_service = AsyncMock()
    risk_service = AsyncMock()
    notification_service = AsyncMock()
    alert_service = AsyncMock()
    return market_data_service, risk_service, notification_service, alert_service


class TestAssetPriceUpdatedEventHandler:
    """Test AssetPriceUpdatedEventHandler."""

    @pytest.mark.asyncio
    async def test_can_handle_asset_price_updated_event(self, mock_repositories, mock_services):
        """Test handler can handle AssetPriceUpdatedEvent."""
        portfolio_repo, position_repo = mock_repositories
        market_data_service = mock_services[0]

        handler = AssetPriceUpdatedEventHandler(
            portfolio_repo, position_repo, market_data_service
        )

        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150"),
            new_price=Decimal("155")
        )

        assert await handler.can_handle(event) is True

    @pytest.mark.asyncio
    async def test_cannot_handle_other_events(self, mock_repositories, mock_services):
        """Test handler cannot handle other event types."""
        portfolio_repo, position_repo = mock_repositories
        market_data_service = mock_services[0]

        handler = AssetPriceUpdatedEventHandler(
            portfolio_repo, position_repo, market_data_service
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
    async def test_handle_price_update_success(self, price_update_event, mock_repositories, mock_services):
        """Test successful price update handling."""
        portfolio_repo, position_repo = mock_repositories
        market_data_service = mock_services[0]

        # Mock position data
        position1 = MagicMock()
        position1.portfolio_id = uuid4()
        position1.symbol = "AAPL"
        position1.market_value = Decimal("15000")

        position2 = MagicMock()
        position2.portfolio_id = uuid4()
        position2.symbol = "AAPL"
        position2.market_value = Decimal("7750")

        position_repo.find_positions_by_symbol.return_value = [position1, position2]

        # Mock portfolio data
        portfolio1 = MagicMock()
        portfolio1.portfolio_id = position1.portfolio_id
        portfolio1.total_value = Decimal("50000")

        portfolio2 = MagicMock()
        portfolio2.portfolio_id = position2.portfolio_id
        portfolio2.total_value = Decimal("25000")

        portfolio_repo.find_portfolios_by_symbol.return_value = [portfolio1, portfolio2]
        portfolio1.recalculate_total_value = AsyncMock()
        portfolio2.recalculate_total_value = AsyncMock()

        # Create handler and process event
        handler = AssetPriceUpdatedEventHandler(
            portfolio_repo, position_repo, market_data_service
        )

        await handler.handle(price_update_event)

        # Verify position updates
        position_repo.find_positions_by_symbol.assert_called_once_with("AAPL")
        assert position_repo.save_position.call_count == 2

        # Verify portfolio updates
        portfolio_repo.find_portfolios_by_symbol.assert_called_once_with("AAPL")
        portfolio1.recalculate_total_value.assert_called_once()
        portfolio2.recalculate_total_value.assert_called_once()
        assert portfolio_repo.save_portfolio.call_count == 2

        # Verify market data storage
        market_data_service.store_price_update.assert_called_once_with(
            symbol="AAPL",
            price=Decimal("155.75"),
            timestamp=price_update_event.timestamp
        )

    @pytest.mark.asyncio
    async def test_handle_price_update_no_positions(self, price_update_event, mock_repositories, mock_services):
        """Test price update handling when no positions exist for symbol."""
        portfolio_repo, position_repo = mock_repositories
        market_data_service = mock_services[0]

        # Mock empty position list
        position_repo.find_positions_by_symbol.return_value = []
        portfolio_repo.find_portfolios_by_symbol.return_value = []

        handler = AssetPriceUpdatedEventHandler(
            portfolio_repo, position_repo, market_data_service
        )

        await handler.handle(price_update_event)

        # Verify no position/portfolio updates
        position_repo.save_position.assert_not_called()
        portfolio_repo.save_portfolio.assert_not_called()

        # Verify market data still stored
        market_data_service.store_price_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_price_update_error_propagation(self, price_update_event, mock_repositories, mock_services):
        """Test error propagation during price update handling."""
        portfolio_repo, position_repo = mock_repositories
        market_data_service = mock_services[0]

        # Mock error in position lookup
        position_repo.find_positions_by_symbol.side_effect = Exception("Database error")

        handler = AssetPriceUpdatedEventHandler(
            portfolio_repo, position_repo, market_data_service
        )

        with pytest.raises(Exception, match="Database error"):
            await handler.handle(price_update_event)


class TestPortfolioRevaluationEventHandler:
    """Test PortfolioRevaluationEventHandler."""

    @pytest.mark.asyncio
    async def test_can_handle_asset_price_updated_event(self, mock_services):
        """Test handler can handle AssetPriceUpdatedEvent."""
        risk_service, notification_service, alert_service = mock_services[1:4]

        handler = PortfolioRevaluationEventHandler(
            risk_service, notification_service, alert_service
        )

        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("150"),
            new_price=Decimal("155")
        )

        assert await handler.can_handle(event) is True

    @pytest.mark.asyncio
    async def test_handle_significant_price_change(self, mock_services):
        """Test handling of significant price changes."""
        risk_service, notification_service, alert_service = mock_services[1:4]

        # Create event with significant price change (>5%)
        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("110.00")  # 10% increase
        )

        handler = PortfolioRevaluationEventHandler(
            risk_service, notification_service, alert_service
        )

        # Mock portfolio repository for risk metrics update
        handler.portfolio_repository = AsyncMock()
        portfolio = MagicMock()
        portfolio.portfolio_id = uuid4()
        handler.portfolio_repository.find_portfolios_by_symbol.return_value = [portfolio]

        await handler.handle(event)

        # Verify risk metrics update called
        risk_service.update_portfolio_risk.assert_called_once_with(portfolio.portfolio_id)

        # Verify threshold check called
        alert_service.check_price_change_thresholds.assert_called_once_with(
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("110.00"),
            timestamp=event.timestamp
        )

        # Verify notification sent
        notification_service.send_price_alert.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_minor_price_change(self, mock_services):
        """Test handling of minor price changes (no action needed)."""
        risk_service, notification_service, alert_service = mock_services[1:4]

        # Create event with minor price change (<5%)
        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("102.00")  # 2% increase
        )

        handler = PortfolioRevaluationEventHandler(
            risk_service, notification_service, alert_service
        )

        await handler.handle(event)

        # Verify no services called for minor changes
        risk_service.update_portfolio_risk.assert_not_called()
        alert_service.check_price_change_thresholds.assert_not_called()
        notification_service.send_price_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_error_handling_does_not_raise(self, mock_services):
        """Test that errors in revaluation don't break the handler."""
        risk_service, notification_service, alert_service = mock_services[1:4]

        # Create event with significant price change
        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("110.00")
        )

        # Mock error in risk service
        risk_service.update_portfolio_risk.side_effect = Exception("Risk calculation error")

        handler = PortfolioRevaluationEventHandler(
            risk_service, notification_service, alert_service
        )

        # Mock portfolio repository
        handler.portfolio_repository = AsyncMock()
        handler.portfolio_repository.find_portfolios_by_symbol.return_value = []

        # Should not raise exception
        await handler.handle(event)

    @pytest.mark.asyncio
    async def test_handle_price_decrease_significant(self, mock_services):
        """Test handling of significant price decreases."""
        risk_service, notification_service, alert_service = mock_services[1:4]

        # Create event with significant price decrease
        event = AssetPriceUpdatedEvent(
            event_id="test",
            timestamp=datetime.now(timezone.utc),
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("90.00")  # 10% decrease
        )

        handler = PortfolioRevaluationEventHandler(
            risk_service, notification_service, alert_service
        )

        # Mock portfolio repository
        handler.portfolio_repository = AsyncMock()
        handler.portfolio_repository.find_portfolios_by_symbol.return_value = []

        await handler.handle(event)

        # Verify threshold check called for decrease
        alert_service.check_price_change_thresholds.assert_called_once_with(
            symbol="AAPL",
            old_price=Decimal("100.00"),
            new_price=Decimal("90.00"),
            timestamp=event.timestamp
        )
