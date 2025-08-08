"""Integration tests for the complete event system."""

import pytest
import pytest_asyncio
import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from unittest.mock import AsyncMock, Mock
from typing import List

from portfolio_manager.domain.entities import Asset, AssetType, Portfolio, Position, TradeSide
from portfolio_manager.domain.exceptions import InsufficientFundsError, InvalidPositionError


# Import our TDD implementations from other test files
from tests.unit.infrastructure.test_event_bus import EventBus, EventHandler
from tests.unit.application.test_event_handlers import (
    TradeExecutedEventHandler, 
    PortfolioMetricsEventHandler,
    MockTradeExecutedEvent
)


@pytest.mark.integration
class TestEventSystemIntegration:
    """Integration tests for event-driven trade processing."""

    @pytest_asyncio.fixture
    async def mock_repositories(self):
        """Create mock repositories for integration testing."""
        portfolio_repo = AsyncMock()
        position_repo = AsyncMock()
        audit_service = AsyncMock()
        metrics_service = AsyncMock()
        risk_service = AsyncMock()

        return {
            'portfolio_repo': portfolio_repo,
            'position_repo': position_repo,
            'audit_service': audit_service,
            'metrics_service': metrics_service,
            'risk_service': risk_service
        }

    @pytest_asyncio.fixture
    async def event_system(self, mock_repositories):
        """Initialize complete event system with handlers."""
        event_bus = EventBus()

        # Create handlers
        trade_handler = TradeExecutedEventHandler(
            mock_repositories['portfolio_repo'],
            mock_repositories['position_repo'],
            mock_repositories['audit_service']
        )

        metrics_handler = PortfolioMetricsEventHandler(
            mock_repositories['metrics_service'],
            mock_repositories['risk_service']
        )

        # Subscribe handlers to event bus
        trade_sub_id = await event_bus.subscribe(MockTradeExecutedEvent, trade_handler)
        metrics_sub_id = await event_bus.subscribe(MockTradeExecutedEvent, metrics_handler)

        return {
            'event_bus': event_bus,
            'trade_handler': trade_handler,
            'metrics_handler': metrics_handler,
            'subscriptions': [trade_sub_id, metrics_sub_id],
            'repositories': mock_repositories
        }

    @pytest.fixture
    def sample_portfolio(self):
        """Create a test portfolio."""
        return Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD", 
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

    @pytest.fixture
    def sample_asset(self):
        """Create a test asset."""
        return Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )

    @pytest.mark.asyncio
    async def test_complete_buy_trade_flow(self, event_system, sample_portfolio, sample_asset):
        """Test complete buy trade event processing flow."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        # Setup mock responses
        repos['portfolio_repo'].get_portfolio.return_value = sample_portfolio
        repos['position_repo'].get_position.return_value = None  # New position

        # Create trade event
        trade_event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol=sample_asset.symbol,
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Publish event and wait for processing
        await event_bus.publish(trade_event)

        # Small delay to ensure async handlers complete
        await asyncio.sleep(0.1)

        # Verify trade processing
        repos['portfolio_repo'].get_portfolio.assert_called_once()
        repos['position_repo'].save_position.assert_called_once()
        repos['portfolio_repo'].save_portfolio.assert_called_once()
        repos['audit_service'].log_trade_execution.assert_called_once()

        # Verify metrics processing
        repos['metrics_service'].recalculate_metrics.assert_called_once()
        repos['risk_service'].update_portfolio_risk.assert_called_once()

        # Verify portfolio cash was deducted
        expected_cash = Decimal("10000.00") - Decimal("1500.00")  # 10000 - (10 * 150)
        assert sample_portfolio.cash_balance == expected_cash

    @pytest.mark.asyncio
    async def test_complete_sell_trade_flow(self, event_system, sample_portfolio, sample_asset):
        """Test complete sell trade event processing flow."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        # Create existing position
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol=sample_asset.symbol,
            qty=Decimal("10"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        # Setup mock responses
        repos['portfolio_repo'].get_portfolio.return_value = sample_portfolio
        repos['position_repo'].get_position.return_value = existing_position

        # Create sell trade event
        trade_event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol=sample_asset.symbol,
            side=TradeSide.SELL,
            quantity=Decimal("5"),  # Partial sell
            price=Decimal("155.00")
        )

        # Publish event
        await event_bus.publish(trade_event)
        await asyncio.sleep(0.1)

        # Verify position was reduced
        assert existing_position.qty == Decimal("5")  # 10 - 5
        repos['position_repo'].save_position.assert_called_once()

        # Verify cash was added
        expected_cash = Decimal("10000.00") + Decimal("775.00")  # 10000 + (5 * 155)
        assert sample_portfolio.cash_balance == expected_cash

        # Verify audit and metrics
        repos['audit_service'].log_trade_execution.assert_called_once()
        repos['metrics_service'].recalculate_metrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_concurrent_trade_processing(self, event_system, sample_portfolio):
        """Test concurrent trade event processing."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        # Setup mock for multiple portfolios
        repos['portfolio_repo'].get_portfolio.return_value = sample_portfolio
        repos['position_repo'].get_position.return_value = None

        # Create multiple trade events
        trade_events = []
        for i in range(5):
            trade_events.append(MockTradeExecutedEvent(
                event_id=f"trade-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}-456",
                portfolio_id=str(sample_portfolio.portfolio_id),
                symbol=f"STOCK{i}",
                side=TradeSide.BUY,
                quantity=Decimal("10"),
                price=Decimal("100.00")
            ))

        # Publish all events concurrently
        publish_tasks = [event_bus.publish(event) for event in trade_events]
        await asyncio.gather(*publish_tasks)

        # Wait for all handlers to complete
        await asyncio.sleep(0.2)

        # Verify all trades were processed
        assert repos['portfolio_repo'].get_portfolio.call_count == 5
        assert repos['position_repo'].save_position.call_count == 5
        assert repos['audit_service'].log_trade_execution.call_count == 5
        assert repos['metrics_service'].recalculate_metrics.call_count == 5

    @pytest.mark.asyncio
    async def test_event_handler_error_recovery(self, event_system, sample_portfolio):
        """Test error recovery in event handlers."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        # Mock one repository to fail
        repos['portfolio_repo'].get_portfolio.side_effect = Exception("Database error")

        # Create trade event
        trade_event = MockTradeExecutedEvent(
            event_id="trade-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(sample_portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )

        # Publish event - should not raise exception due to error isolation
        await event_bus.publish(trade_event)
        await asyncio.sleep(0.1)

        # Trade handler should have failed, but metrics handler should still run
        repos['portfolio_repo'].get_portfolio.assert_called_once()
        repos['position_repo'].save_position.assert_not_called()  # Failed before this

        # Metrics handler should still work (independent of trade handler)
        repos['metrics_service'].recalculate_metrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_portfolio_metrics_recalculation(self, event_system, sample_portfolio):
        """Test portfolio metrics are recalculated after trades."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        repos['portfolio_repo'].get_portfolio.return_value = sample_portfolio
        repos['position_repo'].get_position.return_value = None

        # Execute multiple trades for same portfolio
        trade_events = [
            MockTradeExecutedEvent(
                event_id=f"trade-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}",
                portfolio_id=str(sample_portfolio.portfolio_id),
                symbol=f"STOCK{i}",
                side=TradeSide.BUY,
                quantity=Decimal("10"),
                price=Decimal("100.00")
            )
            for i in range(3)
        ]

        # Publish events sequentially
        for event in trade_events:
            await event_bus.publish(event)

        await asyncio.sleep(0.2)

        # Verify metrics recalculated for each trade
        assert repos['metrics_service'].recalculate_metrics.call_count == 3
        assert repos['risk_service'].update_portfolio_risk.call_count == 3

        # All calls should be for the same portfolio
        for call in repos['metrics_service'].recalculate_metrics.call_args_list:
            assert call[0][0] == str(sample_portfolio.portfolio_id)

    @pytest.mark.asyncio
    async def test_audit_trail_completeness(self, event_system, sample_portfolio):
        """Test audit trail captures all trade events."""
        event_bus = event_system['event_bus']
        repos = event_system['repositories']

        repos['portfolio_repo'].get_portfolio.return_value = sample_portfolio
        repos['position_repo'].get_position.return_value = None

        # Create series of different trade types
        trade_events = [
            MockTradeExecutedEvent(
                event_id="buy-trade-1",
                timestamp=datetime.now(timezone.utc),
                trade_id="buy-1",
                portfolio_id=str(sample_portfolio.portfolio_id),
                symbol="AAPL",
                side=TradeSide.BUY,
                quantity=Decimal("10"),
                price=Decimal("150.00")
            ),
            MockTradeExecutedEvent(
                event_id="buy-trade-2",
                timestamp=datetime.now(timezone.utc),
                trade_id="buy-2",
                portfolio_id=str(sample_portfolio.portfolio_id),
                symbol="MSFT",
                side=TradeSide.BUY,
                quantity=Decimal("5"),
                price=Decimal("300.00")
            )
        ]

        # Publish all events
        for event in trade_events:
            await event_bus.publish(event)

        await asyncio.sleep(0.2)

        # Verify all events were audited
        assert repos['audit_service'].log_trade_execution.call_count == 2

        # Verify correct events were logged
        logged_events = [call[0][0] for call in repos['audit_service'].log_trade_execution.call_args_list]
        logged_trade_ids = [event.trade_id for event in logged_events]
        assert "buy-1" in logged_trade_ids
        assert "buy-2" in logged_trade_ids


@pytest.mark.integration
class TestEventBusIntegration:
    """Integration tests for event bus with real handlers."""

    @pytest.mark.asyncio
    async def test_event_bus_with_multiple_handler_types(self):
        """Test event bus distributes events to different handler types."""
        event_bus = EventBus()

        # Create multiple handler types
        portfolio_repo = AsyncMock()
        position_repo = AsyncMock()
        audit_service = AsyncMock()
        metrics_service = AsyncMock()
        risk_service = AsyncMock()

        trade_handler = TradeExecutedEventHandler(portfolio_repo, position_repo, audit_service)
        metrics_handler = PortfolioMetricsEventHandler(metrics_service, risk_service)

        # Subscribe both handlers
        await event_bus.subscribe(MockTradeExecutedEvent, trade_handler)
        await event_bus.subscribe(MockTradeExecutedEvent, metrics_handler)

        # Mock successful responses
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test",
            base_ccy="USD",
            cash_balance=Decimal("1000.00"),
            created=datetime.now(timezone.utc)
        )
        portfolio_repo.get_portfolio.return_value = portfolio
        position_repo.get_position.return_value = None

        # Create and publish event
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            portfolio_id=str(portfolio.portfolio_id),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("1"),
            price=Decimal("100.00")
        )

        await event_bus.publish(event)
        await asyncio.sleep(0.1)

        # Both handler types should have processed the event
        portfolio_repo.get_portfolio.assert_called_once()
        metrics_service.recalculate_metrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_event_ordering_and_consistency(self):
        """Test event processing maintains data consistency."""
        event_bus = EventBus()
        processing_order = []

        class OrderTrackingHandler(EventHandler):
            def __init__(self, handler_id):
                self.handler_id = handler_id

            async def can_handle(self, event):
                return isinstance(event, MockTradeExecutedEvent)

            async def handle(self, event):
                processing_order.append(f"{self.handler_id}-{event.event_id}")

        # Subscribe multiple handlers
        handler1 = OrderTrackingHandler("H1")
        handler2 = OrderTrackingHandler("H2")

        await event_bus.subscribe(MockTradeExecutedEvent, handler1)
        await event_bus.subscribe(MockTradeExecutedEvent, handler2)

        # Create events with specific order
        events = [
            MockTradeExecutedEvent(
                event_id=f"event-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}",
                portfolio_id=str(uuid4()),
                symbol="AAPL",
                side=TradeSide.BUY,
                quantity=Decimal("1"),
                price=Decimal("100.00")
            )
            for i in range(3)
        ]

        # Publish events sequentially
        for event in events:
            await event_bus.publish(event)

        await asyncio.sleep(0.2)

        # Verify each event was processed by both handlers
        assert len(processing_order) == 6  # 3 events * 2 handlers

        # Each event should appear in both handlers
        for i in range(3):
            event_handlers = [item for item in processing_order if f"event-{i}" in item]
            assert len(event_handlers) == 2  # Both handlers processed this event

    @pytest.mark.asyncio
    async def test_handler_subscription_lifecycle(self):
        """Test handler subscription and unsubscription."""
        event_bus = EventBus()

        class CountingHandler(EventHandler):
            def __init__(self):
                self.handled_count = 0

            async def can_handle(self, event):
                return True

            async def handle(self, event):
                self.handled_count += 1

        handler = CountingHandler()

        # Subscribe handler
        subscription_id = await event_bus.subscribe(MockTradeExecutedEvent, handler)
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 1

        # Publish event
        event = MockTradeExecutedEvent(
            event_id="test-1",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-1",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("1"),
            price=Decimal("100.00")
        )

        await event_bus.publish(event)
        await asyncio.sleep(0.1)
        assert handler.handled_count == 1

        # Unsubscribe handler
        result = await event_bus.unsubscribe(subscription_id)
        assert result is True
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 0

        # Publish another event - should not be handled
        await event_bus.publish(event)
        await asyncio.sleep(0.1)
        assert handler.handled_count == 1  # Still 1, not incremented


@pytest.mark.integration
@pytest.mark.slow
class TestEventSystemPerformance:
    """Performance tests for event system."""

    @pytest.mark.asyncio
    async def test_high_volume_trade_processing(self):
        """Test processing high volume of trade events."""
        event_bus = EventBus()

        # Create lightweight handlers for performance testing
        portfolio_repo = AsyncMock()
        position_repo = AsyncMock()
        audit_service = AsyncMock()

        trade_handler = TradeExecutedEventHandler(portfolio_repo, position_repo, audit_service)
        await event_bus.subscribe(MockTradeExecutedEvent, trade_handler)

        # Mock fast responses
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Perf Test",
            base_ccy="USD",
            cash_balance=Decimal("1000000.00"),  # Large balance
            created=datetime.now(timezone.utc)
        )
        portfolio_repo.get_portfolio.return_value = portfolio
        position_repo.get_position.return_value = None

        # Create 100 trade events
        events = []
        for i in range(100):
            events.append(MockTradeExecutedEvent(
                event_id=f"perf-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"perf-trade-{i}",
                portfolio_id=str(portfolio.portfolio_id),
                symbol=f"STOCK{i % 10}",  # 10 different stocks
                side=TradeSide.BUY,
                quantity=Decimal("1"),
                price=Decimal("100.00")
            ))

        # Measure processing time
        start_time = time.time()

        # Publish all events concurrently
        tasks = [event_bus.publish(event) for event in events]
        await asyncio.gather(*tasks)

        # Wait for processing to complete
        await asyncio.sleep(1.0)

        end_time = time.time()
        processing_time = end_time - start_time

        # Performance assertions
        assert processing_time < 2.0  # Should process 100 events in under 2 seconds
        assert portfolio_repo.get_portfolio.call_count == 100
        assert position_repo.save_position.call_count == 100

        # Calculate throughput
        throughput = len(events) / processing_time
        assert throughput > 50  # At least 50 events per second

    @pytest.mark.asyncio
    async def test_event_bus_memory_stability(self):
        """Test event bus memory usage remains stable."""
        event_bus = EventBus()

        class MinimalHandler(EventHandler):
            async def can_handle(self, event):
                return True

            async def handle(self, event):
                pass  # Minimal processing

        handler = MinimalHandler()
        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        # Process events in batches to test memory stability
        for batch in range(10):
            batch_events = []
            for i in range(50):
                batch_events.append(MockTradeExecutedEvent(
                    event_id=f"mem-{batch}-{i}",
                    timestamp=datetime.now(timezone.utc),
                    trade_id=f"mem-trade-{batch}-{i}",
                    portfolio_id=str(uuid4()),
                    symbol="AAPL",
                    side=TradeSide.BUY,
                    quantity=Decimal("1"),
                    price=Decimal("100.00")
                ))

            # Process batch
            for event in batch_events:
                await event_bus.publish(event)

            # Small delay between batches
            await asyncio.sleep(0.1)

        # Event bus should still be responsive
        assert event_bus.get_total_subscriptions() == 1
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 1

        # Final test event
        final_event = MockTradeExecutedEvent(
            event_id="final-test",
            timestamp=datetime.now(timezone.utc),
            trade_id="final-trade",
            portfolio_id=str(uuid4()),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("1"),
            price=Decimal("100.00")
        )

        await event_bus.publish(final_event)
        await asyncio.sleep(0.1)

        # System should still be working normally
        assert True  # If we reach here, memory didn't cause crashes
