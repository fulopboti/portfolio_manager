"""Unit tests for production EventBus with recursion prevention."""

import pytest
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from unittest.mock import AsyncMock, Mock

from portfolio_manager.infrastructure.events.event_bus import EventBus, EventSubscription
from portfolio_manager.infrastructure.events.handlers import EventHandler
from portfolio_manager.domain.events import TradeExecutedEvent


class MockEventHandler(EventHandler):
    """Mock event handler for testing."""
    
    def __init__(self, can_handle_result=True):
        self.can_handle_result = can_handle_result
        self.handled_events = []
        self.can_handle_calls = []
    
    async def can_handle(self, event):
        self.can_handle_calls.append(event)
        return self.can_handle_result
    
    async def handle(self, event):
        self.handled_events.append(event)


@pytest.fixture
def event_bus():
    """Create a fresh EventBus instance for each test."""
    return EventBus()


@pytest.fixture
def sample_event():
    """Create a sample TradeExecutedEvent for testing."""
    return TradeExecutedEvent(
        event_id="test-123",
        timestamp=datetime.now(timezone.utc),
        trade_id=uuid4(),
        portfolio_id=uuid4(),
        symbol="AAPL",
        side="BUY",
        quantity=Decimal("10"),
        price=Decimal("150.00")
    )


class TestProductionEventBusRecursionPrevention:
    """Test recursion prevention in production EventBus."""
    
    @pytest.mark.asyncio
    async def test_basic_recursion_prevention(self, event_bus, sample_event):
        """Should prevent basic recursion scenarios."""
        call_count = 0
        
        class RecursiveHandler(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                nonlocal call_count
                call_count += 1
                
                # Try to publish the same event recursively
                if call_count == 1:
                    await event_bus.publish(event)
        
        handler = RecursiveHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        # Publish event - should only be handled once
        await event_bus.publish(sample_event)
        
        assert call_count == 1
        assert len(event_bus._processing_events) == 0  # Cleaned up
    
    @pytest.mark.asyncio
    async def test_recursion_prevention_with_different_event_instances(self, event_bus):
        """Should prevent recursion even with different event instances having same event_id."""
        call_count = 0
        
        class RecursiveHandler(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                nonlocal call_count
                call_count += 1
                
                if call_count == 1:
                    # Create new event with same event_id but different content
                    duplicate_event = TradeExecutedEvent(
                        event_id="same-id",  # Same as original
                        timestamp=datetime.now(timezone.utc),
                        trade_id=uuid4(),  # Different
                        portfolio_id=uuid4(),  # Different  
                        symbol="MSFT",  # Different
                        side="SELL",  # Different
                        quantity=Decimal("5"),  # Different
                        price=Decimal("200.00")  # Different
                    )
                    await event_bus.publish(duplicate_event)
        
        handler = RecursiveHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        original_event = TradeExecutedEvent(
            event_id="same-id",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side="BUY",
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )
        
        await event_bus.publish(original_event)
        
        # Should only be called once (recursion prevented by event_id)
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_no_false_positive_recursion_with_different_event_ids(self, event_bus):
        """Should allow events with different event_ids to be published normally."""
        processed_events = []
        
        class ChainHandler(EventHandler):
            def __init__(self):
                self.call_count = 0
            
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                self.call_count += 1
                processed_events.append(event.event_id)
                
                # Publish a different event (different event_id)
                if self.call_count == 1:
                    next_event = TradeExecutedEvent(
                        event_id="different-id",  # Different event_id
                        timestamp=datetime.now(timezone.utc),
                        trade_id=uuid4(),
                        portfolio_id=uuid4(),
                        symbol="MSFT",
                        side="SELL",
                        quantity=Decimal("5"),
                        price=Decimal("200.00")
                    )
                    await event_bus.publish(next_event)
        
        handler = ChainHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        initial_event = TradeExecutedEvent(
            event_id="initial-id",
            timestamp=datetime.now(timezone.utc),
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            side="BUY",
            quantity=Decimal("10"),
            price=Decimal("150.00")
        )
        
        await event_bus.publish(initial_event)
        
        # Should process both events (different event_ids)
        assert handler.call_count == 2
        assert "initial-id" in processed_events
        assert "different-id" in processed_events
        assert len(event_bus._processing_events) == 0
    
    @pytest.mark.asyncio
    async def test_recursion_prevention_memory_cleanup_on_exception(self, event_bus, sample_event):
        """Should clean up processing events even when handler throws exception."""
        class FailingRecursiveHandler(EventHandler):
            def __init__(self):
                self.call_count = 0
            
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                self.call_count += 1
                
                # Try to recurse, then fail
                if self.call_count == 1:
                    await event_bus.publish(event)  # Should be prevented
                    raise RuntimeError("Handler intentionally failed")
        
        handler = FailingRecursiveHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        # Should handle exception gracefully due to error isolation
        await event_bus.publish(sample_event)
        
        assert handler.call_count == 1
        assert len(event_bus._processing_events) == 0  # Cleaned up despite exception
    
    @pytest.mark.asyncio
    async def test_concurrent_events_no_false_recursion_detection(self, event_bus):
        """Should handle concurrent events without false recursion detection."""
        processed_events = []
        processing_lock = asyncio.Lock()
        
        class ConcurrentHandler(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                # Simulate some async work
                await asyncio.sleep(0.02)
                async with processing_lock:
                    processed_events.append(event.event_id)
        
        handler = ConcurrentHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        # Create multiple events with different IDs
        events = []
        for i in range(5):
            event = TradeExecutedEvent(
                event_id=f"concurrent-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol=f"STOCK{i}",
                side="BUY",
                quantity=Decimal("1"),
                price=Decimal("100.00")
            )
            events.append(event)
        
        # Publish all events concurrently
        tasks = [event_bus.publish(event) for event in events]
        await asyncio.gather(*tasks)
        
        # All events should be processed
        assert len(processed_events) == 5
        for i in range(5):
            assert f"concurrent-{i}" in processed_events
        
        # Processing set should be empty
        assert len(event_bus._processing_events) == 0
    
    @pytest.mark.asyncio
    async def test_recursion_prevention_logging(self, event_bus, sample_event, caplog):
        """Should log when recursion is prevented."""
        class RecursiveHandler(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                # Try to publish the same event recursively
                await event_bus.publish(event)
        
        handler = RecursiveHandler()
        await event_bus.subscribe(TradeExecutedEvent, handler)
        
        # Enable debug logging
        with caplog.at_level(logging.WARNING):
            await event_bus.publish(sample_event)
        
        # Should log recursion prevention
        warning_logs = [record for record in caplog.records if record.levelname == "WARNING"]
        assert len(warning_logs) > 0
        assert any("Recursive event publishing prevented" in record.message for record in warning_logs)
    
    @pytest.mark.asyncio
    async def test_recursion_prevention_with_multiple_handlers(self, event_bus, sample_event):
        """Should prevent recursion across multiple handlers for the same event."""
        handler1_calls = 0
        handler2_calls = 0
        
        class RecursiveHandler1(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                nonlocal handler1_calls
                handler1_calls += 1
                await event_bus.publish(event)  # Try to recurse
        
        class RecursiveHandler2(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                nonlocal handler2_calls  
                handler2_calls += 1
                await event_bus.publish(event)  # Try to recurse
        
        handler1 = RecursiveHandler1()
        handler2 = RecursiveHandler2()
        
        await event_bus.subscribe(TradeExecutedEvent, handler1)
        await event_bus.subscribe(TradeExecutedEvent, handler2)
        
        await event_bus.publish(sample_event)
        
        # Both handlers should be called once, but their recursion attempts should be prevented
        assert handler1_calls == 1
        assert handler2_calls == 1
        assert len(event_bus._processing_events) == 0
    
    @pytest.mark.asyncio
    async def test_event_key_generation_without_event_id(self, event_bus):
        """Should handle events without event_id attribute by using object id."""
        call_count = 0
        
        class SimpleEvent:
            """Event without event_id attribute."""
            def __init__(self, data):
                self.data = data
        
        class RecursiveHandler(EventHandler):
            async def can_handle(self, event):
                return True
            
            async def handle(self, event):
                nonlocal call_count
                call_count += 1
                
                if call_count == 1:
                    # Try to publish same event object (should use object id for recursion detection)
                    await event_bus.publish(event)
        
        handler = RecursiveHandler()
        await event_bus.subscribe(SimpleEvent, handler)
        
        simple_event = SimpleEvent("test-data")
        await event_bus.publish(simple_event)
        
        # Should only be called once (recursion prevented using object id)
        assert call_count == 1
        assert len(event_bus._processing_events) == 0
