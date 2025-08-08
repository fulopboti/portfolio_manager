"""Unit tests for event bus infrastructure."""

import pytest
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from typing import List, Any, Dict, Set, Type, Callable, Awaitable
from unittest.mock import Mock, AsyncMock, call
from collections import defaultdict

from portfolio_manager.domain.entities import TradeSide


# Event Bus Implementation for TDD
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class MockDomainEvent:
    """Mock domain event for testing."""
    event_id: str
    timestamp: datetime


@dataclass(frozen=True)
class MockTradeExecutedEvent(MockDomainEvent):
    """Mock trade executed event for testing."""
    trade_id: str
    symbol: str
    side: TradeSide = TradeSide.BUY
    quantity: Decimal = Decimal("10")
    price: Decimal = Decimal("150.00")


class EventHandler(ABC):
    """Abstract base class for event handlers."""

    @abstractmethod
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can handle the given event."""
        pass

    @abstractmethod
    async def handle(self, event: Any) -> None:
        """Handle the given event."""
        pass


class EventSubscription:
    """Represents an event subscription."""

    def __init__(self, event_type: Type, handler: EventHandler, subscription_id: str):
        self.event_type = event_type
        self.handler = handler
        self.subscription_id = subscription_id
        self.is_active = True

    def deactivate(self):
        """Deactivate this subscription."""
        self.is_active = False


class EventBus:
    """In-memory event bus for local-first architecture."""

    def __init__(self):
        self._subscriptions: Dict[Type, List[EventSubscription]] = defaultdict(list)
        self._handler_subscriptions: Dict[str, EventSubscription] = {}
        self._logger = logging.getLogger(__name__)
        self._processing = False
        self._processing_events: Set[str] = set()  # Track events being processed

    async def subscribe(
        self, 
        event_type: Type, 
        handler: EventHandler
    ) -> str:
        """Subscribe a handler to an event type.

        Returns:
            Subscription ID for later unsubscription
        """
        subscription_id = f"{event_type.__name__}_{id(handler)}_{uuid4().hex[:8]}"

        subscription = EventSubscription(event_type, handler, subscription_id)
        self._subscriptions[event_type].append(subscription)
        self._handler_subscriptions[subscription_id] = subscription

        self._logger.debug(f"Subscribed handler to {event_type.__name__}")
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe a handler using subscription ID.

        Returns:
            True if subscription was found and removed
        """
        if subscription_id not in self._handler_subscriptions:
            return False

        subscription = self._handler_subscriptions[subscription_id]
        subscription.deactivate()

        # Remove from type subscriptions
        event_type = subscription.event_type
        if event_type in self._subscriptions:
            self._subscriptions[event_type] = [
                s for s in self._subscriptions[event_type] 
                if s.subscription_id != subscription_id
            ]

        # Remove from handler subscriptions
        del self._handler_subscriptions[subscription_id]

        self._logger.debug(f"Unsubscribed handler from {event_type.__name__}")
        return True

    async def publish(self, event: Any) -> None:
        """Publish an event to all subscribed handlers."""
        event_type = type(event)

        # Generate event key for recursion detection
        event_key = f"{event_type.__name__}_{getattr(event, 'event_id', id(event))}"

        # Check for recursion
        if event_key in self._processing_events:
            self._logger.warning(f"Recursive event publishing prevented for {event_key}")
            return

        subscriptions = self._subscriptions.get(event_type, [])

        if not subscriptions:
            self._logger.debug(f"No subscribers for {event_type.__name__}")
            return

        # Mark event as being processed
        self._processing_events.add(event_key)

        try:
            # Process handlers concurrently but isolate errors
            tasks = []
            for subscription in subscriptions:
                if subscription.is_active:
                    task = self._handle_event_safely(subscription.handler, event)
                    tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # Always remove from processing set
            self._processing_events.discard(event_key)

    async def _handle_event_safely(self, handler: EventHandler, event: Any) -> None:
        """Handle event with error isolation."""
        try:
            if await handler.can_handle(event):
                await handler.handle(event)
        except Exception as e:
            self._logger.error(f"Handler {handler.__class__.__name__} failed: {e}")
            # Don't re-raise - isolate handler errors

    def get_subscription_count(self, event_type: Type) -> int:
        """Get number of active subscriptions for event type."""
        return len([s for s in self._subscriptions.get(event_type, []) if s.is_active])

    def get_total_subscriptions(self) -> int:
        """Get total number of active subscriptions."""
        return len([s for s in self._handler_subscriptions.values() if s.is_active])


class TestEventBus:
    """Test in-memory event bus implementation."""

    @pytest.fixture
    def event_bus(self):
        """Create event bus instance for testing."""
        return EventBus()

    @pytest.fixture
    def mock_handler(self):
        """Create mock event handler."""
        handler = AsyncMock(spec=EventHandler)
        handler.can_handle.return_value = True
        return handler

    def test_event_bus_initialization(self, event_bus):
        """Should initialize event bus correctly."""
        assert event_bus is not None
        assert event_bus.get_total_subscriptions() == 0
        assert not event_bus._processing

    @pytest.mark.asyncio
    async def test_subscribe_to_event_type(self, event_bus, mock_handler):
        """Should allow subscribing to specific event types."""
        # Subscribe handler to MockTradeExecutedEvent
        subscription_id = await event_bus.subscribe(MockTradeExecutedEvent, mock_handler)

        # Verify subscription
        assert subscription_id is not None
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 1
        assert event_bus.get_total_subscriptions() == 1

        # Verify subscription ID format
        assert "MockTradeExecutedEvent" in subscription_id

    @pytest.mark.asyncio
    async def test_publish_event_to_subscribers(self, event_bus, mock_handler):
        """Should publish events to all subscribers."""
        # Subscribe handler
        await event_bus.subscribe(MockTradeExecutedEvent, mock_handler)

        # Create and publish event
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # Verify handler was called
        mock_handler.can_handle.assert_called_once_with(event)
        mock_handler.handle.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_event(self, event_bus):
        """Should support multiple handlers for the same event type."""
        handler1 = AsyncMock(spec=EventHandler)
        handler1.can_handle.return_value = True

        handler2 = AsyncMock(spec=EventHandler)
        handler2.can_handle.return_value = True

        # Subscribe both handlers
        sub_id1 = await event_bus.subscribe(MockTradeExecutedEvent, handler1)
        sub_id2 = await event_bus.subscribe(MockTradeExecutedEvent, handler2)

        assert sub_id1 != sub_id2
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 2

        # Publish event
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # Both handlers should be called
        handler1.can_handle.assert_called_once_with(event)
        handler1.handle.assert_called_once_with(event)
        handler2.can_handle.assert_called_once_with(event)
        handler2.handle.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_handler_error_isolation(self, event_bus):
        """Should isolate handler errors and continue processing."""
        good_handler = AsyncMock(spec=EventHandler)
        good_handler.can_handle.return_value = True

        failing_handler = AsyncMock(spec=EventHandler)
        failing_handler.can_handle.return_value = True
        failing_handler.handle.side_effect = Exception("Handler failed")

        # Subscribe both handlers
        await event_bus.subscribe(MockTradeExecutedEvent, good_handler)
        await event_bus.subscribe(MockTradeExecutedEvent, failing_handler)

        # Publish event
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        # Should not raise exception despite failing handler
        await event_bus.publish(event)

        # Good handler should still be called
        good_handler.handle.assert_called_once_with(event)
        failing_handler.handle.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_unsubscribe_handler(self, event_bus, mock_handler):
        """Should allow unsubscribing handlers."""
        # Subscribe handler
        subscription_id = await event_bus.subscribe(MockTradeExecutedEvent, mock_handler)
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 1

        # Unsubscribe
        result = await event_bus.unsubscribe(subscription_id)
        assert result is True
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 0
        assert event_bus.get_total_subscriptions() == 0

        # Unsubscribing non-existent subscription should return False
        result = await event_bus.unsubscribe("non-existent")
        assert result is False

    @pytest.mark.asyncio
    async def test_publish_with_no_subscribers(self, event_bus):
        """Should handle publishing events with no subscribers gracefully."""
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        # Should not raise exception
        await event_bus.publish(event)

        # Verify no subscriptions
        assert event_bus.get_subscription_count(MockTradeExecutedEvent) == 0

    @pytest.mark.asyncio
    async def test_concurrent_event_publishing(self, event_bus):
        """Should handle concurrent event publishing safely."""
        handler = AsyncMock(spec=EventHandler)
        handler.can_handle.return_value = True

        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        # Create multiple events
        events = [
            MockTradeExecutedEvent(
                event_id=f"test-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}",
                symbol="AAPL"
            )
            for i in range(10)
        ]

        # Publish all events concurrently
        tasks = [event_bus.publish(event) for event in events]
        await asyncio.gather(*tasks)

        # Handler should be called for each event
        assert handler.handle.call_count == 10

    @pytest.mark.asyncio
    async def test_handler_can_handle_filtering(self, event_bus):
        """Should respect handler can_handle filtering."""
        selective_handler = AsyncMock(spec=EventHandler)
        selective_handler.can_handle.return_value = False  # Reject all events

        await event_bus.subscribe(MockTradeExecutedEvent, selective_handler)

        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # can_handle should be called but handle should not
        selective_handler.can_handle.assert_called_once_with(event)
        selective_handler.handle.assert_not_called()

    @pytest.mark.asyncio
    async def test_recursive_event_publishing_prevention(self, event_bus):
        """Should prevent recursive event publishing."""
        recursive_handler = AsyncMock(spec=EventHandler)
        recursive_handler.can_handle.return_value = True

        # Handler that tries to publish another event
        async def recursive_handle(event):
            await event_bus.publish(event)  # Try to publish same event again

        recursive_handler.handle.side_effect = recursive_handle

        await event_bus.subscribe(MockTradeExecutedEvent, recursive_handler)

        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        # Should not cause infinite recursion
        await event_bus.publish(event)

        # Handler should be called only once
        recursive_handler.handle.assert_called_once()

    @pytest.mark.asyncio
    async def test_recursion_prevention_with_different_events(self, event_bus):
        """Should allow different events to be published recursively."""
        call_sequence = []

        class RecursiveHandler(EventHandler):
            def __init__(self, event_type_to_publish):
                self.event_type_to_publish = event_type_to_publish

            async def can_handle(self, event):
                return True

            async def handle(self, event):
                call_sequence.append(f"handling_{type(event).__name__}_{event.event_id}")

                # Publish a different event type to test cross-event recursion
                if self.event_type_to_publish and len(call_sequence) == 1:
                    new_event = self.event_type_to_publish(
                        event_id="recursive-456",
                        timestamp=datetime.now(timezone.utc),
                        trade_id="recursive-trade",
                        symbol="MSFT"
                    )
                    await event_bus.publish(new_event)

        # Subscribe handlers for both event types
        handler1 = RecursiveHandler(MockTradeExecutedEvent)
        handler2 = RecursiveHandler(None)  # No recursion

        await event_bus.subscribe(MockTradeExecutedEvent, handler1)
        await event_bus.subscribe(MockTradeExecutedEvent, handler2)

        # Publish initial event
        event = MockTradeExecutedEvent(
            event_id="initial-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="initial-trade",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # Should have processed both events
        assert len(call_sequence) == 4  # initial event handled by 2 handlers, recursive event handled by 2 handlers
        assert "handling_MockTradeExecutedEvent_initial-123" in call_sequence
        assert "handling_MockTradeExecutedEvent_recursive-456" in call_sequence

        # Count occurrences of each event
        initial_count = sum(1 for call in call_sequence if "initial-123" in call)
        recursive_count = sum(1 for call in call_sequence if "recursive-456" in call)

        assert initial_count == 2  # Both handlers processed initial event
        assert recursive_count == 2  # Both handlers processed recursive event

    @pytest.mark.asyncio
    async def test_recursion_prevention_memory_cleanup(self, event_bus):
        """Should clean up processing events set after handling."""
        recursive_handler = AsyncMock(spec=EventHandler)
        recursive_handler.can_handle.return_value = True

        # Handler that doesn't recurse
        async def normal_handle(event):
            pass

        recursive_handler.handle.side_effect = normal_handle
        await event_bus.subscribe(MockTradeExecutedEvent, recursive_handler)

        event = MockTradeExecutedEvent(
            event_id="cleanup-test",
            timestamp=datetime.now(timezone.utc),
            trade_id="cleanup-trade",
            symbol="AAPL"
        )

        # Check processing events is initially empty
        assert len(event_bus._processing_events) == 0

        await event_bus.publish(event)

        # Should be cleaned up after processing
        assert len(event_bus._processing_events) == 0

    @pytest.mark.asyncio
    async def test_recursion_prevention_with_exception_handling(self, event_bus):
        """Should clean up processing events even when handler throws exception."""
        class FailingHandler(EventHandler):
            def __init__(self):
                self.call_count = 0

            async def can_handle(self, event):
                return True

            async def handle(self, event):
                self.call_count += 1
                if self.call_count == 1:
                    # First call should try recursion then fail
                    await event_bus.publish(event)  # This should be prevented
                    raise ValueError("Handler failed intentionally")

        handler = FailingHandler()
        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        event = MockTradeExecutedEvent(
            event_id="exception-test",
            timestamp=datetime.now(timezone.utc),
            trade_id="exception-trade",
            symbol="AAPL"
        )

        # Should not raise exception due to error isolation
        await event_bus.publish(event)

        # Handler should be called only once (recursion prevented)
        assert handler.call_count == 1

        # Processing events should be cleaned up
        assert len(event_bus._processing_events) == 0

    @pytest.mark.asyncio
    async def test_recursion_prevention_with_multiple_concurrent_events(self, event_bus):
        """Should handle concurrent events without false recursion detection."""
        processed_events = []

        class ConcurrentHandler(EventHandler):
            async def can_handle(self, event):
                return True

            async def handle(self, event):
                # Simulate some async work
                await asyncio.sleep(0.01)
                processed_events.append(event.event_id)

        handler = ConcurrentHandler()
        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        # Create multiple different events
        events = [
            MockTradeExecutedEvent(
                event_id=f"concurrent-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"concurrent-trade-{i}",
                symbol="AAPL"
            )
            for i in range(5)
        ]

        # Publish all events concurrently
        tasks = [event_bus.publish(event) for event in events]
        await asyncio.gather(*tasks)

        # All events should be processed (no false recursion detection)
        assert len(processed_events) == 5
        assert all(f"concurrent-{i}" in processed_events for i in range(5))

        # Processing events should be cleaned up
        assert len(event_bus._processing_events) == 0

    @pytest.mark.asyncio 
    async def test_deep_recursion_prevention(self, event_bus):
        """Should prevent deep recursion chains."""
        recursion_depth = []

        class DeepRecursiveHandler(EventHandler):
            async def can_handle(self, event):
                return True

            async def handle(self, event):
                current_depth = len(recursion_depth)
                recursion_depth.append(current_depth)

                # Try to publish same event again (should be prevented after first level)
                if current_depth < 10:  # Attempt up to 10 levels deep
                    await event_bus.publish(event)

        handler = DeepRecursiveHandler()
        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        event = MockTradeExecutedEvent(
            event_id="deep-recursion",
            timestamp=datetime.now(timezone.utc),
            trade_id="deep-trade",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # Should only process once (no recursion allowed)
        assert len(recursion_depth) == 1
        assert recursion_depth[0] == 0

    @pytest.mark.asyncio
    async def test_recursion_prevention_with_same_event_id_different_instances(self, event_bus):
        """Should treat events with same event_id as the same for recursion prevention."""
        call_count = 0

        class SameIdHandler(EventHandler):
            async def can_handle(self, event):
                return True

            async def handle(self, event):
                nonlocal call_count
                call_count += 1

                # Create new event instance with same event_id
                duplicate_event = MockTradeExecutedEvent(
                    event_id="same-id",  # Same ID
                    timestamp=datetime.now(timezone.utc),
                    trade_id="different-trade",  # Different other fields
                    symbol="DIFFERENT"
                )

                # This should be prevented (same event_id)
                await event_bus.publish(duplicate_event)

        handler = SameIdHandler()
        await event_bus.subscribe(MockTradeExecutedEvent, handler)

        event = MockTradeExecutedEvent(
            event_id="same-id",
            timestamp=datetime.now(timezone.utc),
            trade_id="original-trade",
            symbol="AAPL"
        )

        await event_bus.publish(event)

        # Should only be called once (recursion with same event_id prevented)
        assert call_count == 1


class TestEventHandler:
    """Test event handler base functionality."""

    def test_event_handler_interface(self):
        """Should define proper event handler interface."""
        # EventHandler should be abstract
        with pytest.raises(TypeError):
            EventHandler()

        # Should require can_handle and handle methods
        class ConcreteHandler(EventHandler):
            async def can_handle(self, event):
                return True

            async def handle(self, event):
                pass

        handler = ConcreteHandler()
        assert isinstance(handler, EventHandler)

    @pytest.mark.asyncio
    async def test_event_handler_execution(self):
        """Should execute event handlers correctly."""
        handled_events = []

        class TestHandler(EventHandler):
            async def can_handle(self, event):
                return isinstance(event, MockTradeExecutedEvent)

            async def handle(self, event):
                handled_events.append(event)

        handler = TestHandler()
        event = MockTradeExecutedEvent(
            event_id="test-123",
            timestamp=datetime.now(timezone.utc),
            trade_id="trade-456",
            symbol="AAPL"
        )

        # Test can_handle
        can_handle = await handler.can_handle(event)
        assert can_handle is True

        # Test handle
        await handler.handle(event)
        assert len(handled_events) == 1
        assert handled_events[0] == event


class TestEventSubscription:
    """Test event subscription management."""

    def test_subscription_creation(self):
        """Should create event subscriptions correctly."""
        handler = AsyncMock(spec=EventHandler)
        subscription = EventSubscription(MockTradeExecutedEvent, handler, "test-sub-123")

        assert subscription.event_type == MockTradeExecutedEvent
        assert subscription.handler == handler
        assert subscription.subscription_id == "test-sub-123"
        assert subscription.is_active is True

    def test_subscription_deactivation(self):
        """Should deactivate subscriptions properly."""
        handler = AsyncMock(spec=EventHandler)
        subscription = EventSubscription(MockTradeExecutedEvent, handler, "test-sub-123")

        assert subscription.is_active is True

        subscription.deactivate()
        assert subscription.is_active is False


@pytest.mark.unit
class TestEventBusIntegration:
    """Integration tests for event bus components."""

    @pytest.mark.asyncio
    async def test_full_event_flow(self):
        """Should handle complete event publishing flow."""
        event_bus = EventBus()
        processed_events = []

        class TradeHandler(EventHandler):
            async def can_handle(self, event):
                return isinstance(event, MockTradeExecutedEvent)

            async def handle(self, event):
                processed_events.append(f"Processed {event.symbol} trade")

        # Subscribe handler
        handler = TradeHandler()
        subscription_id = await event_bus.subscribe(MockTradeExecutedEvent, handler)

        # Publish multiple events
        events = [
            MockTradeExecutedEvent(
                event_id=f"test-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}",
                symbol=symbol
            )
            for i, symbol in enumerate(["AAPL", "MSFT", "GOOGL"])
        ]

        for event in events:
            await event_bus.publish(event)

        # Verify all events processed
        assert len(processed_events) == 3
        assert "Processed AAPL trade" in processed_events
        assert "Processed MSFT trade" in processed_events
        assert "Processed GOOGL trade" in processed_events

        # Clean up
        await event_bus.unsubscribe(subscription_id)
        assert event_bus.get_total_subscriptions() == 0

    @pytest.mark.asyncio
    async def test_event_ordering_with_sequential_processing(self):
        """Should maintain event processing order when needed."""
        event_bus = EventBus()
        processing_order = []

        class OrderedHandler(EventHandler):
            def __init__(self, handler_id):
                self.handler_id = handler_id

            async def can_handle(self, event):
                return True

            async def handle(self, event):
                # Add small delay to test ordering
                await asyncio.sleep(0.01)
                processing_order.append(f"{self.handler_id}-{event.event_id}")

        # Subscribe multiple handlers
        handler1 = OrderedHandler("H1")
        handler2 = OrderedHandler("H2")

        await event_bus.subscribe(MockTradeExecutedEvent, handler1)
        await event_bus.subscribe(MockTradeExecutedEvent, handler2)

        # Publish events sequentially
        events = [
            MockTradeExecutedEvent(
                event_id=f"event-{i}",
                timestamp=datetime.now(timezone.utc),
                trade_id=f"trade-{i}",
                symbol="AAPL"
            )
            for i in range(3)
        ]

        # Process events one by one to maintain order
        for event in events:
            await event_bus.publish(event)

        # Each event should be processed by both handlers
        assert len(processing_order) == 6

        # Events should be processed in order (though handlers may be concurrent)
        event_0_handlers = [item for item in processing_order if "event-0" in item]
        event_1_handlers = [item for item in processing_order if "event-1" in item]
        event_2_handlers = [item for item in processing_order if "event-2" in item]

        assert len(event_0_handlers) == 2
        assert len(event_1_handlers) == 2
        assert len(event_2_handlers) == 2
