"""
Event bus implementation for async event-driven architecture.

This module provides the core event bus infrastructure that enables
decoupled communication between different parts of the system through
domain events.
"""

import asyncio
import logging
from collections import defaultdict
from typing import Any
from uuid import uuid4

from .handlers import EventHandler


class EventSubscription:
    """Represents a subscription to an event type."""

    def __init__(self, event_type: type, handler: EventHandler, subscription_id: str):
        """
        Initialize an event subscription.

        Args:
            event_type: The type of event to subscribe to
            handler: The handler that will process events
            subscription_id: Unique identifier for this subscription
        """
        self.event_type = event_type
        self.handler = handler
        self.subscription_id = subscription_id
        self.is_active = True

    def deactivate(self) -> None:
        """Deactivate this subscription."""
        self.is_active = False


class EventBus:
    """
    In-memory event bus for local-first architecture.

    Provides async publish/subscribe functionality with error isolation
    and concurrent event processing capabilities.
    """

    def __init__(self):
        """Initialize the event bus."""
        self._subscriptions: dict[type, list[EventSubscription]] = defaultdict(list)
        self._handler_subscriptions: dict[str, EventSubscription] = {}
        self._logger = logging.getLogger(__name__)
        self._processing_events: set[str] = set()  # Track events being processed to prevent recursion

    async def subscribe(self, event_type: type, handler: EventHandler) -> str:
        """
        Subscribe a handler to an event type.

        Args:
            event_type: The type of event to subscribe to
            handler: The handler that will process events of this type

        Returns:
            Subscription ID that can be used to unsubscribe later
        """
        subscription_id = f"{event_type.__name__}_{id(handler)}_{uuid4().hex[:8]}"

        subscription = EventSubscription(event_type, handler, subscription_id)
        self._subscriptions[event_type].append(subscription)
        self._handler_subscriptions[subscription_id] = subscription

        self._logger.debug(f"Subscribed handler to {event_type.__name__}")
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe a handler using its subscription ID.

        Args:
            subscription_id: The ID returned from subscribe()

        Returns:
            True if subscription was found and removed, False otherwise
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
        """
        Publish an event to all subscribed handlers.

        Events are processed concurrently with error isolation - if one
        handler fails, others will still process the event. Includes
        recursion prevention to avoid infinite loops.

        Args:
            event: The event to publish
        """
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
        """
        Handle an event with error isolation.

        Args:
            handler: The event handler to execute
            event: The event to process
        """
        try:
            if await handler.can_handle(event):
                await handler.handle(event)
        except Exception as e:
            self._logger.error(f"Handler {handler.__class__.__name__} failed: {e}")
            # Don't re-raise - isolate handler errors from other handlers

    def get_subscription_count(self, event_type: type) -> int:
        """
        Get the number of active subscriptions for an event type.

        Args:
            event_type: The event type to check

        Returns:
            Number of active subscriptions
        """
        return len([s for s in self._subscriptions.get(event_type, []) if s.is_active])

    def get_total_subscriptions(self) -> int:
        """
        Get the total number of active subscriptions.

        Returns:
            Total number of active subscriptions across all event types
        """
        return len([s for s in self._handler_subscriptions.values() if s.is_active])
