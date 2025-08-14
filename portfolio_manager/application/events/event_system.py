"""
Event system integration and setup.

This module provides the main EventSystem class that coordinates
the event bus with handlers and manages the overall event-driven
architecture setup.
"""

import logging

from ...domain.events import TradeExecutedEvent
from ...infrastructure.events.event_bus import EventBus
from .trade_handlers import PortfolioMetricsEventHandler, TradeExecutedEventHandler


class EventSystem:
    """
    Main event system coordinator.

    This class manages the event bus and handler registration,
    providing a central point for event system configuration.
    """

    def __init__(self, event_bus: EventBus = None):
        """
        Initialize the event system.

        Args:
            event_bus: Optional event bus instance. If None, creates a new one.
        """
        self.event_bus = event_bus or EventBus()
        self._subscription_ids: list[str] = []
        self._logger = logging.getLogger(__name__)

    async def setup_trade_processing(
        self,
        portfolio_repository,
        position_repository,
        audit_service,
        portfolio_metrics_service,
        risk_service
    ) -> None:
        """
        Set up trade processing event handlers.

        This configures the handlers needed for trade execution processing
        including portfolio updates and metrics recalculation.

        Args:
            portfolio_repository: Repository for portfolio data access
            position_repository: Repository for position data access
            audit_service: Service for audit logging
            portfolio_metrics_service: Service for portfolio metrics
            risk_service: Service for risk calculations
        """
        # Create trade execution handler
        trade_handler = TradeExecutedEventHandler(
            portfolio_repository,
            position_repository,
            audit_service
        )

        # Create portfolio metrics handler
        metrics_handler = PortfolioMetricsEventHandler(
            portfolio_metrics_service,
            risk_service
        )

        # Subscribe handlers to trade events
        trade_sub_id = await self.event_bus.subscribe(TradeExecutedEvent, trade_handler)
        metrics_sub_id = await self.event_bus.subscribe(TradeExecutedEvent, metrics_handler)

        # Track subscriptions for cleanup
        self._subscription_ids.extend([trade_sub_id, metrics_sub_id])

        self._logger.info("Trade processing event handlers configured")

    async def publish_event(self, event) -> None:
        """
        Publish an event through the event bus.

        Args:
            event: The domain event to publish
        """
        await self.event_bus.publish(event)

    async def shutdown(self) -> None:
        """
        Shutdown the event system.

        This unsubscribes all handlers and cleans up resources.
        """
        for subscription_id in self._subscription_ids:
            await self.event_bus.unsubscribe(subscription_id)

        self._subscription_ids.clear()
        self._logger.info("Event system shutdown complete")

    def get_subscription_stats(self) -> dict[str, int]:
        """
        Get statistics about current subscriptions.

        Returns:
            Dictionary with subscription statistics
        """
        return {
            "total_subscriptions": self.event_bus.get_total_subscriptions(),
            "trade_event_subscriptions": self.event_bus.get_subscription_count(TradeExecutedEvent)
        }
