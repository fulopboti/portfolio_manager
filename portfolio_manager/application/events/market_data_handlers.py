"""
Event handlers for market data processing and distribution.

This module contains handlers that process market data events and
coordinate data distribution to various system components.
"""

from typing import Any

from ...domain.events import AssetPriceUpdatedEvent, MarketDataReceivedEvent
from .base_handler import BaseEventHandler, ErrorHandlingStrategy


class MarketDataReceivedEventHandler(BaseEventHandler):
    """Handler for processing incoming market data."""

    def __init__(
        self, market_data_service: Any, asset_service: Any, event_bus: Any
    ) -> None:
        """
        Initialize the market data received handler.

        Args:
            market_data_service: Service for market data storage and processing
            asset_service: Service for asset management
            event_bus: Event bus for publishing derived events
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.CRITICAL)
        self.market_data_service = market_data_service
        self.asset_service = asset_service
        self.event_bus = event_bus

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can process the event."""
        return isinstance(event, MarketDataReceivedEvent)

    async def _handle_event(self, event: MarketDataReceivedEvent) -> None:
        """
        Handle incoming market data.

        This processes the market data by:
        1. Validating and storing the data
        2. Checking for significant price changes
        3. Publishing price update events if needed
        4. Updating asset information

        Args:
            event: The market data received event to process
        """
        try:
            self._logger.debug(
                f"Processing market data for {event.symbol}: "
                f"price=${event.price}, volume={event.volume}"
            )

            # Store the market data
            await self.market_data_service.store_market_data(
                symbol=event.symbol,
                price=event.price,
                volume=event.volume,
                market_cap=event.market_cap,
                timestamp=event.timestamp,
            )

            # Check if this represents a price change
            price_changed = await self._check_price_change(event)

            if price_changed:
                # Publish asset price updated event
                await self._publish_price_update_event(event, price_changed)

            # Update asset metadata if provided
            if event.market_cap is not None:
                await self._update_asset_metadata(event)

            self._logger.debug(f"Successfully processed market data for {event.symbol}")

        except Exception as e:
            self._logger.error(f"Failed to process market data for {event.symbol}: {e}")
            raise

    async def _check_price_change(self, event: MarketDataReceivedEvent) -> dict | None:
        """
        Check if the received data represents a price change.

        Args:
            event: The market data event

        Returns:
            Dictionary with old/new price info if price changed, None otherwise
        """
        # Get the last known price for this symbol
        last_price = await self.market_data_service.get_last_price(event.symbol)

        if last_price is None or last_price != event.price:
            return {
                "old_price": last_price
                or event.price,  # Use current price if no previous price
                "new_price": event.price,
            }

        return None

    async def _publish_price_update_event(
        self, event: MarketDataReceivedEvent, price_change: dict
    ) -> None:
        """
        Publish an asset price updated event.

        Args:
            event: The original market data event
            price_change: Dictionary containing old and new prices
        """
        try:
            price_update_event = AssetPriceUpdatedEvent(
                event_id=f"price_update_{event.symbol}_{event.timestamp.isoformat()}",
                timestamp=event.timestamp,
                symbol=event.symbol,
                old_price=price_change["old_price"],
                new_price=price_change["new_price"],
            )

            await self.event_bus.publish(price_update_event)

            self._logger.debug(
                f"Published price update event for {event.symbol}: "
                f"{price_change['old_price']} -> {price_change['new_price']}"
            )

        except Exception as e:
            self._logger.warning(
                f"Failed to publish price update event for {event.symbol}: {e}"
            )

    async def _update_asset_metadata(self, event: MarketDataReceivedEvent) -> None:
        """
        Update asset metadata with new market information.

        Args:
            event: The market data event
        """
        try:
            await self.asset_service.update_market_data(
                symbol=event.symbol,
                market_cap=event.market_cap,
                last_price=event.price,
                last_volume=event.volume,
                timestamp=event.timestamp,
            )

        except Exception as e:
            self._logger.warning(
                f"Failed to update asset metadata for {event.symbol}: {e}"
            )


class MarketDataQualityEventHandler(BaseEventHandler):
    """Handler for monitoring market data quality and anomalies."""

    def __init__(self, data_quality_service: Any, alert_service: Any) -> None:
        """
        Initialize the market data quality handler.

        Args:
            data_quality_service: Service for data quality monitoring
            alert_service: Service for sending data quality alerts
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.data_quality_service = data_quality_service
        self.alert_service = alert_service

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes market data events."""
        return isinstance(event, MarketDataReceivedEvent)

    async def _handle_event(self, event: MarketDataReceivedEvent) -> None:
        """
        Monitor market data quality and detect anomalies.

        Args:
            event: The market data received event
        """
        try:
            # Check for data quality issues
            quality_issues = await self._check_data_quality(event)

            if quality_issues:
                await self._handle_quality_issues(event, quality_issues)

            # Update data quality metrics
            await self._update_quality_metrics(event)

        except Exception as e:
            self._logger.warning(
                f"Failed to check data quality for {event.symbol}: {e}"
            )
            # Don't re-raise - quality checks should not break data processing

    async def _check_data_quality(self, event: MarketDataReceivedEvent) -> list:
        """
        Check the market data for quality issues.

        Args:
            event: The market data event

        Returns:
            List of detected quality issues
        """
        issues = []

        # Check for price anomalies
        price_anomaly = await self.data_quality_service.check_price_anomaly(
            symbol=event.symbol, price=event.price, timestamp=event.timestamp
        )
        if price_anomaly:
            issues.append(f"Price anomaly: {price_anomaly}")

        # Check for volume anomalies
        volume_anomaly = await self.data_quality_service.check_volume_anomaly(
            symbol=event.symbol, volume=event.volume, timestamp=event.timestamp
        )
        if volume_anomaly:
            issues.append(f"Volume anomaly: {volume_anomaly}")

        # Check for stale data
        if await self.data_quality_service.is_stale_data(event.symbol, event.timestamp):
            issues.append("Stale data detected")

        return issues

    async def _handle_quality_issues(
        self, event: MarketDataReceivedEvent, issues: list
    ) -> None:
        """
        Handle detected data quality issues.

        Args:
            event: The market data event
            issues: List of quality issues
        """
        self._logger.warning(
            f"Data quality issues detected for {event.symbol}: {', '.join(issues)}"
        )

        # Send quality alert for significant issues
        critical_issues = [issue for issue in issues if "anomaly" in issue.lower()]

        if critical_issues:
            try:
                await self.alert_service.send_data_quality_alert(
                    symbol=event.symbol,
                    issues=critical_issues,
                    data_point={
                        "price": event.price,
                        "volume": event.volume,
                        "timestamp": event.timestamp,
                    },
                )

            except Exception as e:
                self._logger.warning(f"Failed to send data quality alert: {e}")

    async def _update_quality_metrics(self, event: MarketDataReceivedEvent) -> None:
        """
        Update data quality tracking metrics.

        Args:
            event: The market data event
        """
        try:
            await self.data_quality_service.update_quality_metrics(
                symbol=event.symbol, timestamp=event.timestamp, data_received=True
            )

        except Exception as e:
            self._logger.debug(
                f"Failed to update quality metrics for {event.symbol}: {e}"
            )


class MarketDataCachingEventHandler(BaseEventHandler):
    """Handler for caching and distributing market data."""

    def __init__(self, cache_service: Any, distribution_service: Any) -> None:
        """
        Initialize the market data caching handler.

        Args:
            cache_service: Service for data caching
            distribution_service: Service for data distribution
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.cache_service = cache_service
        self.distribution_service = distribution_service

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes market data events."""
        return isinstance(event, MarketDataReceivedEvent)

    async def _handle_event(self, event: MarketDataReceivedEvent) -> None:
        """
        Cache and distribute market data to subscribers.

        Args:
            event: The market data received event
        """
        try:
            # Cache the market data for fast access
            await self.cache_service.cache_market_data(
                symbol=event.symbol,
                price=event.price,
                volume=event.volume,
                market_cap=event.market_cap,
                timestamp=event.timestamp,
            )

            # Distribute to real-time subscribers
            await self.distribution_service.distribute_market_data(
                symbol=event.symbol,
                data={
                    "price": str(event.price),
                    "volume": event.volume,
                    "market_cap": str(event.market_cap) if event.market_cap else None,
                    "timestamp": event.timestamp.isoformat(),
                },
            )

            self._logger.debug(f"Cached and distributed market data for {event.symbol}")

        except Exception as e:
            self._logger.warning(
                f"Failed to cache/distribute market data for {event.symbol}: {e}"
            )
            # Don't re-raise - caching failures should not break data processing
