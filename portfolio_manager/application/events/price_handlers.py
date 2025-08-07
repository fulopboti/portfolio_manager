"""
Event handlers for asset price updates and market data events.

This module contains handlers that process price update events and
coordinate portfolio revaluation and risk calculations.
"""

import logging
from decimal import Decimal
from typing import Any

from ...domain.events import AssetPriceUpdatedEvent
from ...domain.exceptions import DomainError
from .base_handler import BaseEventHandler, ErrorHandlingStrategy


class AssetPriceUpdatedEventHandler(BaseEventHandler):
    """Handler for asset price update events."""
    
    def __init__(self, portfolio_repository, position_repository, market_data_service):
        """
        Initialize the price update event handler.
        
        Args:
            portfolio_repository: Repository for portfolio data access
            position_repository: Repository for position data access
            market_data_service: Service for market data operations
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.CRITICAL)
        self.portfolio_repository = portfolio_repository
        self.position_repository = position_repository
        self.market_data_service = market_data_service
    
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can process the event."""
        return isinstance(event, AssetPriceUpdatedEvent)
    
    async def _handle_event(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Handle an asset price update event.
        
        This processes the price update by:
        1. Updating all positions for the symbol
        2. Recalculating portfolio values
        3. Triggering risk recalculation
        4. Logging the price change
        
        Args:
            event: The asset price updated event to process
        """
        # Update all positions holding this symbol
        await self._update_positions_for_symbol(event)
        
        # Update portfolios containing this symbol
        await self._update_portfolios_for_symbol(event)
        
        # Store the price update in market data
        await self.market_data_service.store_price_update(
            symbol=event.symbol,
            price=event.new_price,
            timestamp=event.timestamp
        )
    
    async def _update_positions_for_symbol(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Update all positions for the given symbol with new market value.
        
        Args:
            event: The price update event
        """
        positions = await self.position_repository.find_positions_by_symbol(event.symbol)
        
        for position in positions:
            # Update position market value
            old_market_value = position.market_value
            position.current_price = event.new_price
            position.last_updated = event.timestamp
            
            # Calculate unrealized P&L change
            market_value_change = position.market_value - old_market_value
            
            await self.position_repository.save_position(position)
            
            self._logger.debug(
                f"Updated position {position.portfolio_id}:{position.symbol} "
                f"market value change: ${market_value_change:.2f}"
            )
    
    async def _update_portfolios_for_symbol(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Update portfolio values for all portfolios holding the symbol.
        
        Args:
            event: The price update event
        """
        # Get portfolios that hold positions in this symbol
        portfolios = await self.portfolio_repository.find_portfolios_by_symbol(event.symbol)
        
        for portfolio in portfolios:
            old_total_value = portfolio.total_value
            
            # Recalculate portfolio total value
            await portfolio.recalculate_total_value(self.position_repository)
            portfolio.last_updated = event.timestamp
            
            value_change = portfolio.total_value - old_total_value
            
            await self.portfolio_repository.save_portfolio(portfolio)
            
            self._logger.debug(
                f"Updated portfolio {portfolio.portfolio_id} "
                f"total value change: ${value_change:.2f}"
            )


class PortfolioRevaluationEventHandler(BaseEventHandler):
    """Handler for portfolio revaluation after price updates."""
    
    def __init__(self, risk_service, notification_service, alert_service):
        """
        Initialize the portfolio revaluation handler.
        
        Args:
            risk_service: Service for risk calculations
            notification_service: Service for sending notifications
            alert_service: Service for threshold alerts
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.risk_service = risk_service
        self.notification_service = notification_service
        self.alert_service = alert_service
    
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes price update events."""
        return isinstance(event, AssetPriceUpdatedEvent)
    
    async def _handle_event(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Handle portfolio revaluation after price updates.
        
        This handler runs after price updates to:
        1. Recalculate risk metrics
        2. Check for threshold breaches
        3. Send notifications if needed
        
        Args:
            event: The asset price updated event
        """
        # Check if this is a significant price change
        price_change_percent = abs(event.price_change_percent())
        significant_threshold = Decimal("0.05")  # 5%
        
        if price_change_percent >= significant_threshold:
            # Update risk metrics for affected portfolios
            await self._update_risk_metrics_for_symbol(event.symbol)
            
            # Check for risk threshold breaches
            await self._check_risk_thresholds(event)
            
            # Send notifications for significant changes
            await self._send_price_change_notifications(event)
    
    async def _update_risk_metrics_for_symbol(self, symbol: str) -> None:
        """
        Update risk metrics for all portfolios holding the symbol.
        
        Args:
            symbol: The symbol that had a price update
        """
        try:
            portfolios = await self.portfolio_repository.find_portfolios_by_symbol(symbol)
            
            for portfolio in portfolios:
                await self.risk_service.update_portfolio_risk(portfolio.portfolio_id)
                
        except Exception as e:
            self._logger.warning(f"Failed to update risk metrics for {symbol}: {e}")
    
    async def _check_risk_thresholds(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Check if the price change triggers any risk threshold alerts.
        
        Args:
            event: The price update event
        """
        try:
            await self.alert_service.check_price_change_thresholds(
                symbol=event.symbol,
                old_price=event.old_price,
                new_price=event.new_price,
                timestamp=event.timestamp
            )
            
        except Exception as e:
            self._logger.warning(f"Failed to check risk thresholds for {event.symbol}: {e}")
    
    async def _send_price_change_notifications(self, event: AssetPriceUpdatedEvent) -> None:
        """
        Send notifications for significant price changes.
        
        Args:
            event: The price update event
        """
        try:
            await self.notification_service.send_price_alert(
                symbol=event.symbol,
                old_price=event.old_price,
                new_price=event.new_price,
                change_percent=event.price_change_percent(),
                timestamp=event.timestamp
            )
            
        except Exception as e:
            self._logger.warning(f"Failed to send price change notification for {event.symbol}: {e}")
