"""
Event handlers for portfolio rebalancing and position management events.

This module contains handlers that process rebalancing events and
coordinate portfolio optimization activities.
"""

import logging
from typing import Any

from ...domain.events import PortfolioRebalancedEvent, PositionChange
from ...domain.exceptions import DomainError
from .base_handler import BaseEventHandler, ErrorHandlingStrategy


class PortfolioRebalancedEventHandler(BaseEventHandler):
    """Handler for portfolio rebalancing events."""
    
    def __init__(self, portfolio_repository, position_repository, audit_service):
        """
        Initialize the portfolio rebalancing event handler.
        
        Args:
            portfolio_repository: Repository for portfolio data access
            position_repository: Repository for position data access
            audit_service: Service for audit logging
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.CRITICAL)
        self.portfolio_repository = portfolio_repository
        self.position_repository = position_repository
        self.audit_service = audit_service
    
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can process the event."""
        return isinstance(event, PortfolioRebalancedEvent)
    
    async def _handle_event(self, event: PortfolioRebalancedEvent) -> None:
        """
        Handle a portfolio rebalancing event.
        
        This processes the rebalancing by:
        1. Validating portfolio exists
        2. Updating all changed positions
        3. Recalculating portfolio metrics
        4. Logging the rebalancing activity
        
        Args:
            event: The portfolio rebalanced event to process
        """
        try:
            self._logger.info(
                f"Processing portfolio rebalancing for {event.portfolio_id} "
                f"with {len(event.changes)} position changes"
            )
            
            # Validate portfolio exists
            portfolio = await self.portfolio_repository.get_portfolio(event.portfolio_id)
            if not portfolio:
                raise ValueError(f"Portfolio {event.portfolio_id} not found")
            
            # Process all position changes
            await self._process_position_changes(event)
            
            # Update portfolio metadata
            portfolio.last_rebalanced = event.timestamp
            portfolio.last_updated = event.timestamp
            await self.portfolio_repository.save_portfolio(portfolio)
            
            # Log audit event
            await self.audit_service.log_portfolio_rebalancing(event)
            
            self._logger.info(
                f"Successfully processed rebalancing for portfolio {event.portfolio_id}"
            )
            
        except Exception as e:
            self._logger.error(
                f"Failed to process rebalancing for portfolio {event.portfolio_id}: {e}"
            )
            raise
    
    async def _process_position_changes(self, event: PortfolioRebalancedEvent) -> None:
        """
        Process all position changes from the rebalancing event.
        
        Args:
            event: The rebalancing event containing position changes
        """
        for change in event.changes:
            await self._process_single_position_change(event.portfolio_id, change, event.timestamp)
    
    async def _process_single_position_change(
        self, 
        portfolio_id: str, 
        change: PositionChange, 
        timestamp
    ) -> None:
        """
        Process a single position change.
        
        Args:
            portfolio_id: The portfolio ID
            change: The position change to process
            timestamp: The event timestamp
        """
        try:
            # Get existing position
            existing_position = await self.position_repository.get_position(
                portfolio_id, change.symbol
            )
            
            if change.new_quantity == 0:
                # Close position completely
                if existing_position:
                    await self.position_repository.delete_position(portfolio_id, change.symbol)
                    self._logger.debug(f"Closed position {portfolio_id}:{change.symbol}")
                
            elif existing_position:
                # Update existing position
                existing_position.qty = change.new_quantity
                existing_position.last_updated = timestamp
                await self.position_repository.save_position(existing_position)
                
                self._logger.debug(
                    f"Updated position {portfolio_id}:{change.symbol} "
                    f"quantity: {change.old_quantity} -> {change.new_quantity}"
                )
                
            else:
                # This shouldn't happen in normal rebalancing, but handle it
                self._logger.warning(
                    f"Creating new position during rebalancing for {change.symbol} "
                    f"in portfolio {portfolio_id}"
                )
            
        except Exception as e:
            self._logger.error(
                f"Failed to process position change for {change.symbol}: {e}"
            )
            raise


class RebalancingMetricsEventHandler(BaseEventHandler):
    """Handler for updating metrics after portfolio rebalancing."""
    
    def __init__(self, portfolio_metrics_service, risk_service, analytics_service):
        """
        Initialize the rebalancing metrics handler.
        
        Args:
            portfolio_metrics_service: Service for calculating portfolio metrics
            risk_service: Service for risk calculations
            analytics_service: Service for performance analytics
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.portfolio_metrics_service = portfolio_metrics_service
        self.risk_service = risk_service
        self.analytics_service = analytics_service
    
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes rebalancing events."""
        return isinstance(event, PortfolioRebalancedEvent)
    
    async def _handle_event(self, event: PortfolioRebalancedEvent) -> None:
        """
        Update metrics and analytics after portfolio rebalancing.
        
        This handler runs after rebalancing to:
        1. Recalculate portfolio metrics
        2. Update risk calculations
        3. Analyze rebalancing effectiveness
        4. Update performance tracking
        
        Args:
            event: The portfolio rebalanced event
        """
        try:
            portfolio_id = event.portfolio_id
            
            # Recalculate portfolio metrics
            await self.portfolio_metrics_service.recalculate_metrics(portfolio_id)
            
            # Update risk metrics
            await self.risk_service.update_portfolio_risk(portfolio_id)
            
            # Analyze rebalancing impact
            await self._analyze_rebalancing_impact(event)
            
            # Update performance tracking
            await self.analytics_service.track_rebalancing_event(event)
            
            self._logger.info(f"Updated metrics after rebalancing for portfolio {portfolio_id}")
            
        except Exception as e:
            self._logger.error(
                f"Failed to update metrics after rebalancing for portfolio {event.portfolio_id}: {e}"
            )
            # Don't re-raise - metrics failures should not break rebalancing processing
    
    async def _analyze_rebalancing_impact(self, event: PortfolioRebalancedEvent) -> None:
        """
        Analyze the impact of the rebalancing operation.
        
        Args:
            event: The rebalancing event
        """
        try:
            # Calculate concentration changes
            concentration_changes = {}
            
            for change in event.changes:
                if change.quantity_change() != 0:
                    concentration_changes[change.symbol] = {
                        'old_quantity': change.old_quantity,
                        'new_quantity': change.new_quantity,
                        'change': change.quantity_change(),
                        'reason': change.reason
                    }
            
            # Log analysis results
            self._logger.info(
                f"Rebalancing analysis for portfolio {event.portfolio_id}: "
                f"{len(concentration_changes)} positions modified"
            )
            
            for symbol, analysis in concentration_changes.items():
                self._logger.debug(
                    f"Position change {symbol}: {analysis['old_quantity']} -> "
                    f"{analysis['new_quantity']} ({analysis['reason']})"
                )
            
        except Exception as e:
            self._logger.warning(f"Failed to analyze rebalancing impact: {e}")


class RebalancingNotificationEventHandler(BaseEventHandler):
    """Handler for sending notifications after portfolio rebalancing."""
    
    def __init__(self, notification_service, user_service):
        """
        Initialize the rebalancing notification handler.
        
        Args:
            notification_service: Service for sending notifications
            user_service: Service for user management
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.notification_service = notification_service
        self.user_service = user_service
    
    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes rebalancing events."""
        return isinstance(event, PortfolioRebalancedEvent)
    
    async def _handle_event(self, event: PortfolioRebalancedEvent) -> None:
        """
        Send notifications after portfolio rebalancing.
        
        Args:
            event: The portfolio rebalanced event
        """
        try:
            # Get portfolio owner for notifications
            portfolio_owner = await self.user_service.get_portfolio_owner(event.portfolio_id)
            
            if portfolio_owner and portfolio_owner.notifications_enabled:
                # Send rebalancing notification
                await self.notification_service.send_rebalancing_notification(
                    user_id=portfolio_owner.user_id,
                    portfolio_id=event.portfolio_id,
                    changes=event.changes,
                    timestamp=event.timestamp
                )
                
                self._logger.info(
                    f"Sent rebalancing notification for portfolio {event.portfolio_id}"
                )
            
        except Exception as e:
            self._logger.warning(
                f"Failed to send rebalancing notification for portfolio {event.portfolio_id}: {e}"
            )
            # Don't re-raise - notification failures should not break rebalancing processing
