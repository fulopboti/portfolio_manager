"""
Event handlers for trade processing and portfolio management.

This module contains handlers that process trade-related events and
coordinate the necessary business logic across the system.
"""

import logging
from typing import Any

from ...domain.entities import Portfolio, Position, TradeSide
from ...domain.events import TradeExecutedEvent
from ...domain.exceptions import InsufficientFundsError, InvalidPositionError
from ...infrastructure.events.handlers import EventHandler


class TradeExecutedEventHandler(EventHandler):
    """Handler for trade execution events."""

    def __init__(self, portfolio_repository, position_repository, audit_service):
        """
        Initialize the trade event handler.

        Args:
            portfolio_repository: Repository for portfolio data access
            position_repository: Repository for position data access
            audit_service: Service for audit logging
        """
        self.portfolio_repository = portfolio_repository
        self.position_repository = position_repository
        self.audit_service = audit_service
        self._logger = logging.getLogger(__name__)

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can process the event."""
        return isinstance(event, TradeExecutedEvent)

    async def handle(self, event: TradeExecutedEvent) -> None:
        """
        Handle a trade execution event.

        This processes the trade by:
        1. Retrieving the portfolio
        2. Processing the trade (buy/sell)
        3. Updating portfolio cash
        4. Saving changes
        5. Logging the trade

        Args:
            event: The trade executed event to process

        Raises:
            ValueError: If portfolio not found
            InsufficientFundsError: If insufficient funds for buy trades
            InvalidPositionError: If invalid position for sell trades
        """
        try:
            # Get portfolio
            portfolio = await self.portfolio_repository.get_portfolio(
                event.portfolio_id
            )
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

    async def _handle_buy_trade(
        self, event: TradeExecutedEvent, portfolio: Portfolio
    ) -> None:
        """
        Handle a buy trade by creating or updating positions.

        Args:
            event: The trade event
            portfolio: The portfolio to update

        Raises:
            InsufficientFundsError: If insufficient cash for the trade
        """
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
                last_updated=event.timestamp,
            )
            await self.position_repository.save_position(new_position)

    async def _handle_sell_trade(
        self, event: TradeExecutedEvent, portfolio: Portfolio
    ) -> None:
        """
        Handle a sell trade by reducing or closing positions.

        Args:
            event: The trade event
            portfolio: The portfolio to update

        Raises:
            InvalidPositionError: If position doesn't exist or insufficient quantity
        """
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

        # Reduce or close position
        if existing_position.qty == event.quantity:
            # Close position completely
            await self.position_repository.delete_position(
                event.portfolio_id, event.symbol
            )
        else:
            # Reduce position
            existing_position.reduce_shares(event.quantity)
            existing_position.last_updated = event.timestamp
            await self.position_repository.save_position(existing_position)

    async def _update_portfolio_cash(
        self, event: TradeExecutedEvent, portfolio: Portfolio
    ) -> None:
        """
        Update portfolio cash based on the trade.

        Args:
            event: The trade event
            portfolio: The portfolio to update
        """
        trade_amount = event.gross_amount()

        if event.side == TradeSide.BUY:
            portfolio.deduct_cash(trade_amount)
        elif event.side == TradeSide.SELL:
            portfolio.add_cash(trade_amount)


class PortfolioMetricsEventHandler(EventHandler):
    """Handler for portfolio metrics recalculation after trades."""

    def __init__(self, portfolio_metrics_service, risk_service):
        """
        Initialize the portfolio metrics handler.

        Args:
            portfolio_metrics_service: Service for calculating portfolio metrics
            risk_service: Service for calculating risk metrics
        """
        self.portfolio_metrics_service = portfolio_metrics_service
        self.risk_service = risk_service
        self._logger = logging.getLogger(__name__)

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes position change events."""
        return isinstance(event, TradeExecutedEvent)

    async def handle(self, event: TradeExecutedEvent) -> None:
        """
        Recalculate portfolio metrics after position changes.

        This handler runs after trades to ensure portfolio metrics
        and risk calculations are kept up to date.

        Args:
            event: The trade executed event
        """
        try:
            # Recalculate portfolio metrics
            await self.portfolio_metrics_service.recalculate_metrics(event.portfolio_id)

            # Update risk metrics
            await self.risk_service.update_portfolio_risk(event.portfolio_id)

            self._logger.info(f"Updated metrics for portfolio {event.portfolio_id}")

        except Exception as e:
            self._logger.error(
                f"Failed to update metrics for portfolio {event.portfolio_id}: {e}"
            )
            # Don't re-raise - metrics failures should not break trade processing
