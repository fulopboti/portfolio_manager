"""Portfolio simulation service for executing trades and managing positions."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from portfolio_manager.application.ports import AssetRepository, PortfolioRepository
from portfolio_manager.domain.entities import (
    BrokerProfile,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)
from portfolio_manager.domain.exceptions import (
    DomainError,
    InsufficientFundsError,
    InvalidPositionError,
    InvalidTradeError,
)
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError

from .base_service import ResultBasedService


@dataclass
class TradeResult:
    """Result of a trade execution."""

    success: bool
    trade: Trade | None = None
    error: Exception | None = None


@dataclass
class PortfolioMetrics:
    """Portfolio performance metrics."""

    total_market_value: Decimal
    total_cost_basis: Decimal
    total_unrealized_pnl: Decimal
    cash_balance: Decimal
    total_portfolio_value: Decimal
    number_of_positions: int


class PortfolioSimulatorService(ResultBasedService):
    """Service for simulating portfolio operations and trade execution."""

    def __init__(
        self,
        portfolio_repository: PortfolioRepository,
        asset_repository: AssetRepository,
    ):
        super().__init__(logger_name=f"{__name__}.{self.__class__.__name__}")
        self.portfolio_repository = portfolio_repository
        self.asset_repository = asset_repository
        self._config = None  # Will be set by factory

    async def execute_trade(
        self,
        portfolio_id: UUID,
        symbol: str,
        side: TradeSide,
        quantity: Decimal,
        broker_profile: BrokerProfile,
        comment: str = "",
    ) -> TradeResult:
        """Execute a trade within a portfolio."""
        context = f"{portfolio_id}:{symbol}:{side.value}:{quantity}"

        async def _execute():
            # Validate required parameters
            self._validate_required_params(
                {
                    "portfolio_id": portfolio_id,
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "broker_profile": broker_profile,
                }
            )

            # Validate business rules
            self._validate_business_rules(
                [
                    (quantity > 0, "Quantity must be positive"),
                    (symbol.strip(), "Symbol cannot be empty"),
                ]
            )

            # Get portfolio
            portfolio = await self.portfolio_repository.get_portfolio(portfolio_id)
            if not portfolio:
                raise InvalidTradeError(f"Portfolio {portfolio_id} not found")

            # Get current market price
            latest_snapshot = await self.asset_repository.get_latest_snapshot(symbol)
            if not latest_snapshot:
                raise InvalidTradeError(f"No market data available for {symbol}")

            current_price = latest_snapshot.close

            # Create trade object
            trade = Trade(
                trade_id=uuid4(),
                portfolio_id=portfolio_id,
                symbol=symbol,
                timestamp=datetime.now(),
                side=side,
                qty=quantity,
                price=current_price,
                pip_pct=broker_profile.pip_pct,
                fee_flat=broker_profile.fee_flat,
                fee_pct=broker_profile.fee_pct,
                unit="share",
                price_ccy=portfolio.base_ccy,
                comment=comment,
            )

            # Validate broker can execute this trade
            if not broker_profile.can_execute_order(quantity, current_price):
                raise InvalidTradeError("Broker cannot execute this order")

            if side == TradeSide.BUY:
                return await self._execute_buy_trade(trade, portfolio)
            else:
                return await self._execute_sell_trade(trade, portfolio)

        # Execute with base service error handling and convert to TradeResult
        try:
            result = await self._execute_operation(
                "execute_trade",
                _execute,
                context,
                expected_exceptions=[
                    InvalidTradeError,
                    InsufficientFundsError,
                    InvalidPositionError,
                ],
            )
            # If the result is already a TradeResult, return it directly
            if isinstance(result, TradeResult):
                return result
            else:
                return TradeResult(success=True, trade=result)
        except (DomainError, DataAccessError) as e:
            return TradeResult(success=False, error=e)
        except Exception as e:
            # Log unexpected errors and wrap them in domain error
            self._logger.error(f"Unexpected error during trade execution: {e}")
            return TradeResult(
                success=False,
                error=InvalidTradeError(f"Unexpected trade execution error: {e}"),
            )

    async def _execute_buy_trade(
        self, trade: Trade, portfolio: Portfolio
    ) -> TradeResult:
        """Execute a buy trade."""
        try:
            # Check if portfolio has sufficient funds
            total_cost = trade.net_amount()
            if not portfolio.has_sufficient_cash(total_cost):
                return TradeResult(
                    success=False,
                    error=InsufficientFundsError(
                        f"Insufficient funds: need {total_cost}, have {portfolio.cash_balance}"
                    ),
                )

            # Deduct cash from portfolio
            portfolio.deduct_cash(total_cost)

            # Update or create position
            existing_position = await self.portfolio_repository.get_position(
                trade.portfolio_id, trade.symbol
            )

            if existing_position:
                existing_position.add_shares(trade.qty, trade.price)
                existing_position.last_updated = trade.timestamp
                await self.portfolio_repository.save_position(existing_position)
            else:
                new_position = Position(
                    portfolio_id=trade.portfolio_id,
                    symbol=trade.symbol,
                    qty=trade.qty,
                    avg_cost=trade.price,
                    unit=trade.unit,
                    price_ccy=trade.price_ccy,
                    last_updated=trade.timestamp,
                )
                await self.portfolio_repository.save_position(new_position)

            # Save trade and updated portfolio
            await self.portfolio_repository.save_trade(trade)
            await self.portfolio_repository.save_portfolio(portfolio)

            return TradeResult(success=True, trade=trade)

        except Exception as e:
            self._logger.error(f"Unexpected error in buy trade for {trade.symbol}: {e}")
            return TradeResult(
                success=False, error=InvalidTradeError(f"Buy trade failed: {e}")
            )

    async def _execute_sell_trade(
        self, trade: Trade, portfolio: Portfolio
    ) -> TradeResult:
        """Execute a sell trade."""
        try:
            # Get existing position
            existing_position = await self.portfolio_repository.get_position(
                trade.portfolio_id, trade.symbol
            )

            if not existing_position:
                return TradeResult(
                    success=False,
                    error=InvalidTradeError(f"No position found for {trade.symbol}"),
                )

            # Check if we have enough shares to sell
            if existing_position.qty < trade.qty:
                return TradeResult(
                    success=False,
                    error=InvalidPositionError(
                        f"Insufficient position: have {existing_position.qty} shares, trying to sell {trade.qty}"
                    ),
                )

            # Add proceeds to portfolio
            proceeds = trade.net_amount()  # For sell trades, this is positive proceeds
            portfolio.add_cash(proceeds)

            # Update position
            existing_position.reduce_shares(trade.qty)
            existing_position.last_updated = trade.timestamp

            if existing_position.qty == Decimal("0"):
                # Position is fully closed, remove it
                await self.portfolio_repository.delete_position(
                    trade.portfolio_id, trade.symbol
                )
            else:
                await self.portfolio_repository.save_position(existing_position)

            # Save trade and updated portfolio
            await self.portfolio_repository.save_trade(trade)
            await self.portfolio_repository.save_portfolio(portfolio)

            return TradeResult(success=True, trade=trade)

        except Exception as e:
            self._logger.error(
                f"Unexpected error in sell trade for {trade.symbol}: {e}"
            )
            return TradeResult(
                success=False, error=InvalidTradeError(f"Sell trade failed: {e}")
            )

    async def calculate_portfolio_metrics(self, portfolio_id: UUID) -> PortfolioMetrics:
        """Calculate comprehensive portfolio metrics."""
        portfolio = await self.portfolio_repository.get_portfolio(portfolio_id)
        if not portfolio:
            raise InvalidTradeError(f"Portfolio {portfolio_id} not found")

        positions = await self.portfolio_repository.get_positions_for_portfolio(
            portfolio_id
        )

        total_market_value = Decimal("0")
        total_cost_basis = Decimal("0")

        for position in positions:
            # Get current market price
            latest_snapshot = await self.asset_repository.get_latest_snapshot(
                position.symbol
            )
            if latest_snapshot:
                current_price = latest_snapshot.close
                total_market_value += position.market_value(current_price)

            total_cost_basis += position.cost_basis()

        total_unrealized_pnl = total_market_value - total_cost_basis
        total_portfolio_value = portfolio.cash_balance + total_market_value

        return PortfolioMetrics(
            total_market_value=total_market_value,
            total_cost_basis=total_cost_basis,
            total_unrealized_pnl=total_unrealized_pnl,
            cash_balance=portfolio.cash_balance,
            total_portfolio_value=total_portfolio_value,
            number_of_positions=len(positions),
        )
