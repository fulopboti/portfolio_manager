"""DuckDB concrete implementation of PortfolioDataAccess interface."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from portfolio_manager.domain.entities import Portfolio, Position, Trade, TradeSide
from portfolio_manager.infrastructure.data_access.exceptions import (
    DataAccessError,
    NotFoundError,
)
from portfolio_manager.infrastructure.data_access.portfolio_data_access import (
    PortfolioDataAccess,
)

from .base_repository import BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin
from .connection import DuckDBConnection
from .query_executor import DuckDBQueryExecutor


class DuckDBPortfolioRepository(BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin, PortfolioDataAccess):
    """DuckDB concrete implementation of PortfolioDataAccess interface.

    Provides full implementation of portfolio and trading data persistence
    using DuckDB's columnar storage optimized for analytical workloads.
    """

    def __init__(self, connection: DuckDBConnection, query_executor: DuckDBQueryExecutor):
        """Initialize the DuckDB portfolio repository.

        Args:
            connection: Active DuckDB connection
            query_executor: Query executor for database operations
        """
        super().__init__(connection, query_executor)

    # Portfolio Management Methods

    async def save_portfolio(self, portfolio: Portfolio) -> None:
        """Persist a portfolio to the database.

        Args:
            portfolio: Portfolio entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        portfolio_info = str(portfolio.portfolio_id)
        self._log_operation_start("save_portfolio", portfolio_info)

        query = """
        INSERT INTO portfolios
        (portfolio_id, name, base_ccy, cash_balance, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, NOW())
        ON CONFLICT (portfolio_id) DO UPDATE SET
            name = EXCLUDED.name,
            base_ccy = EXCLUDED.base_ccy,
            cash_balance = EXCLUDED.cash_balance,
            updated_at = NOW()
        """
        parameters = [
            str(portfolio.portfolio_id),
            portfolio.name,
            portfolio.base_ccy,
            str(portfolio.cash_balance),
            portfolio.created
        ]

        await self._execute_query(query, parameters, f"save_portfolio({portfolio_info})")
        self._log_operation_success("save_portfolio", portfolio_info)

    async def get_portfolio(self, portfolio_id: UUID) -> Portfolio | None:
        """Retrieve a portfolio by ID.

        Args:
            portfolio_id: Unique portfolio identifier

        Returns:
            Portfolio entity if found, None otherwise
        """
        query = """
        SELECT portfolio_id, name, base_ccy, cash_balance, created_at
        FROM portfolios
        WHERE portfolio_id = ?
        """
        result = await self._fetch_one(query, [str(portfolio_id)], f"get_portfolio({portfolio_id})")

        if result is None:
            return None

        return Portfolio(
            portfolio_id=self._safe_uuid_convert(result[0]),
            name=result[1],
            base_ccy=result[2],
            cash_balance=self._safe_decimal_convert(result[3]),
            created=result[4]
        )

    async def get_all_portfolios(self) -> list[Portfolio]:
        """Retrieve all portfolios.

        Returns:
            List of all Portfolio entities
        """
        try:
            query = """
            SELECT portfolio_id, name, base_ccy, cash_balance, created_at
            FROM portfolios
            ORDER BY name
            """
            results = await self.query_executor.fetch_all(query)

            return [
                Portfolio(
                    portfolio_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    name=row[1],
                    base_ccy=row[2],
                    cash_balance=self._safe_decimal_convert(row[3]),
                    created=row[4]
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get all portfolios: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def update_portfolio(self, portfolio: Portfolio) -> None:
        """Update an existing portfolio.

        Args:
            portfolio: Portfolio entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If portfolio doesn't exist
        """
        try:
            # Check if portfolio exists
            if not await self.portfolio_exists(portfolio.portfolio_id):
                raise NotFoundError(f"Portfolio {portfolio.portfolio_id} not found")

            query = """
            UPDATE portfolios
            SET name = ?, base_ccy = ?, cash_balance = ?, updated_at = CURRENT_TIMESTAMP
            WHERE portfolio_id = ?
            """
            parameters = [
                portfolio.name,
                portfolio.base_ccy,
                str(portfolio.cash_balance),
                str(portfolio.portfolio_id)
            ]

            await self.query_executor.execute_query(query, parameters)
            self.logger.debug(f"Updated portfolio: {portfolio.portfolio_id}")

        except NotFoundError:
            raise
        except Exception as e:
            error_msg = f"Failed to update portfolio {portfolio.portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def delete_portfolio(self, portfolio_id: UUID) -> None:
        """Delete a portfolio and all related data.

        Args:
            portfolio_id: ID of portfolio to delete

        Raises:
            DataAccessError: If deletion fails
        """
        try:
            # Delete related data first (cascading)
            portfolio_id_str = str(portfolio_id)
            delete_queries = [
                "DELETE FROM portfolio_metrics WHERE portfolio_id = ?",
                "DELETE FROM positions WHERE portfolio_id = ?",
                "DELETE FROM trades WHERE portfolio_id = ?",
                "DELETE FROM portfolios WHERE portfolio_id = ?"
            ]

            for query in delete_queries:
                await self.query_executor.execute_query(query, [portfolio_id_str])

            self.logger.debug(f"Deleted portfolio and all related data: {portfolio_id}")

        except Exception as e:
            error_msg = f"Failed to delete portfolio {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def portfolio_exists(self, portfolio_id: UUID) -> bool:
        """Check if a portfolio exists in the database.

        Args:
            portfolio_id: Portfolio ID to check

        Returns:
            bool: True if portfolio exists, False otherwise
        """
        try:
            query = "SELECT 1 FROM portfolios WHERE portfolio_id = ? LIMIT 1"
            result = await self.query_executor.fetch_one(query, [str(portfolio_id)])
            return result is not None

        except Exception as e:
            error_msg = f"Failed to check portfolio existence {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_portfolio_ids(self) -> set[UUID]:
        """Get all portfolio IDs in the database.

        Returns:
            Set of portfolio UUID identifiers
        """
        try:
            query = "SELECT portfolio_id FROM portfolios ORDER BY portfolio_id"
            results = await self.query_executor.fetch_all(query)
            return {row[0] if isinstance(row[0], UUID) else UUID(row[0]) for row in results}

        except Exception as e:
            error_msg = f"Failed to get portfolio IDs: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def update_portfolio_cash(
        self,
        portfolio_id: UUID,
        new_balance: Decimal
    ) -> None:
        """Update the cash balance of a portfolio.

        Args:
            portfolio_id: Portfolio to update
            new_balance: New cash balance

        Raises:
            DataAccessError: If update fails
            NotFoundError: If portfolio doesn't exist
        """
        try:
            # Check if portfolio exists
            if not await self.portfolio_exists(portfolio_id):
                raise NotFoundError(f"Portfolio {portfolio_id} not found")

            query = """
            UPDATE portfolios
            SET cash_balance = ?, updated_at = CURRENT_TIMESTAMP
            WHERE portfolio_id = ?
            """
            parameters = [str(new_balance), str(portfolio_id)]

            await self.query_executor.execute_query(query, parameters)
            self.logger.debug(f"Updated cash balance for portfolio {portfolio_id}: {new_balance}")

        except NotFoundError:
            raise
        except Exception as e:
            error_msg = f"Failed to update cash balance for portfolio {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Trade Management Methods

    async def save_trade(self, trade: Trade) -> None:
        """Save a trade to the database.

        Args:
            trade: Trade entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        try:
            query = """
            INSERT INTO trades
            (trade_id, portfolio_id, symbol, timestamp, side, qty, price,
             pip_pct, fee_flat, fee_pct, unit, price_ccy, comment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (trade_id) DO UPDATE SET
                portfolio_id = EXCLUDED.portfolio_id,
                symbol = EXCLUDED.symbol,
                timestamp = EXCLUDED.timestamp,
                side = EXCLUDED.side,
                qty = EXCLUDED.qty,
                price = EXCLUDED.price,
                pip_pct = EXCLUDED.pip_pct,
                fee_flat = EXCLUDED.fee_flat,
                fee_pct = EXCLUDED.fee_pct,
                unit = EXCLUDED.unit,
                price_ccy = EXCLUDED.price_ccy,
                comment = EXCLUDED.comment
            """
            parameters = [
                str(trade.trade_id),
                str(trade.portfolio_id),
                trade.symbol,
                trade.timestamp,
                trade.side.value,
                str(trade.qty),
                str(trade.price),
                str(trade.pip_pct),
                str(trade.fee_flat),
                str(trade.fee_pct),
                trade.unit,
                trade.price_ccy,
                trade.comment
            ]

            await self.query_executor.execute_query(query, parameters)
            self.logger.debug(f"Saved trade: {trade.trade_id}")

        except Exception as e:
            error_msg = f"Failed to save trade {trade.trade_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trade(self, trade_id: UUID) -> Trade | None:
        """Retrieve a trade by ID.

        Args:
            trade_id: Unique trade identifier

        Returns:
            Trade entity if found, None otherwise
        """
        try:
            query = """
            SELECT trade_id, portfolio_id, symbol, timestamp, side, qty, price,
                   pip_pct, fee_flat, fee_pct, unit, price_ccy, comment
            FROM trades
            WHERE trade_id = ?
            """
            result = await self.query_executor.fetch_one(query, [str(trade_id)])

            if result is None:
                return None

            return Trade(
                trade_id=result[0] if isinstance(result[0], UUID) else UUID(result[0]),
                portfolio_id=result[1] if isinstance(result[1], UUID) else UUID(result[1]),
                symbol=result[2],
                timestamp=result[3],
                side=TradeSide(result[4]),
                qty=Decimal(str(result[5])),
                price=Decimal(str(result[6])),
                pip_pct=Decimal(str(result[7])),
                fee_flat=Decimal(str(result[8])),
                fee_pct=Decimal(str(result[9])),
                unit=result[10],
                price_ccy=result[11],
                comment=result[12] or ""
            )

        except Exception as e:
            error_msg = f"Failed to get trade {trade_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trades_for_portfolio(
        self,
        portfolio_id: UUID,
        limit: int | None = None,
        offset: int | None = None
    ) -> list[Trade]:
        """Get all trades for a specific portfolio.

        Args:
            portfolio_id: Portfolio to get trades for
            limit: Maximum number of trades to return
            offset: Number of trades to skip

        Returns:
            List of Trade entities ordered by timestamp (newest first)
        """
        try:
            query = """
            SELECT trade_id, portfolio_id, symbol, timestamp, side, qty, price,
                   pip_pct, fee_flat, fee_pct, unit, price_ccy, comment
            FROM trades
            WHERE portfolio_id = ?
            ORDER BY timestamp DESC
            """

            if limit is not None:
                query += f" LIMIT {limit}"
                if offset is not None:
                    query += f" OFFSET {offset}"

            results = await self.query_executor.fetch_all(query, [str(portfolio_id)])

            return [
                Trade(
                    trade_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    portfolio_id=row[1] if isinstance(row[1], UUID) else UUID(row[1]),
                    symbol=row[2],
                    timestamp=row[3],
                    side=TradeSide(row[4]),
                    qty=Decimal(str(row[5])),
                    price=Decimal(str(row[6])),
                    pip_pct=Decimal(str(row[7])),
                    fee_flat=Decimal(str(row[8])),
                    fee_pct=Decimal(str(row[9])),
                    unit=row[10],
                    price_ccy=row[11],
                    comment=row[12] or ""
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get trades for portfolio {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trades_for_symbol(
        self,
        portfolio_id: UUID,
        symbol: str,
        limit: int | None = None
    ) -> list[Trade]:
        """Get all trades for a specific asset in a portfolio.

        Args:
            portfolio_id: Portfolio to get trades for
            symbol: Asset symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade entities ordered by timestamp (newest first)
        """
        try:
            query = """
            SELECT trade_id, portfolio_id, symbol, timestamp, side, qty, price,
                   pip_pct, fee_flat, fee_pct, unit, price_ccy, comment
            FROM trades
            WHERE portfolio_id = ? AND symbol = ?
            ORDER BY timestamp DESC
            """

            if limit is not None:
                query += f" LIMIT {limit}"

            results = await self.query_executor.fetch_all(query, [str(portfolio_id), symbol])

            return [
                Trade(
                    trade_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    portfolio_id=row[1] if isinstance(row[1], UUID) else UUID(row[1]),
                    symbol=row[2],
                    timestamp=row[3],
                    side=TradeSide(row[4]),
                    qty=Decimal(str(row[5])),
                    price=Decimal(str(row[6])),
                    pip_pct=Decimal(str(row[7])),
                    fee_flat=Decimal(str(row[8])),
                    fee_pct=Decimal(str(row[9])),
                    unit=row[10],
                    price_ccy=row[11],
                    comment=row[12] or ""
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get trades for {portfolio_id}.{symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trades_in_date_range(
        self,
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> list[Trade]:
        """Get trades within a specific date range.

        Args:
            portfolio_id: Portfolio to get trades for
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            List of Trade entities ordered by timestamp
        """
        try:
            query = """
            SELECT trade_id, portfolio_id, symbol, timestamp, side, qty, price,
                   pip_pct, fee_flat, fee_pct, unit, price_ccy, comment
            FROM trades
            WHERE portfolio_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """

            results = await self.query_executor.fetch_all(
                query, [str(portfolio_id), start_date, end_date]
            )

            return [
                Trade(
                    trade_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    portfolio_id=row[1] if isinstance(row[1], UUID) else UUID(row[1]),
                    symbol=row[2],
                    timestamp=row[3],
                    side=TradeSide(row[4]),
                    qty=Decimal(str(row[5])),
                    price=Decimal(str(row[6])),
                    pip_pct=Decimal(str(row[7])),
                    fee_flat=Decimal(str(row[8])),
                    fee_pct=Decimal(str(row[9])),
                    unit=row[10],
                    price_ccy=row[11],
                    comment=row[12] or ""
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get trades in date range for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trade_count(self, portfolio_id: UUID) -> int:
        """Get total number of trades for a portfolio.

        Args:
            portfolio_id: Portfolio to count trades for

        Returns:
            Total count of trades
        """
        try:
            # Use the count pattern for cleaner code
            return await self._count_pattern("trades", {"portfolio_id": str(portfolio_id)})

        except Exception as e:
            error_msg = f"Failed to get trade count for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_trade_volume_stats(
        self,
        portfolio_id: UUID,
        start_date: datetime | None = None,
        end_date: datetime | None = None
    ) -> dict[str, Decimal]:
        """Get trade volume statistics for a portfolio.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of analysis period (optional)
            end_date: End of analysis period (optional)

        Returns:
            Dictionary with volume statistics (total_volume, avg_trade_size, etc.)
        """
        try:
            base_query = """
            SELECT
                COUNT(*) as trade_count,
                SUM(qty * price) as total_volume,
                AVG(qty * price) as avg_trade_size,
                MIN(qty * price) as min_trade_size,
                MAX(qty * price) as max_trade_size,
                SUM(CASE WHEN side = 'BUY' THEN qty * price ELSE 0 END) as buy_volume,
                SUM(CASE WHEN side = 'SELL' THEN qty * price ELSE 0 END) as sell_volume
            FROM trades
            WHERE portfolio_id = ?
            """

            parameters = [str(portfolio_id)]

            if start_date and end_date:
                base_query += " AND timestamp >= ? AND timestamp <= ?"
                parameters.extend([start_date, end_date])
            elif start_date:
                base_query += " AND timestamp >= ?"
                parameters.append(start_date)
            elif end_date:
                base_query += " AND timestamp <= ?"
                parameters.append(end_date)

            result = await self.query_executor.fetch_one(base_query, parameters)

            if not result or result[0] == 0:
                return {
                    'trade_count': Decimal('0'),
                    'total_volume': Decimal('0'),
                    'avg_trade_size': Decimal('0'),
                    'min_trade_size': Decimal('0'),
                    'max_trade_size': Decimal('0'),
                    'buy_volume': Decimal('0'),
                    'sell_volume': Decimal('0'),
                    'net_volume': Decimal('0')
                }

            buy_volume = Decimal(str(result[5])) if result[5] else Decimal('0')
            sell_volume = Decimal(str(result[6])) if result[6] else Decimal('0')

            return {
                'trade_count': Decimal(str(result[0])),
                'total_volume': Decimal(str(result[1])) if result[1] else Decimal('0'),
                'avg_trade_size': Decimal(str(result[2])) if result[2] else Decimal('0'),
                'min_trade_size': Decimal(str(result[3])) if result[3] else Decimal('0'),
                'max_trade_size': Decimal(str(result[4])) if result[4] else Decimal('0'),
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'net_volume': buy_volume - sell_volume
            }

        except Exception as e:
            error_msg = f"Failed to get trade volume stats for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Position Management Methods

    async def save_position(self, position: Position) -> None:
        """Save a position to the database.

        Args:
            position: Position entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        try:
            query = """
            INSERT INTO positions
            (portfolio_id, symbol, qty, avg_cost, unit, price_ccy, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (portfolio_id, symbol) DO UPDATE SET
                qty = EXCLUDED.qty,
                avg_cost = EXCLUDED.avg_cost,
                unit = EXCLUDED.unit,
                price_ccy = EXCLUDED.price_ccy,
                last_updated = EXCLUDED.last_updated
            """
            parameters = [
                str(position.portfolio_id),
                position.symbol,
                str(position.qty),
                str(position.avg_cost),
                position.unit,
                position.price_ccy,
                position.last_updated
            ]

            await self.query_executor.execute_query(query, parameters)
            self.logger.debug(f"Saved position: {position.portfolio_id}.{position.symbol}")

        except Exception as e:
            error_msg = f"Failed to save position {position.portfolio_id}.{position.symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_position(
        self,
        portfolio_id: UUID,
        symbol: str
    ) -> Position | None:
        """Get a specific position.

        Args:
            portfolio_id: Portfolio containing the position
            symbol: Asset symbol

        Returns:
            Position entity if found, None otherwise
        """
        try:
            query = """
            SELECT portfolio_id, symbol, qty, avg_cost, unit, price_ccy, last_updated
            FROM positions
            WHERE portfolio_id = ? AND symbol = ?
            """
            result = await self.query_executor.fetch_one(query, [str(portfolio_id), symbol])

            if result is None:
                return None

            return Position(
                portfolio_id=UUID(result[0]),
                symbol=result[1],
                qty=Decimal(str(result[2])),
                avg_cost=Decimal(str(result[3])),
                unit=result[4],
                price_ccy=result[5],
                last_updated=result[6]
            )

        except Exception as e:
            error_msg = f"Failed to get position {portfolio_id}.{symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_positions_for_portfolio(self, portfolio_id: UUID) -> list[Position]:
        """Get all positions for a portfolio.

        Args:
            portfolio_id: Portfolio to get positions for

        Returns:
            List of Position entities
        """
        try:
            query = """
            SELECT portfolio_id, symbol, qty, avg_cost, unit, price_ccy, last_updated
            FROM positions
            WHERE portfolio_id = ? AND qty > 0
            ORDER BY symbol
            """
            results = await self.query_executor.fetch_all(query, [str(portfolio_id)])

            return [
                Position(
                    portfolio_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    symbol=row[1],
                    qty=Decimal(str(row[2])),
                    avg_cost=Decimal(str(row[3])),
                    unit=row[4],
                    price_ccy=row[5],
                    last_updated=row[6]
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get positions for portfolio {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_positions_for_symbols(
        self,
        portfolio_id: UUID,
        symbols: list[str]
    ) -> dict[str, Position | None]:
        """Get positions for multiple symbols.

        Args:
            portfolio_id: Portfolio to get positions for
            symbols: List of asset symbols

        Returns:
            Dictionary mapping symbols to their positions
        """
        try:
            if not symbols:
                return {}

            placeholders = ','.join(['?'] * len(symbols))
            query = f"""
            SELECT portfolio_id, symbol, qty, avg_cost, unit, price_ccy, last_updated
            FROM positions
            WHERE portfolio_id = ? AND symbol IN ({placeholders}) AND qty > 0
            """

            parameters = [str(portfolio_id)] + symbols
            results = await self.query_executor.fetch_all(query, parameters)

            # Build result dictionary
            result_dict = dict.fromkeys(symbols)
            for row in results:
                symbol = row[1]
                result_dict[symbol] = Position(
                    portfolio_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    symbol=row[1],
                    qty=Decimal(str(row[2])),
                    avg_cost=Decimal(str(row[3])),
                    unit=row[4],
                    price_ccy=row[5],
                    last_updated=row[6]
                )

            return result_dict

        except Exception as e:
            error_msg = f"Failed to get positions for symbols in {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def update_position(self, position: Position) -> None:
        """Update an existing position.

        Args:
            position: Position entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If position doesn't exist
        """
        try:
            query = """
            UPDATE positions
            SET qty = ?, avg_cost = ?, unit = ?, price_ccy = ?, last_updated = ?
            WHERE portfolio_id = ? AND symbol = ?
            """
            parameters = [
                str(position.qty),
                str(position.avg_cost),
                position.unit,
                position.price_ccy,
                position.last_updated,
                str(position.portfolio_id),
                position.symbol
            ]

            cursor = await self.query_executor.execute_query(query, parameters)
            if cursor and cursor.rowcount == 0:
                raise NotFoundError(f"Position {position.portfolio_id}.{position.symbol} not found")

            self.logger.debug(f"Updated position: {position.portfolio_id}.{position.symbol}")

        except NotFoundError:
            raise
        except Exception as e:
            error_msg = f"Failed to update position {position.portfolio_id}.{position.symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def delete_position(self, portfolio_id: UUID, symbol: str) -> None:
        """Delete a position from a portfolio.

        Args:
            portfolio_id: Portfolio containing the position
            symbol: Asset symbol of position to delete

        Raises:
            DataAccessError: If deletion fails
        """
        try:
            query = "DELETE FROM positions WHERE portfolio_id = ? AND symbol = ?"
            await self.query_executor.execute_query(query, [str(portfolio_id), symbol])

            self.logger.debug(f"Deleted position: {portfolio_id}.{symbol}")

        except Exception as e:
            error_msg = f"Failed to delete position {portfolio_id}.{symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_position_count(self, portfolio_id: UUID) -> int:
        """Get total number of positions in a portfolio.

        Args:
            portfolio_id: Portfolio to count positions for

        Returns:
            Total count of positions
        """
        try:
            # Note: This has qty > 0 condition, so we need custom query for now
            # Could be enhanced in the future to support complex WHERE conditions
            query = "SELECT COUNT(*) FROM positions WHERE portfolio_id = ? AND qty > 0"
            result = await self._fetch_one(query, [str(portfolio_id)])
            return result[0] if result else 0

        except Exception as e:
            error_msg = f"Failed to get position count for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_largest_positions(
        self,
        portfolio_id: UUID,
        limit: int = 10
    ) -> list[Position]:
        """Get the largest positions by market value.

        Args:
            portfolio_id: Portfolio to analyze
            limit: Maximum number of positions to return

        Returns:
            List of Position entities ordered by market value (descending)
        """
        try:
            query = f"""
            SELECT
                p.portfolio_id, p.symbol, p.qty, p.avg_cost, p.unit, p.price_ccy, p.last_updated,
                (p.qty * p.avg_cost) as cost_basis
            FROM positions p
            WHERE p.portfolio_id = ? AND p.qty > 0
            ORDER BY cost_basis DESC
            LIMIT {limit}
            """
            results = await self.query_executor.fetch_all(query, [str(portfolio_id)])

            return [
                Position(
                    portfolio_id=row[0] if isinstance(row[0], UUID) else UUID(row[0]),
                    symbol=row[1],
                    qty=Decimal(str(row[2])),
                    avg_cost=Decimal(str(row[3])),
                    unit=row[4],
                    price_ccy=row[5],
                    last_updated=row[6]
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get largest positions for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Portfolio Analytics Methods (Simplified implementations)

    async def calculate_portfolio_value(
        self,
        portfolio_id: UUID,
        as_of_date: datetime | None = None
    ) -> dict[str, Decimal]:
        """Calculate total portfolio value and breakdown.

        Args:
            portfolio_id: Portfolio to calculate value for
            as_of_date: Date to calculate value for (defaults to now)

        Returns:
            Dictionary with cash_value, market_value, total_value
        """
        try:
            # Get portfolio cash balance
            portfolio = await self.get_portfolio(portfolio_id)
            if not portfolio:
                raise NotFoundError(f"Portfolio {portfolio_id} not found")

            cash_value = portfolio.cash_balance

            # Calculate market value from positions (simplified - using avg_cost as proxy)
            # In a real implementation, this would use current market prices
            query = """
            SELECT SUM(qty * avg_cost) as invested_value
            FROM positions
            WHERE portfolio_id = ? AND qty > 0
            """
            result = await self.query_executor.fetch_one(query, [str(portfolio_id)])

            market_value = Decimal(str(result[0])) if result and result[0] else Decimal('0')
            total_value = cash_value + market_value

            return {
                'cash_value': cash_value,
                'market_value': market_value,
                'total_value': total_value
            }

        except Exception as e:
            error_msg = f"Failed to calculate portfolio value for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def calculate_portfolio_returns(
        self,
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> dict[str, Decimal]:
        """Calculate portfolio returns over a period.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with total_return, annualized_return, etc.
        """
        try:
            # Simplified implementation - calculate based on cash flows
            trades = await self.get_trades_in_date_range(portfolio_id, start_date, end_date)

            total_invested = sum(
                trade.net_amount() for trade in trades if trade.side == TradeSide.BUY
            )
            total_divested = sum(
                trade.net_amount() for trade in trades if trade.side == TradeSide.SELL
            )

            net_cash_flow = total_invested - total_divested

            return {
                'total_invested': total_invested,
                'total_divested': total_divested,
                'net_cash_flow': net_cash_flow,
                'trade_count': Decimal(str(len(trades)))
            }

        except Exception as e:
            error_msg = f"Failed to calculate returns for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_portfolio_allocation(self, portfolio_id: UUID) -> dict[str, Decimal]:
        """Get asset allocation breakdown for a portfolio.

        Args:
            portfolio_id: Portfolio to analyze

        Returns:
            Dictionary mapping asset symbols to allocation percentages
        """
        try:
            query = """
            SELECT
                symbol,
                (qty * avg_cost) as position_value,
                SUM(qty * avg_cost) OVER () as total_invested
            FROM positions
            WHERE portfolio_id = ? AND qty > 0
            """
            results = await self.query_executor.fetch_all(query, [str(portfolio_id)])

            if not results:
                return {}

            allocations = {}
            for row in results:
                symbol = row[0]
                position_value = Decimal(str(row[1]))
                total_invested = Decimal(str(row[2]))

                if total_invested > 0:
                    allocation_pct = (position_value / total_invested) * 100
                    allocations[symbol] = allocation_pct

            return allocations

        except Exception as e:
            error_msg = f"Failed to get portfolio allocation for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_portfolio_performance_history(
        self,
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> list[dict[str, Any]]:
        """Get historical portfolio performance data.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of history period
            end_date: End of history period

        Returns:
            List of dictionaries with date, value, return data
        """
        try:
            # Simplified implementation using daily trade volumes
            query = """
            SELECT
                DATE(timestamp) as trade_date,
                SUM(CASE WHEN side = 'BUY' THEN qty * price ELSE -qty * price END) as net_flow,
                COUNT(*) as trade_count
            FROM trades
            WHERE portfolio_id = ? AND timestamp >= ? AND timestamp <= ?
            GROUP BY DATE(timestamp)
            ORDER BY trade_date
            """

            results = await self.query_executor.fetch_all(
                query, [str(portfolio_id), start_date, end_date]
            )

            return [
                {
                    'date': row[0],
                    'net_flow': Decimal(str(row[1])) if row[1] else Decimal('0.0'),
                    'trade_count': row[2]
                }
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get performance history for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Data Maintenance Methods

    async def cleanup_zero_positions(self, portfolio_id: UUID) -> int:
        """Remove positions with zero quantity.

        Args:
            portfolio_id: Portfolio to clean up

        Returns:
            Number of positions removed
        """
        try:
            query = "DELETE FROM positions WHERE portfolio_id = ? AND qty <= 0"
            cursor = await self.query_executor.execute_query(query, [str(portfolio_id)])
            deleted_count = cursor.rowcount if cursor else 0

            self.logger.debug(f"Cleaned up {deleted_count} zero positions for {portfolio_id}")
            return deleted_count

        except Exception as e:
            error_msg = f"Failed to cleanup zero positions for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def archive_old_trades(
        self,
        portfolio_id: UUID,
        before_date: datetime
    ) -> int:
        """Archive trades older than a specific date.

        Args:
            portfolio_id: Portfolio to archive trades for
            before_date: Archive trades before this date

        Returns:
            Number of trades archived
        """
        try:
            # In a real implementation, this would move trades to an archive table
            # For now, we'll just count them
            query = """
            SELECT COUNT(*) FROM trades
            WHERE portfolio_id = ? AND timestamp < ?
            """
            result = await self.query_executor.fetch_one(
                query, [str(portfolio_id), before_date]
            )

            count = result[0] if result else 0
            self.logger.debug(f"Would archive {count} trades for {portfolio_id} before {before_date}")
            return count

        except Exception as e:
            error_msg = f"Failed to archive trades for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def validate_portfolio_integrity(self, portfolio_id: UUID) -> dict[str, Any]:
        """Validate data integrity for a portfolio.

        Args:
            portfolio_id: Portfolio to validate

        Returns:
            Dictionary with validation results and any issues found
        """
        try:
            issues = []

            # Check if portfolio exists
            if not await self.portfolio_exists(portfolio_id):
                return {'valid': False, 'issues': ['Portfolio does not exist']}

            # Check for negative positions
            query = """
            SELECT COUNT(*) FROM positions
            WHERE portfolio_id = ? AND qty < 0
            """
            result = await self.query_executor.fetch_one(query, [str(portfolio_id)])
            negative_positions = result[0] if result else 0

            if negative_positions > 0:
                issues.append(f"{negative_positions} positions have negative quantities")

            # Check for orphaned trades (trades without corresponding asset)
            query = """
            SELECT COUNT(*) FROM trades t
            LEFT JOIN assets a ON t.symbol = a.symbol
            WHERE t.portfolio_id = ? AND a.symbol IS NULL
            """
            result = await self.query_executor.fetch_one(query, [str(portfolio_id)])
            orphaned_trades = result[0] if result else 0

            if orphaned_trades > 0:
                issues.append(f"{orphaned_trades} trades reference non-existent assets")

            return {
                'valid': len(issues) == 0,
                'issues': issues,
                'negative_positions': negative_positions,
                'orphaned_trades': orphaned_trades
            }

        except Exception as e:
            error_msg = f"Failed to validate portfolio integrity for {portfolio_id}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e
