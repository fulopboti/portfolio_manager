"""DuckDB concrete implementation of AssetDataAccess interface."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from portfolio_manager.domain.entities import Asset, AssetSnapshot, AssetType
from portfolio_manager.infrastructure.data_access.asset_data_access import (
    AssetDataAccess,
)
from portfolio_manager.infrastructure.data_access.exceptions import (
    DataAccessError,
    NotFoundError,
)

from .base_repository import BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin
from .connection import DuckDBConnection
from .query_executor import DuckDBQueryExecutor


class DuckDBAssetRepository(
    BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin, AssetDataAccess
):
    """DuckDB concrete implementation of AssetDataAccess interface.

    Provides full implementation of asset and market data persistence
    using DuckDB's columnar storage optimized for analytical workloads.
    """

    def __init__(
        self, connection: DuckDBConnection, query_executor: DuckDBQueryExecutor
    ):
        """Initialize the DuckDB asset repository.

        Args:
            connection: Active DuckDB connection
            query_executor: Query executor for database operations
        """
        super().__init__(connection, query_executor)

    # Asset Management Methods

    async def save_asset(self, asset: Asset) -> None:
        """Persist an asset to the database.

        Args:
            asset: Asset entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        self._log_operation_start("save_asset", asset.symbol)

        query = """
        INSERT INTO assets (symbol, exchange, asset_type, name, updated_at)
        VALUES (?, ?, ?, ?, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            exchange = EXCLUDED.exchange,
            asset_type = EXCLUDED.asset_type,
            name = EXCLUDED.name,
            updated_at = NOW()
        """
        parameters = [asset.symbol, asset.exchange, asset.asset_type.value, asset.name]

        await self._execute_query(query, parameters, f"save_asset({asset.symbol})")
        self._log_operation_success("save_asset", asset.symbol)

    async def get_asset(self, symbol: str) -> Asset | None:
        """Retrieve an asset by symbol.

        Args:
            symbol: Asset symbol to look up

        Returns:
            Asset entity if found, None otherwise
        """
        query = """
        SELECT symbol, exchange, asset_type, name
        FROM assets
        WHERE symbol = ?
        """
        result = await self._fetch_one(query, [symbol], f"get_asset({symbol})")

        if result is None:
            return None

        return Asset(
            symbol=result[0],
            exchange=result[1],
            asset_type=self._safe_enum_convert(result[2], AssetType),
            name=result[3],
        )

    async def get_assets_by_type(self, asset_type: AssetType) -> list[Asset]:
        """Retrieve all assets of a specific type.

        Args:
            asset_type: Type of assets to retrieve

        Returns:
            List of Asset entities
        """
        try:
            query = """
            SELECT symbol, exchange, asset_type, name
            FROM assets
            WHERE asset_type = ?
            ORDER BY symbol
            """
            results = await self.query_executor.fetch_all(query, [asset_type.value])

            return [
                Asset(
                    symbol=row[0],
                    exchange=row[1],
                    asset_type=AssetType(row[2]),
                    name=row[3],
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get assets by type {asset_type}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_all_assets(self) -> list[Asset]:
        """Retrieve all assets in the database.

        Returns:
            List of all Asset entities
        """
        try:
            query = """
            SELECT symbol, exchange, asset_type, name
            FROM assets
            ORDER BY symbol
            """
            results = await self.query_executor.fetch_all(query)

            return [
                Asset(
                    symbol=row[0],
                    exchange=row[1],
                    asset_type=AssetType(row[2]),
                    name=row[3],
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get all assets: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def update_asset(self, asset: Asset) -> None:
        """Update an existing asset.

        Args:
            asset: Asset entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If asset doesn't exist
        """
        try:
            # Check if asset exists
            if not await self.asset_exists(asset.symbol):
                raise NotFoundError(f"Asset {asset.symbol} not found")

            query = """
            UPDATE assets
            SET exchange = ?, asset_type = ?, name = ?, updated_at = NOW()
            WHERE symbol = ?
            """
            parameters = [
                asset.exchange,
                asset.asset_type.value,
                asset.name,
                asset.symbol,
            ]

            await self.query_executor.execute_query(query, parameters)
            self.logger.debug(f"Updated asset: {asset.symbol}")

        except NotFoundError:
            raise
        except Exception as e:
            error_msg = f"Failed to update asset {asset.symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def delete_asset(self, symbol: str) -> None:
        """Delete an asset and all related data.

        Args:
            symbol: Symbol of asset to delete

        Raises:
            DataAccessError: If deletion fails
        """
        try:
            # Delete related data first (cascading)
            delete_queries = [
                "DELETE FROM asset_metrics WHERE symbol = ?",
                "DELETE FROM asset_snapshots WHERE symbol = ?",
                "DELETE FROM strategy_scores WHERE symbol = ?",
                "DELETE FROM assets WHERE symbol = ?",
            ]

            for query in delete_queries:
                await self.query_executor.execute_query(query, [symbol])

            self.logger.debug(f"Deleted asset and all related data: {symbol}")

        except Exception as e:
            error_msg = f"Failed to delete asset {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def asset_exists(self, symbol: str) -> bool:
        """Check if an asset exists in the database.

        Args:
            symbol: Asset symbol to check

        Returns:
            bool: True if asset exists, False otherwise
        """
        try:
            # Use the exists pattern for cleaner code
            return await self._exists_pattern("assets", {"symbol": symbol})

        except Exception as e:
            error_msg = f"Failed to check asset existence {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_asset_symbols(self) -> set[str]:
        """Get all asset symbols in the database.

        Returns:
            Set of asset symbol strings
        """
        try:
            query = "SELECT symbol FROM assets ORDER BY symbol"
            results = await self.query_executor.fetch_all(query)
            return {row[0] for row in results}

        except Exception as e:
            error_msg = f"Failed to get asset symbols: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Price Data Management Methods

    async def save_snapshot(self, snapshot: AssetSnapshot) -> None:
        """Save a price snapshot for an asset.

        Args:
            snapshot: AssetSnapshot entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        try:
            # Use the upsert pattern for cleaner code
            values = [
                snapshot.symbol,
                snapshot.timestamp,
                snapshot.open,
                snapshot.high,
                snapshot.low,
                snapshot.close,
                snapshot.volume,
            ]

            # The table has compound primary key (symbol, timestamp), so we need custom upsert logic
            # since our query builder doesn't yet support compound conflict columns
            query = """
            INSERT INTO asset_snapshots (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            """
            parameters = self.param_builder.build_parameters(values)
            await self._execute_query(
                query, parameters, f"save_snapshot({snapshot.symbol})"
            )
            self.logger.debug(
                f"Saved snapshot for {snapshot.symbol} at {snapshot.timestamp}"
            )

        except Exception as e:
            error_msg = f"Failed to save snapshot for {snapshot.symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_latest_snapshot(self, symbol: str) -> AssetSnapshot | None:
        """Get the most recent price snapshot for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Latest AssetSnapshot if available, None otherwise
        """
        try:
            query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM asset_snapshots
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """
            result = await self.query_executor.fetch_one(query, [symbol])

            if result is None:
                return None

            return AssetSnapshot(
                symbol=result[0],
                timestamp=result[1],
                open=Decimal(str(result[2])),
                high=Decimal(str(result[3])),
                low=Decimal(str(result[4])),
                close=Decimal(str(result[5])),
                volume=result[6],
            )

        except Exception as e:
            error_msg = f"Failed to get latest snapshot for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_snapshot_at_date(
        self, symbol: str, date: datetime
    ) -> AssetSnapshot | None:
        """Get price snapshot closest to a specific date.

        Args:
            symbol: Asset symbol
            date: Target date

        Returns:
            AssetSnapshot closest to the date, None if none found
        """
        try:
            query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM asset_snapshots
            WHERE symbol = ?
            ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - ?::TIMESTAMP)))
            LIMIT 1
            """
            result = await self.query_executor.fetch_one(query, [symbol, date])

            if result is None:
                return None

            return AssetSnapshot(
                symbol=result[0],
                timestamp=result[1],
                open=Decimal(str(result[2])),
                high=Decimal(str(result[3])),
                low=Decimal(str(result[4])),
                close=Decimal(str(result[5])),
                volume=result[6],
            )

        except Exception as e:
            error_msg = f"Failed to get snapshot at date for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_historical_snapshots(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> list[AssetSnapshot]:
        """Get historical price snapshots for a date range.

        Args:
            symbol: Asset symbol
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            List of AssetSnapshot entities ordered by timestamp
        """
        try:
            query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM asset_snapshots
            WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            """
            results = await self.query_executor.fetch_all(
                query, [symbol, start_date, end_date]
            )

            return [
                AssetSnapshot(
                    symbol=row[0],
                    timestamp=row[1],
                    open=Decimal(str(row[2])),
                    high=Decimal(str(row[3])),
                    low=Decimal(str(row[4])),
                    close=Decimal(str(row[5])),
                    volume=row[6],
                )
                for row in results
            ]

        except Exception as e:
            error_msg = f"Failed to get historical snapshots for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_snapshots_bulk(
        self, symbols: list[str], date: datetime
    ) -> dict[str, AssetSnapshot | None]:
        """Get snapshots for multiple assets at a specific date.

        Args:
            symbols: List of asset symbols
            date: Target date

        Returns:
            Dictionary mapping symbols to their snapshots
        """
        try:
            if not symbols:
                return {}

            # Use a window function to get the closest snapshot for each symbol
            placeholders = ",".join(["?"] * len(symbols))
            query = f"""
            WITH ranked_snapshots AS (
                SELECT
                    symbol, timestamp, open, high, low, close, volume,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - ?::TIMESTAMP)))) as rn
                FROM asset_snapshots
                WHERE symbol IN ({placeholders})
            )
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM ranked_snapshots
            WHERE rn = 1
            """

            parameters = [date] + symbols
            results = await self.query_executor.fetch_all(query, parameters)

            # Build result dictionary
            result_dict = dict.fromkeys(symbols)
            for row in results:
                result_dict[row[0]] = AssetSnapshot(
                    symbol=row[0],
                    timestamp=row[1],
                    open=Decimal(str(row[2])),
                    high=Decimal(str(row[3])),
                    low=Decimal(str(row[4])),
                    close=Decimal(str(row[5])),
                    volume=row[6],
                )

            return result_dict

        except Exception as e:
            error_msg = f"Failed to get bulk snapshots: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def delete_snapshots_before(self, symbol: str, date: datetime) -> int:
        """Delete old snapshots before a specific date.

        Args:
            symbol: Asset symbol
            date: Cutoff date (snapshots before this will be deleted)

        Returns:
            Number of snapshots deleted
        """
        try:
            query = """
            DELETE FROM asset_snapshots
            WHERE symbol = ? AND timestamp < ?
            """
            cursor = await self.query_executor.execute_query(query, [symbol, date])
            deleted_count = cursor.row_count if cursor else 0

            self.logger.debug(
                f"Deleted {deleted_count} snapshots for {symbol} before {date}"
            )
            return deleted_count

        except Exception as e:
            error_msg = f"Failed to delete snapshots for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_snapshot_count(self, symbol: str) -> int:
        """Get total number of snapshots for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Count of snapshots
        """
        try:
            # Use the count pattern for cleaner code
            return await self._count_pattern("asset_snapshots", {"symbol": symbol})

        except Exception as e:
            error_msg = f"Failed to get snapshot count for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Fundamental Data Management Methods

    async def save_fundamental_metrics(
        self,
        symbol: str,
        metrics: dict[str, Decimal],
        as_of_date: datetime | None = None,
    ) -> None:
        """Save fundamental metrics for an asset.

        Args:
            symbol: Asset symbol
            metrics: Dictionary of metric names to values
            as_of_date: Date the metrics are valid for (defaults to now)

        Raises:
            DataAccessError: If save operation fails
        """
        try:
            if not metrics:
                return

            target_date = as_of_date or datetime.now(UTC)

            # Save each metric individually
            for metric_name, value in metrics.items():
                # Check if metric already exists
                exists_query = """
                SELECT 1 FROM asset_metrics
                WHERE symbol = ? AND metric_name = ? AND as_of_date = ?
                """
                exists = await self.query_executor.fetch_one(
                    exists_query, [symbol, metric_name, target_date]
                )

                if exists:
                    # Update existing metric
                    query = """
                    UPDATE asset_metrics
                    SET value = ?, metric_type = 'FUNDAMENTAL'
                    WHERE symbol = ? AND metric_name = ? AND as_of_date = ?
                    """
                    parameters = [str(value), symbol, metric_name, target_date]
                else:
                    # Insert new metric
                    query = """
                    INSERT INTO asset_metrics
                    (symbol, metric_name, metric_type, value, as_of_date)
                    VALUES (?, ?, 'FUNDAMENTAL', ?, ?)
                    """
                    parameters = [symbol, metric_name, str(value), target_date]

                await self.query_executor.execute_query(query, parameters)

            self.logger.debug(f"Saved {len(metrics)} fundamental metrics for {symbol}")

        except Exception as e:
            error_msg = f"Failed to save fundamental metrics for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_fundamental_metrics(
        self, symbol: str, as_of_date: datetime | None = None
    ) -> dict[str, Decimal] | None:
        """Get fundamental metrics for an asset.

        Args:
            symbol: Asset symbol
            as_of_date: Date to get metrics for (defaults to latest)

        Returns:
            Dictionary of metric names to values, None if not found
        """
        try:
            if as_of_date:
                # Get metrics for specific date
                query = """
                SELECT metric_name, value
                FROM asset_metrics
                WHERE symbol = ? AND metric_type = 'FUNDAMENTAL' AND as_of_date <= ?
                ORDER BY as_of_date DESC
                """
                results = await self.query_executor.fetch_all(
                    query, [symbol, as_of_date]
                )
            else:
                # Get latest metrics
                query = """
                WITH latest_metrics AS (
                    SELECT
                        metric_name, value,
                        ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY as_of_date DESC) as rn
                    FROM asset_metrics
                    WHERE symbol = ? AND metric_type = 'FUNDAMENTAL'
                )
                SELECT metric_name, value
                FROM latest_metrics
                WHERE rn = 1
                """
                results = await self.query_executor.fetch_all(query, [symbol])

            if not results:
                return None

            # Deduplicate to get the latest value for each metric
            metrics = {}
            for row in results:
                metric_name = row[0]
                if metric_name not in metrics:  # Take first (latest) value
                    metrics[metric_name] = Decimal(str(row[1]))

            return metrics if metrics else None

        except Exception as e:
            error_msg = f"Failed to get fundamental metrics for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_fundamental_metrics_bulk(
        self, symbols: list[str], as_of_date: datetime | None = None
    ) -> dict[str, dict[str, Decimal] | None]:
        """Get fundamental metrics for multiple assets.

        Args:
            symbols: List of asset symbols
            as_of_date: Date to get metrics for (defaults to latest)

        Returns:
            Dictionary mapping symbols to their metrics
        """
        try:
            if not symbols:
                return {}

            result_dict = dict.fromkeys(symbols)

            # Get metrics for each symbol (could be optimized with a single query)
            for symbol in symbols:
                metrics = await self.get_fundamental_metrics(symbol, as_of_date)
                result_dict[symbol] = metrics

            return result_dict

        except Exception as e:
            error_msg = f"Failed to get bulk fundamental metrics: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def get_metric_history(
        self, symbol: str, metric_name: str, start_date: datetime, end_date: datetime
    ) -> list[tuple[datetime, Decimal]]:
        """Get historical values for a specific fundamental metric.

        Args:
            symbol: Asset symbol
            metric_name: Name of the metric
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of (date, value) tuples ordered by date
        """
        try:
            query = """
            SELECT as_of_date, value
            FROM asset_metrics
            WHERE symbol = ? AND metric_name = ? AND metric_type = 'FUNDAMENTAL'
              AND as_of_date >= ? AND as_of_date <= ?
            ORDER BY as_of_date
            """
            results = await self.query_executor.fetch_all(
                query, [symbol, metric_name, start_date, end_date]
            )

            return [(row[0], Decimal(str(row[1]))) for row in results]

        except Exception as e:
            error_msg = (
                f"Failed to get metric history for {symbol}.{metric_name}: {str(e)}"
            )
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def delete_fundamental_metrics(
        self, symbol: str, before_date: datetime | None = None
    ) -> int:
        """Delete fundamental metrics for an asset.

        Args:
            symbol: Asset symbol
            before_date: Only delete metrics before this date (None = all)

        Returns:
            Number of metric records deleted
        """
        try:
            if before_date:
                query = """
                DELETE FROM asset_metrics
                WHERE symbol = ? AND metric_type = 'FUNDAMENTAL' AND as_of_date < ?
                """
                cursor = await self.query_executor.execute_query(
                    query, [symbol, before_date]
                )
            else:
                query = """
                DELETE FROM asset_metrics
                WHERE symbol = ? AND metric_type = 'FUNDAMENTAL'
                """
                cursor = await self.query_executor.execute_query(query, [symbol])

            deleted_count = cursor.row_count if cursor else 0
            self.logger.debug(
                f"Deleted {deleted_count} fundamental metrics for {symbol}"
            )
            return deleted_count

        except Exception as e:
            error_msg = f"Failed to delete fundamental metrics for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    # Data Quality and Maintenance Methods

    async def get_data_quality_report(self, symbol: str) -> dict[str, Any]:
        """Generate a data quality report for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Dictionary containing quality metrics and statistics
        """
        try:
            report = {}

            # Basic asset info
            asset = await self.get_asset(symbol)
            if asset:
                report["asset_exists"] = True
                report["asset_name"] = asset.name
                report["asset_type"] = asset.asset_type.value
                report["exchange"] = asset.exchange
            else:
                report["asset_exists"] = False
                return report

            # Snapshot statistics
            snapshot_query = """
            SELECT
                COUNT(*) as total_snapshots,
                MIN(timestamp) as first_snapshot,
                MAX(timestamp) as last_snapshot,
                AVG(volume) as avg_volume,
                COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume_count
            FROM asset_snapshots
            WHERE symbol = ?
            """
            snapshot_result = await self.query_executor.fetch_one(
                snapshot_query, [symbol]
            )

            if snapshot_result:
                report["total_snapshots"] = snapshot_result[0]
                report["first_snapshot"] = snapshot_result[1]
                report["last_snapshot"] = snapshot_result[2]
                report["avg_volume"] = (
                    Decimal(str(snapshot_result[3]))
                    if snapshot_result[3]
                    else Decimal("0")
                )
                report["zero_volume_count"] = snapshot_result[4]

                # Data quality score (0-100)
                quality_score = 100
                if snapshot_result[0] == 0:
                    quality_score = 0
                elif (
                    snapshot_result[4] > snapshot_result[0] * 0.1
                ):  # More than 10% zero volume
                    quality_score -= 20

                report["quality_score"] = quality_score

            # Fundamental metrics count
            metrics_query = """
            SELECT COUNT(DISTINCT metric_name) as metric_count
            FROM asset_metrics
            WHERE symbol = ? AND metric_type = 'FUNDAMENTAL'
            """
            metrics_result = await self.query_executor.fetch_one(
                metrics_query, [symbol]
            )
            if metrics_result:
                report["fundamental_metric_count"] = metrics_result[0]

            return report

        except Exception as e:
            error_msg = f"Failed to generate data quality report for {symbol}: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e

    async def vacuum_asset_data(self, symbol: str | None = None) -> None:
        """Optimize storage and clean up fragmented data.

        Args:
            symbol: Specific asset to vacuum, or None for all
        """
        try:
            # DuckDB doesn't have explicit VACUUM, but we can run ANALYZE
            if symbol:
                await self.query_executor.execute_query(
                    "ANALYZE assets WHERE symbol = ?", [symbol]
                )
                await self.query_executor.execute_query(
                    "ANALYZE asset_snapshots WHERE symbol = ?", [symbol]
                )
                await self.query_executor.execute_query(
                    "ANALYZE asset_metrics WHERE symbol = ?", [symbol]
                )
            else:
                await self.query_executor.execute_query("ANALYZE assets")
                await self.query_executor.execute_query("ANALYZE asset_snapshots")
                await self.query_executor.execute_query("ANALYZE asset_metrics")

            self.logger.debug(f"Vacuumed asset data for {symbol or 'all assets'}")

        except Exception as e:
            error_msg = f"Failed to vacuum asset data: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e
