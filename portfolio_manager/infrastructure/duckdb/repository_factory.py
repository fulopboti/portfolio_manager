"""Repository factory for creating DuckDB repository instances."""

import logging
from typing import Any

from portfolio_manager.application.ports import AssetRepository, PortfolioRepository
from portfolio_manager.config.schema import DatabaseConfig
from portfolio_manager.infrastructure.data_access.asset_data_access import (
    AssetDataAccess,
)
from portfolio_manager.infrastructure.data_access.exceptions import (
    ConnectionError,
    DataAccessError,
)
from portfolio_manager.infrastructure.data_access.portfolio_data_access import (
    PortfolioDataAccess,
)

from .asset_repository import DuckDBAssetRepository
from .connection import DuckDBConfig, DuckDBConnection
from .portfolio_repository import DuckDBPortfolioRepository
from .query_executor import DuckDBQueryExecutor
from .schema.schema_manager import DuckDBSchemaManager

logger = logging.getLogger(__name__)


class RepositoryAdapterBase:
    """Base class for repository adapters that bridge data access to application ports."""

    def __init__(self, data_access: Any) -> None:
        """Initialize with data access implementation."""
        self.data_access = data_access


class AssetRepositoryAdapter(RepositoryAdapterBase, AssetRepository):
    """Adapter that bridges AssetDataAccess to AssetRepository interface."""

    def __init__(self, asset_data_access: AssetDataAccess):
        """Initialize the asset repository adapter.

        Args:
            asset_data_access: Concrete implementation of AssetDataAccess
        """
        super().__init__(asset_data_access)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # Map AssetRepository methods to AssetDataAccess methods
    async def save_asset(self, asset: Any) -> Any:
        return await self.data_access.save_asset(asset)

    async def get_asset(self, symbol: str) -> Any:
        return await self.data_access.get_asset(symbol)

    async def get_all_assets(self, asset_type: Any = None) -> Any:
        if asset_type is None:
            return await self.data_access.get_all_assets()
        else:
            return await self.data_access.get_assets_by_type(asset_type)

    async def save_snapshot(self, snapshot: Any) -> Any:
        return await self.data_access.save_snapshot(snapshot)

    async def get_latest_snapshot(self, symbol: str) -> Any:
        return await self.data_access.get_latest_snapshot(symbol)

    async def get_historical_snapshots(
        self, symbol: str, start_date: Any, end_date: Any
    ) -> Any:
        return await self.data_access.get_historical_snapshots(
            symbol, start_date, end_date
        )

    async def get_fundamental_metrics(self, symbol: str) -> Any:
        metrics = await self.data_access.get_fundamental_metrics(symbol)
        # Convert Decimal values to dict for application layer
        return dict(metrics) if metrics else None

    async def save_fundamental_metrics(
        self, symbol: str, metrics: dict[str, Any]
    ) -> Any:
        return await self.data_access.save_fundamental_metrics(symbol, metrics)

    async def delete_asset(self, symbol: str) -> Any:
        return await self.data_access.delete_asset(symbol)

    async def asset_exists(self, symbol: str) -> bool:
        result = await self.data_access.asset_exists(symbol)
        return bool(result)

    async def get_snapshot_count(self, symbol: str) -> int:
        result = await self.data_access.get_snapshot_count(symbol)
        return int(result)


class PortfolioRepositoryAdapter(RepositoryAdapterBase, PortfolioRepository):
    """Adapter that bridges PortfolioDataAccess to PortfolioRepository interface."""

    def __init__(self, portfolio_data_access: PortfolioDataAccess):
        """Initialize the portfolio repository adapter.

        Args:
            portfolio_data_access: Concrete implementation of PortfolioDataAccess
        """
        super().__init__(portfolio_data_access)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # Map PortfolioRepository methods to PortfolioDataAccess methods
    async def save_portfolio(self, portfolio: Any) -> Any:
        return await self.data_access.save_portfolio(portfolio)

    async def get_portfolio(self, portfolio_id: Any) -> Any:
        return await self.data_access.get_portfolio(portfolio_id)

    async def get_all_portfolios(self) -> Any:
        return await self.data_access.get_all_portfolios()

    async def delete_portfolio(self, portfolio_id: Any) -> Any:
        return await self.data_access.delete_portfolio(portfolio_id)

    async def save_trade(self, trade: Any) -> Any:
        return await self.data_access.save_trade(trade)

    async def get_trade(self, trade_id: Any) -> Any:
        return await self.data_access.get_trade(trade_id)

    async def get_trades_for_portfolio(
        self, portfolio_id: Any, limit: int | None = None
    ) -> Any:
        return await self.data_access.get_trades_for_portfolio(portfolio_id, limit)

    async def save_position(self, position: Any) -> Any:
        return await self.data_access.save_position(position)

    async def get_position(self, portfolio_id: Any, symbol: str) -> Any:
        return await self.data_access.get_position(portfolio_id, symbol)

    async def get_positions_for_portfolio(self, portfolio_id: Any) -> Any:
        return await self.data_access.get_positions_for_portfolio(portfolio_id)

    async def delete_position(self, portfolio_id: Any, symbol: str) -> Any:
        return await self.data_access.delete_position(portfolio_id, symbol)

    async def portfolio_exists(self, portfolio_id: Any) -> bool:
        result = await self.data_access.portfolio_exists(portfolio_id)
        return bool(result)


class DuckDBRepositoryFactory:
    """Factory for creating DuckDB repository instances with proper dependency injection.

    This factory manages the creation of all repository implementations and their adapters,
    ensuring consistent configuration and proper dependency wiring.
    """

    def __init__(
        self,
        database_path: str,
        auto_initialize: bool = True,
        config: DatabaseConfig | None = None,
    ) -> None:
        """Initialize the repository factory.

        Args:
            database_path: Path to the DuckDB database file
            auto_initialize: Whether to automatically initialize the database schema
            config: Database configuration object (optional)
        """
        self.database_path = database_path
        self.auto_initialize = auto_initialize
        self.config = config
        self._logger = logging.getLogger(__name__)

        # Core infrastructure components
        self._connection: DuckDBConnection | None = None
        self._query_executor: DuckDBQueryExecutor | None = None
        self._schema_manager: DuckDBSchemaManager | None = None

        # Repository instances (cached)
        self._asset_repository: AssetRepository | None = None
        self._portfolio_repository: PortfolioRepository | None = None

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    async def initialize(self) -> None:
        """Initialize the database connection and schema."""
        try:
            # Create DuckDB configuration from config object if available
            duckdb_config = None
            if self.config:
                # Map configuration pragmas to DuckDBConfig fields
                config_overrides: dict[str, Any] = {
                    "read_only": self.config.connection.read_only
                }

                # Map common pragma settings to DuckDBConfig fields
                pragmas = self.config.connection.pragmas or {}
                if "threads" in pragmas:
                    config_overrides["threads"] = int(pragmas["threads"])
                if "memory_limit" in pragmas:
                    config_overrides["memory_limit"] = str(pragmas["memory_limit"])
                if "timezone" in pragmas:
                    config_overrides["timezone"] = str(pragmas["timezone"])
                if "enable_optimizer" in pragmas:
                    config_overrides["enable_optimizer"] = bool(
                        pragmas["enable_optimizer"]
                    )
                if "enable_profiling" in pragmas:
                    config_overrides["enable_profiling"] = bool(
                        pragmas["enable_profiling"]
                    )

                # Include the pragmas dict
                if self.config.connection.pragmas:
                    config_overrides["pragmas"] = self.config.connection.pragmas

                duckdb_config = DuckDBConfig.from_environment(**config_overrides)
                self._logger.info(
                    f"Using configuration: read_only={self.config.connection.read_only}, pragmas={self.config.connection.pragmas}"
                )

            # Initialize connection with configuration
            self._connection = DuckDBConnection(self.database_path, duckdb_config)
            await self._connection.connect()

            # Initialize query executor
            self._query_executor = DuckDBQueryExecutor(self._connection)

            # Initialize schema manager
            self._schema_manager = DuckDBSchemaManager(self._query_executor)

            # Auto-initialize schema if requested
            if self.auto_initialize:
                await self._schema_manager.create_schema()
                self._logger.info("Database schema initialized successfully")

            self._logger.info(
                f"Repository factory initialized with database: {self.database_path}"
            )

        except Exception as e:
            error_msg = f"Failed to initialize repository factory: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e

    async def shutdown(self) -> None:
        """Shutdown the factory and clean up resources."""
        try:
            if self._connection:
                await self._connection.disconnect()

            # Clear cached instances
            self._asset_repository = None
            self._portfolio_repository = None
            self._connection = None
            self._query_executor = None
            self._schema_manager = None

            self.logger.info("Repository factory shutdown complete")

        except Exception as e:
            self.logger.error(f"Error during factory shutdown: {str(e)}")

    def _ensure_initialized(self) -> None:
        """Ensure the factory has been initialized."""
        if self._connection is None or self._query_executor is None:
            raise ConnectionError("Repository factory must be initialized before use")

    def create_asset_repository(self) -> AssetRepository:
        """Create an AssetRepository instance.

        Returns:
            AssetRepository: Configured asset repository implementation
        """
        self._ensure_initialized()

        if self._asset_repository is None:
            # Create concrete DuckDB implementation
            if self._connection is None or self._query_executor is None:
                raise ConnectionError("Connection or query executor not initialized")
            duckdb_repo = DuckDBAssetRepository(self._connection, self._query_executor)

            # Wrap with adapter to match application interface
            self._asset_repository = AssetRepositoryAdapter(duckdb_repo)

            self.logger.debug("Created asset repository instance")

        return self._asset_repository

    def create_portfolio_repository(self) -> PortfolioRepository:
        """Create a PortfolioRepository instance.

        Returns:
            PortfolioRepository: Configured portfolio repository implementation
        """
        self._ensure_initialized()

        if self._portfolio_repository is None:
            # Create concrete DuckDB implementation
            if self._connection is None or self._query_executor is None:
                raise ConnectionError("Connection or query executor not initialized")
            duckdb_repo = DuckDBPortfolioRepository(
                self._connection, self._query_executor
            )

            # Wrap with adapter to match application interface
            self._portfolio_repository = PortfolioRepositoryAdapter(duckdb_repo)

            self.logger.debug("Created portfolio repository instance")

        return self._portfolio_repository

    def get_connection(self) -> DuckDBConnection:
        """Get the underlying database connection.

        Returns:
            DuckDBConnection: The database connection instance
        """
        self._ensure_initialized()
        if self._connection is None:
            raise ConnectionError("Connection not initialized")
        return self._connection

    def get_query_executor(self) -> DuckDBQueryExecutor:
        """Get the query executor.

        Returns:
            DuckDBQueryExecutor: The query executor instance
        """
        self._ensure_initialized()
        if self._query_executor is None:
            raise ConnectionError("Query executor not initialized")
        return self._query_executor

    def get_schema_manager(self) -> DuckDBSchemaManager:
        """Get the schema manager.

        Returns:
            DuckDBSchemaManager: The schema manager instance
        """
        self._ensure_initialized()
        if self._schema_manager is None:
            raise ConnectionError("Schema manager not initialized")
        return self._schema_manager

    async def health_check(self) -> dict[str, Any]:
        """Perform a health check on the repository factory.

        Returns:
            dict: Health check results
        """
        try:
            if not self._connection:
                return {"status": "unhealthy", "reason": "Not initialized"}

            # Check database connectivity
            is_connected = await self._connection.ping()
            if not is_connected:
                return {"status": "unhealthy", "reason": "Database connection failed"}

            # Get connection info
            conn_info = await self._connection.get_connection_info()

            return {
                "status": "healthy",
                "database_path": self.database_path,
                "connection_info": conn_info,
                "repositories": {
                    "asset_repository": self._asset_repository is not None,
                    "portfolio_repository": self._portfolio_repository is not None,
                },
            }

        except Exception as e:
            return {"status": "unhealthy", "reason": f"Health check failed: {str(e)}"}

    async def reset_database(self) -> None:
        """Reset the database by dropping and recreating all tables.

        WARNING: This will delete all data!
        """
        try:
            self._ensure_initialized()

            self.logger.warning("Resetting database - all data will be lost!")

            # Drop and recreate schema
            if self._schema_manager is not None:
                await self._schema_manager.drop_schema()
                await self._schema_manager.create_schema()
            else:
                raise DataAccessError("Schema manager not initialized")

            self.logger.info("Database reset complete")

        except Exception as e:
            error_msg = f"Failed to reset database: {str(e)}"
            self.logger.error(error_msg)
            raise DataAccessError(error_msg) from e


# Convenience function for quick setup
async def create_repository_factory(
    database_path: str, auto_initialize: bool = True
) -> DuckDBRepositoryFactory:
    """Create and initialize a repository factory.

    Args:
        database_path: Path to the DuckDB database file
        auto_initialize: Whether to automatically initialize the database schema

    Returns:
        DuckDBRepositoryFactory: Initialized repository factory
    """
    factory = DuckDBRepositoryFactory(database_path, auto_initialize)
    await factory.initialize()
    return factory
