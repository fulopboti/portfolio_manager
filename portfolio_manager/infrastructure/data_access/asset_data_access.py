"""Asset and market data access layer abstractions."""

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any

from portfolio_manager.domain.entities import Asset, AssetSnapshot, AssetType


class AssetDataAccess(ABC):
    """Abstract interface for asset and market data persistence.

    Provides methods for storing and retrieving asset information,
    price history, and fundamental metrics.
    """

    # Asset Management
    @abstractmethod
    async def save_asset(self, asset: Asset) -> None:
        """Persist an asset to the database.

        Args:
            asset: Asset entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_asset(self, symbol: str) -> Asset | None:
        """Retrieve an asset by symbol.

        Args:
            symbol: Asset symbol to look up

        Returns:
            Asset entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_assets_by_type(self, asset_type: AssetType) -> list[Asset]:
        """Retrieve all assets of a specific type.

        Args:
            asset_type: Type of assets to retrieve

        Returns:
            List of Asset entities
        """
        pass

    @abstractmethod
    async def get_all_assets(self) -> list[Asset]:
        """Retrieve all assets in the database.

        Returns:
            List of all Asset entities
        """
        pass

    @abstractmethod
    async def update_asset(self, asset: Asset) -> None:
        """Update an existing asset.

        Args:
            asset: Asset entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If asset doesn't exist
        """
        pass

    @abstractmethod
    async def delete_asset(self, symbol: str) -> None:
        """Delete an asset and all related data.

        Args:
            symbol: Symbol of asset to delete

        Raises:
            DataAccessError: If deletion fails
        """
        pass

    @abstractmethod
    async def asset_exists(self, symbol: str) -> bool:
        """Check if an asset exists in the database.

        Args:
            symbol: Asset symbol to check

        Returns:
            bool: True if asset exists, False otherwise
        """
        pass

    @abstractmethod
    async def get_asset_symbols(self) -> set[str]:
        """Get all asset symbols in the database.

        Returns:
            Set of asset symbol strings
        """
        pass

    # Price Data Management
    @abstractmethod
    async def save_snapshot(self, snapshot: AssetSnapshot) -> None:
        """Save a price snapshot for an asset.

        Args:
            snapshot: AssetSnapshot entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_latest_snapshot(self, symbol: str) -> AssetSnapshot | None:
        """Get the most recent price snapshot for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Latest AssetSnapshot if available, None otherwise
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def delete_snapshots_before(self, symbol: str, date: datetime) -> int:
        """Delete old snapshots before a specific date.

        Args:
            symbol: Asset symbol
            date: Cutoff date (snapshots before this will be deleted)

        Returns:
            Number of snapshots deleted
        """
        pass

    @abstractmethod
    async def get_snapshot_count(self, symbol: str) -> int:
        """Get total number of snapshots for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Count of snapshots
        """
        pass

    # Fundamental Data Management
    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    # Data Quality and Maintenance
    @abstractmethod
    async def get_data_quality_report(self, symbol: str) -> dict[str, Any]:
        """Generate a data quality report for an asset.

        Args:
            symbol: Asset symbol

        Returns:
            Dictionary containing quality metrics and statistics
        """
        pass

    @abstractmethod
    async def vacuum_asset_data(self, symbol: str | None = None) -> None:
        """Optimize storage and clean up fragmented data.

        Args:
            symbol: Specific asset to vacuum, or None for all
        """
        pass
