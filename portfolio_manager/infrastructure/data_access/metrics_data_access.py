"""Metrics and analytics data access layer abstractions."""

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID


class MetricType(Enum):
    """Types of metrics that can be stored."""

    FUNDAMENTAL = "fundamental"  # P/E, PEG, dividend yield, etc.
    TECHNICAL = "technical"  # RSI, moving averages, etc.
    STRATEGY_SCORE = "strategy_score"  # Strategy ranking scores
    PERFORMANCE = "performance"  # Portfolio performance metrics
    RISK = "risk"  # Risk metrics (VaR, beta, etc.)


class MetricsDataAccess(ABC):
    """Abstract interface for metrics and analytics data persistence.

    Provides methods for storing and retrieving various types of
    calculated metrics, scores, and analytical data.
    """

    # Metric Storage and Retrieval
    @abstractmethod
    async def save_metric(
        self,
        entity_id: str,  # asset symbol or portfolio ID
        metric_name: str,
        metric_type: MetricType,
        value: Decimal,
        as_of_date: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Save a single metric value.

        Args:
            entity_id: Identifier for the entity (asset symbol, portfolio ID)
            metric_name: Name of the metric
            metric_type: Type/category of the metric
            value: Metric value
            as_of_date: Date/time the metric applies to
            metadata: Additional context data

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def save_metrics_batch(
        self,
        metrics: list[
            tuple[str, str, MetricType, Decimal, datetime, dict[str, Any] | None]
        ],
    ) -> None:
        """Save multiple metrics in a single batch operation.

        Args:
            metrics: List of (entity_id, metric_name, metric_type, value, as_of_date, metadata) tuples

        Raises:
            DataAccessError: If batch save operation fails
        """
        pass

    @abstractmethod
    async def get_metric(
        self, entity_id: str, metric_name: str, as_of_date: datetime | None = None
    ) -> tuple[Decimal, datetime, dict[str, Any] | None] | None:
        """Get a specific metric value.

        Args:
            entity_id: Entity identifier
            metric_name: Name of the metric
            as_of_date: Specific date to get metric for (None = latest)

        Returns:
            Tuple of (value, date, metadata) if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_metrics_for_entity(
        self,
        entity_id: str,
        metric_type: MetricType | None = None,
        as_of_date: datetime | None = None,
    ) -> dict[str, tuple[Decimal, datetime, dict[str, Any] | None]]:
        """Get all metrics for an entity.

        Args:
            entity_id: Entity identifier
            metric_type: Filter by metric type (None = all types)
            as_of_date: Specific date to get metrics for (None = latest)

        Returns:
            Dictionary mapping metric names to (value, date, metadata) tuples
        """
        pass

    @abstractmethod
    async def get_metrics_bulk(
        self,
        entity_ids: list[str],
        metric_names: list[str],
        as_of_date: datetime | None = None,
    ) -> dict[str, dict[str, tuple[Decimal, datetime, dict[str, Any] | None] | None]]:
        """Get specific metrics for multiple entities.

        Args:
            entity_ids: List of entity identifiers
            metric_names: List of metric names to retrieve
            as_of_date: Specific date to get metrics for (None = latest)

        Returns:
            Nested dictionary: entity_id -> metric_name -> (value, date, metadata)
        """
        pass

    # Historical Metrics
    @abstractmethod
    async def get_metric_history(
        self, entity_id: str, metric_name: str, start_date: datetime, end_date: datetime
    ) -> list[tuple[datetime, Decimal, dict[str, Any] | None]]:
        """Get historical values for a metric.

        Args:
            entity_id: Entity identifier
            metric_name: Name of the metric
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of (date, value, metadata) tuples ordered by date
        """
        pass

    @abstractmethod
    async def get_metric_statistics(
        self, entity_id: str, metric_name: str, start_date: datetime, end_date: datetime
    ) -> dict[str, Decimal]:
        """Get statistical summary for a metric over time.

        Args:
            entity_id: Entity identifier
            metric_name: Name of the metric
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with min, max, mean, std_dev, median, etc.
        """
        pass

    # Strategy Scores
    @abstractmethod
    async def save_strategy_scores(
        self,
        strategy_name: str,
        scores: dict[str, Decimal],  # asset_symbol -> score
        as_of_date: datetime,
    ) -> None:
        """Save strategy scores for multiple assets.

        Args:
            strategy_name: Name of the strategy
            scores: Dictionary mapping asset symbols to scores
            as_of_date: Date the scores were calculated

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_strategy_scores(
        self,
        strategy_name: str,
        as_of_date: datetime | None = None,
        limit: int | None = None,
    ) -> list[tuple[str, Decimal]]:
        """Get strategy scores for assets.

        Args:
            strategy_name: Name of the strategy
            as_of_date: Specific date to get scores for (None = latest)
            limit: Maximum number of results (None = all)

        Returns:
            List of (asset_symbol, score) tuples ordered by score (descending)
        """
        pass

    @abstractmethod
    async def get_strategy_score_history(
        self,
        strategy_name: str,
        asset_symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[tuple[datetime, Decimal]]:
        """Get historical strategy scores for an asset.

        Args:
            strategy_name: Name of the strategy
            asset_symbol: Asset to get scores for
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of (date, score) tuples ordered by date
        """
        pass

    @abstractmethod
    async def get_available_strategies(self) -> list[str]:
        """Get list of all strategies that have scores stored.

        Returns:
            List of strategy name strings
        """
        pass

    # Performance Metrics
    @abstractmethod
    async def save_portfolio_performance(
        self,
        portfolio_id: UUID,
        performance_data: dict[str, Decimal],
        as_of_date: datetime,
    ) -> None:
        """Save portfolio performance metrics.

        Args:
            portfolio_id: Portfolio identifier
            performance_data: Dictionary of performance metrics
            as_of_date: Date the metrics were calculated

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_portfolio_performance(
        self, portfolio_id: UUID, as_of_date: datetime | None = None
    ) -> dict[str, Decimal] | None:
        """Get portfolio performance metrics.

        Args:
            portfolio_id: Portfolio identifier
            as_of_date: Specific date to get metrics for (None = latest)

        Returns:
            Dictionary of performance metrics if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_portfolio_performance_history(
        self, portfolio_id: UUID, start_date: datetime, end_date: datetime
    ) -> list[tuple[datetime, dict[str, Decimal]]]:
        """Get historical portfolio performance data.

        Args:
            portfolio_id: Portfolio identifier
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of (date, performance_metrics) tuples ordered by date
        """
        pass

    # Risk Metrics
    @abstractmethod
    async def save_risk_metrics(
        self,
        entity_id: str,  # asset symbol or portfolio ID
        risk_data: dict[str, Decimal],
        as_of_date: datetime,
    ) -> None:
        """Save risk metrics for an entity.

        Args:
            entity_id: Entity identifier
            risk_data: Dictionary of risk metrics (VaR, beta, etc.)
            as_of_date: Date the metrics were calculated

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_risk_metrics(
        self, entity_id: str, as_of_date: datetime | None = None
    ) -> dict[str, Decimal] | None:
        """Get risk metrics for an entity.

        Args:
            entity_id: Entity identifier
            as_of_date: Specific date to get metrics for (None = latest)

        Returns:
            Dictionary of risk metrics if found, None otherwise
        """
        pass

    # Data Maintenance
    @abstractmethod
    async def delete_metrics_before_date(
        self,
        entity_id: str,
        before_date: datetime,
        metric_type: MetricType | None = None,
    ) -> int:
        """Delete old metrics before a specific date.

        Args:
            entity_id: Entity identifier
            before_date: Delete metrics before this date
            metric_type: Only delete metrics of this type (None = all types)

        Returns:
            Number of metric records deleted
        """
        pass

    @abstractmethod
    async def cleanup_stale_metrics(self, days_old: int) -> int:
        """Clean up metrics that are older than specified days.

        Args:
            days_old: Delete metrics older than this many days

        Returns:
            Number of metric records deleted
        """
        pass

    @abstractmethod
    async def get_metric_storage_stats(self) -> dict[str, int]:
        """Get statistics about metric storage usage.

        Returns:
            Dictionary with counts by metric type, entity, etc.
        """
        pass

    @abstractmethod
    async def validate_metric_integrity(self) -> list[str]:
        """Validate integrity of stored metrics.

        Returns:
            List of validation error messages (empty if no issues)
        """
        pass

    # Aggregation and Analysis
    @abstractmethod
    async def get_cross_sectional_metrics(
        self,
        metric_name: str,
        entity_ids: list[str],
        as_of_date: datetime | None = None,
    ) -> dict[str, Decimal | None]:
        """Get a specific metric for multiple entities at a point in time.

        Args:
            metric_name: Name of the metric
            entity_ids: List of entity identifiers
            as_of_date: Date to get metrics for (None = latest)

        Returns:
            Dictionary mapping entity IDs to metric values
        """
        pass

    @abstractmethod
    async def calculate_metric_correlation(
        self,
        entity_ids: list[str],
        metric_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[tuple[str, str], Decimal]:
        """Calculate correlation matrix for a metric across entities.

        Args:
            entity_ids: List of entity identifiers
            metric_name: Name of the metric to analyze
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with (entity1, entity2) -> correlation pairs
        """
        pass
