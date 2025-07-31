"""Port interfaces for the application layer following hexagonal architecture."""

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    Portfolio,
    Position,
    Trade,
)


class AssetRepository(ABC):
    """Abstract repository for asset-related data operations."""

    @abstractmethod
    async def save_asset(self, asset: Asset) -> None:
        """Save an asset to the repository."""
        pass

    @abstractmethod
    async def get_asset(self, symbol: str) -> Optional[Asset]:
        """Retrieve an asset by symbol."""
        pass

    @abstractmethod
    async def get_all_assets(self, asset_type: Optional[AssetType] = None) -> List[Asset]:
        """Retrieve all assets, optionally filtered by type."""
        pass

    @abstractmethod
    async def save_snapshot(self, snapshot: AssetSnapshot) -> None:
        """Save an asset snapshot to the repository."""
        pass

    @abstractmethod
    async def get_latest_snapshot(self, symbol: str) -> Optional[AssetSnapshot]:
        """Retrieve the latest snapshot for an asset."""
        pass

    @abstractmethod
    async def get_historical_snapshots(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> List[AssetSnapshot]:
        """Retrieve historical snapshots for an asset within a date range."""
        pass

    @abstractmethod
    async def get_fundamental_metrics(self, symbol: str) -> Optional[dict]:
        """Retrieve fundamental metrics for an asset."""
        pass

    @abstractmethod
    async def save_fundamental_metrics(self, symbol: str, metrics: dict) -> None:
        """Save fundamental metrics for an asset."""
        pass

    @abstractmethod
    async def delete_asset(self, symbol: str) -> None:
        """Delete an asset and all related data."""
        pass

    @abstractmethod
    async def asset_exists(self, symbol: str) -> bool:
        """Check if an asset exists in the repository."""
        pass


class PortfolioRepository(ABC):
    """Abstract repository for portfolio-related data operations."""

    @abstractmethod
    async def save_portfolio(self, portfolio: Portfolio) -> None:
        """Save a portfolio to the repository."""
        pass

    @abstractmethod
    async def get_portfolio(self, portfolio_id: UUID) -> Optional[Portfolio]:
        """Retrieve a portfolio by ID."""
        pass

    @abstractmethod
    async def get_all_portfolios(self) -> List[Portfolio]:
        """Retrieve all portfolios."""
        pass

    @abstractmethod
    async def delete_portfolio(self, portfolio_id: UUID) -> None:
        """Delete a portfolio and all related data."""
        pass

    @abstractmethod
    async def save_trade(self, trade: Trade) -> None:
        """Save a trade to the repository."""
        pass

    @abstractmethod
    async def get_trade(self, trade_id: UUID) -> Optional[Trade]:
        """Retrieve a trade by ID."""
        pass

    @abstractmethod
    async def get_trades_for_portfolio(
        self, portfolio_id: UUID, limit: Optional[int] = None
    ) -> List[Trade]:
        """Retrieve trades for a portfolio, optionally limited."""
        pass

    @abstractmethod
    async def save_position(self, position: Position) -> None:
        """Save a position to the repository."""
        pass

    @abstractmethod
    async def get_position(self, portfolio_id: UUID, symbol: str) -> Optional[Position]:
        """Retrieve a position for a specific asset in a portfolio."""
        pass

    @abstractmethod
    async def get_positions_for_portfolio(self, portfolio_id: UUID) -> List[Position]:
        """Retrieve all positions for a portfolio."""
        pass

    @abstractmethod
    async def delete_position(self, portfolio_id: UUID, symbol: str) -> None:
        """Delete a position from a portfolio."""
        pass

    @abstractmethod
    async def portfolio_exists(self, portfolio_id: UUID) -> bool:
        """Check if a portfolio exists in the repository."""
        pass

    async def get_positions(self, portfolio_id: UUID) -> List[Position]:
        """Get positions for portfolio - alias for get_positions_for_portfolio."""
        return await self.get_positions_for_portfolio(portfolio_id)


class DataProvider(ABC):
    """Abstract interface for external data providers."""

    @abstractmethod
    async def get_ohlcv_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> List[AssetSnapshot]:
        """Retrieve OHLCV data for a symbol within a date range."""
        pass

    @abstractmethod
    async def get_fundamental_data(self, symbol: str) -> dict:
        """Retrieve fundamental data for a symbol."""
        pass

    @abstractmethod
    def supports_symbol(self, symbol: str) -> bool:
        """Check if the provider supports the given symbol."""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Get the name of the data provider."""
        pass

    @abstractmethod
    def get_rate_limit_info(self) -> dict:
        """Get rate limiting information for the provider."""
        pass


class StrategyCalculator(ABC):
    """Abstract interface for strategy calculation engines."""

    @abstractmethod
    def calculate_score(
        self, asset: Asset, snapshot: AssetSnapshot, fundamentals: dict
    ) -> Decimal:
        """Calculate a strategy score for an asset."""
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the name of the strategy."""
        pass

    @abstractmethod
    def get_strategy_description(self) -> str:
        """Get a description of the strategy."""
        pass

    @abstractmethod
    def get_required_metrics(self) -> List[str]:
        """Get the list of required fundamental metrics."""
        pass

    @abstractmethod
    def validate_metrics(self, fundamentals: dict) -> bool:
        """Validate that all required metrics are present."""
        pass