"""Portfolio and trading data access layer abstractions."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from portfolio_manager.domain.entities import Portfolio, Trade, Position, TradeSide


class PortfolioDataAccess(ABC):
    """Abstract interface for portfolio and trading data persistence.

    Provides methods for storing and retrieving portfolio information,
    trades, positions, and related financial data.
    """

    # Portfolio Management
    @abstractmethod
    async def save_portfolio(self, portfolio: Portfolio) -> None:
        """Persist a portfolio to the database.

        Args:
            portfolio: Portfolio entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_portfolio(self, portfolio_id: UUID) -> Optional[Portfolio]:
        """Retrieve a portfolio by ID.

        Args:
            portfolio_id: Unique portfolio identifier

        Returns:
            Portfolio entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_all_portfolios(self) -> List[Portfolio]:
        """Retrieve all portfolios.

        Returns:
            List of all Portfolio entities
        """
        pass

    @abstractmethod
    async def update_portfolio(self, portfolio: Portfolio) -> None:
        """Update an existing portfolio.

        Args:
            portfolio: Portfolio entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If portfolio doesn't exist
        """
        pass

    @abstractmethod
    async def delete_portfolio(self, portfolio_id: UUID) -> None:
        """Delete a portfolio and all related data.

        Args:
            portfolio_id: ID of portfolio to delete

        Raises:
            DataAccessError: If deletion fails
        """
        pass

    @abstractmethod
    async def portfolio_exists(self, portfolio_id: UUID) -> bool:
        """Check if a portfolio exists in the database.

        Args:
            portfolio_id: Portfolio ID to check

        Returns:
            bool: True if portfolio exists, False otherwise
        """
        pass

    @abstractmethod
    async def get_portfolio_ids(self) -> Set[UUID]:
        """Get all portfolio IDs in the database.

        Returns:
            Set of portfolio UUID identifiers
        """
        pass

    @abstractmethod
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
        pass

    # Trade Management
    @abstractmethod
    async def save_trade(self, trade: Trade) -> None:
        """Save a trade to the database.

        Args:
            trade: Trade entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_trade(self, trade_id: UUID) -> Optional[Trade]:
        """Retrieve a trade by ID.

        Args:
            trade_id: Unique trade identifier

        Returns:
            Trade entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_trades_for_portfolio(
        self, 
        portfolio_id: UUID,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Trade]:
        """Get all trades for a specific portfolio.

        Args:
            portfolio_id: Portfolio to get trades for
            limit: Maximum number of trades to return
            offset: Number of trades to skip

        Returns:
            List of Trade entities ordered by timestamp (newest first)
        """
        pass

    @abstractmethod
    async def get_trades_for_symbol(
        self, 
        portfolio_id: UUID,
        symbol: str,
        limit: Optional[int] = None
    ) -> List[Trade]:
        """Get all trades for a specific asset in a portfolio.

        Args:
            portfolio_id: Portfolio to get trades for
            symbol: Asset symbol
            limit: Maximum number of trades to return

        Returns:
            List of Trade entities ordered by timestamp (newest first)
        """
        pass

    @abstractmethod
    async def get_trades_in_date_range(
        self, 
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> List[Trade]:
        """Get trades within a specific date range.

        Args:
            portfolio_id: Portfolio to get trades for
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            List of Trade entities ordered by timestamp
        """
        pass

    @abstractmethod
    async def get_trade_count(self, portfolio_id: UUID) -> int:
        """Get total number of trades for a portfolio.

        Args:
            portfolio_id: Portfolio to count trades for

        Returns:
            Total count of trades
        """
        pass

    @abstractmethod
    async def get_trade_volume_stats(
        self, 
        portfolio_id: UUID,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Decimal]:
        """Get trade volume statistics for a portfolio.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of analysis period (optional)
            end_date: End of analysis period (optional)

        Returns:
            Dictionary with volume statistics (total_volume, avg_trade_size, etc.)
        """
        pass

    # Position Management
    @abstractmethod
    async def save_position(self, position: Position) -> None:
        """Save a position to the database.

        Args:
            position: Position entity to save

        Raises:
            DataAccessError: If save operation fails
        """
        pass

    @abstractmethod
    async def get_position(
        self, 
        portfolio_id: UUID, 
        symbol: str
    ) -> Optional[Position]:
        """Get a specific position.

        Args:
            portfolio_id: Portfolio containing the position
            symbol: Asset symbol

        Returns:
            Position entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_positions_for_portfolio(self, portfolio_id: UUID) -> List[Position]:
        """Get all positions for a portfolio.

        Args:
            portfolio_id: Portfolio to get positions for

        Returns:
            List of Position entities
        """
        pass

    @abstractmethod
    async def get_positions_for_symbols(
        self, 
        portfolio_id: UUID,
        symbols: List[str]
    ) -> Dict[str, Optional[Position]]:
        """Get positions for multiple symbols.

        Args:
            portfolio_id: Portfolio to get positions for
            symbols: List of asset symbols

        Returns:
            Dictionary mapping symbols to their positions
        """
        pass

    @abstractmethod
    async def update_position(self, position: Position) -> None:
        """Update an existing position.

        Args:
            position: Position entity with updated information

        Raises:
            DataAccessError: If update fails
            NotFoundError: If position doesn't exist
        """
        pass

    @abstractmethod
    async def delete_position(self, portfolio_id: UUID, symbol: str) -> None:
        """Delete a position from a portfolio.

        Args:
            portfolio_id: Portfolio containing the position
            symbol: Asset symbol of position to delete

        Raises:
            DataAccessError: If deletion fails
        """
        pass

    @abstractmethod
    async def get_position_count(self, portfolio_id: UUID) -> int:
        """Get total number of positions in a portfolio.

        Args:
            portfolio_id: Portfolio to count positions for

        Returns:
            Total count of positions
        """
        pass

    @abstractmethod
    async def get_largest_positions(
        self, 
        portfolio_id: UUID,
        limit: int = 10
    ) -> List[Position]:
        """Get the largest positions by market value.

        Args:
            portfolio_id: Portfolio to analyze
            limit: Maximum number of positions to return

        Returns:
            List of Position entities ordered by market value (descending)
        """
        pass

    # Portfolio Analytics
    @abstractmethod
    async def calculate_portfolio_value(
        self, 
        portfolio_id: UUID,
        as_of_date: Optional[datetime] = None
    ) -> Dict[str, Decimal]:
        """Calculate total portfolio value and breakdown.

        Args:
            portfolio_id: Portfolio to calculate value for
            as_of_date: Date to calculate value for (defaults to now)

        Returns:
            Dictionary with cash_value, market_value, total_value
        """
        pass

    @abstractmethod
    async def calculate_portfolio_returns(
        self, 
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Decimal]:
        """Calculate portfolio returns over a period.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with total_return, annualized_return, etc.
        """
        pass

    @abstractmethod
    async def get_portfolio_allocation(self, portfolio_id: UUID) -> Dict[str, Decimal]:
        """Get asset allocation breakdown for a portfolio.

        Args:
            portfolio_id: Portfolio to analyze

        Returns:
            Dictionary mapping asset symbols to allocation percentages
        """
        pass

    @abstractmethod
    async def get_portfolio_performance_history(
        self, 
        portfolio_id: UUID,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get historical portfolio performance data.

        Args:
            portfolio_id: Portfolio to analyze
            start_date: Start of history period
            end_date: End of history period

        Returns:
            List of dictionaries with date, value, return data
        """
        pass

    # Data Maintenance
    @abstractmethod
    async def cleanup_zero_positions(self, portfolio_id: UUID) -> int:
        """Remove positions with zero quantity.

        Args:
            portfolio_id: Portfolio to clean up

        Returns:
            Number of positions removed
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def validate_portfolio_integrity(self, portfolio_id: UUID) -> Dict[str, Any]:
        """Validate data integrity for a portfolio.

        Args:
            portfolio_id: Portfolio to validate

        Returns:
            Dictionary with validation results and any issues found
        """
        pass
