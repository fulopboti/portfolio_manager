"""Unit tests for application ports."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4
import pytest

from portfolio_manager.application.ports import PortfolioRepository
from portfolio_manager.domain.entities import Position
from datetime import datetime, timezone
from decimal import Decimal


class TestPortfolioRepositoryInterface:
    """Test cases for PortfolioRepository interface methods."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Create a mock portfolio repository."""
        repo = Mock(spec=PortfolioRepository)
        repo.get_positions_for_portfolio = AsyncMock()
        repo.get_positions = AsyncMock()
        return repo

    @pytest.fixture
    def sample_position(self):
        """Create a sample position."""
        return Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

    @pytest.mark.asyncio
    async def test_get_positions_alias_method(self, sample_position):
        """Test that get_positions method calls get_positions_for_portfolio."""
        # Create a concrete implementation to test the alias
        class ConcretePortfolioRepository(PortfolioRepository):
            async def save_portfolio(self, portfolio):
                pass

            async def get_portfolio(self, portfolio_id):
                pass

            async def get_all_portfolios(self):
                pass

            async def delete_portfolio(self, portfolio_id):
                pass

            async def save_trade(self, trade):
                pass

            async def get_trade(self, trade_id):
                pass

            async def get_trades_for_portfolio(self, portfolio_id, limit=None):
                pass

            async def save_position(self, position):
                pass

            async def get_position(self, portfolio_id, symbol):
                pass

            async def get_positions_for_portfolio(self, portfolio_id):
                return [sample_position]

            async def delete_position(self, portfolio_id, symbol):
                pass

            async def portfolio_exists(self, portfolio_id):
                pass

        repo = ConcretePortfolioRepository()
        portfolio_id = uuid4()

        # Test the alias method
        result = await repo.get_positions(portfolio_id)

        # Verify it returns the same result as get_positions_for_portfolio
        expected = await repo.get_positions_for_portfolio(portfolio_id)
        assert result == expected
        assert result == [sample_position]
