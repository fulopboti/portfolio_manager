"""Additional unit tests for application ports to improve coverage."""

from uuid import UUID

import pytest

from stockapp.application.ports import PortfolioRepository
from stockapp.domain.entities import Position


class TestPortfolioRepositoryAdditionalCoverage:
    """Additional tests for PortfolioRepository to improve coverage."""

    def test_portfolio_repository_get_positions_alias(self):
        """Test the get_positions method alias."""
        
        class MockPortfolioRepository(PortfolioRepository):
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
                return [Position(
                    portfolio_id=portfolio_id,
                    symbol="TEST",
                    qty=100,
                    avg_cost=50.0,
                    unit="share",
                    price_ccy="USD",
                    last_updated=None
                )]
            
            async def delete_position(self, portfolio_id, symbol):
                pass
            
            async def portfolio_exists(self, portfolio_id):
                pass

        # Test that get_positions calls get_positions_for_portfolio
        repo = MockPortfolioRepository()
        portfolio_id = UUID('12345678-1234-1234-1234-123456789012')
        
        # This should work and call the underlying method
        import asyncio
        positions = asyncio.run(repo.get_positions(portfolio_id))
        assert len(positions) == 1
        assert positions[0].symbol == "TEST"