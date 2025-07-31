"""Unit tests for repository interfaces and ports."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

import pytest

from stockapp.application.ports import (
    AssetRepository,
    DataProvider,
    PortfolioRepository,
    StrategyCalculator,
)
from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)
from stockapp.domain.exceptions import RepositoryError


class TestAssetRepositoryInterface:
    """Test cases for AssetRepository interface contract."""

    def test_asset_repository_is_abstract(self):
        """Test that AssetRepository is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            AssetRepository()  # Should raise TypeError

    def test_asset_repository_required_methods(self):
        """Test that AssetRepository defines required abstract methods."""
        required_methods = [
            'save_asset',
            'get_asset',
            'get_all_assets',
            'save_snapshot',
            'get_latest_snapshot',
            'get_historical_snapshots',
            'get_fundamental_metrics',
            'save_fundamental_metrics',
            'delete_asset',
            'asset_exists'
        ]
        
        for method_name in required_methods:
            assert hasattr(AssetRepository, method_name)
            method = getattr(AssetRepository, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    @pytest.mark.asyncio
    async def test_asset_repository_method_signatures(self):
        """Test that AssetRepository methods have correct signatures."""
        # This test ensures the interface methods have the expected signatures
        # We'll create a mock implementation to verify the signatures
        
        class MockAssetRepository(AssetRepository):
            async def save_asset(self, asset: Asset) -> None:
                pass
            
            async def get_asset(self, symbol: str) -> Optional[Asset]:
                pass
            
            async def get_all_assets(self, asset_type: Optional[AssetType] = None) -> List[Asset]:
                pass
            
            async def save_snapshot(self, snapshot: AssetSnapshot) -> None:
                pass
            
            async def get_latest_snapshot(self, symbol: str) -> Optional[AssetSnapshot]:
                pass
            
            async def get_historical_snapshots(
                self, symbol: str, start_date: datetime, end_date: datetime
            ) -> List[AssetSnapshot]:
                pass
            
            async def get_fundamental_metrics(self, symbol: str) -> Optional[dict]:
                pass
            
            async def save_fundamental_metrics(self, symbol: str, metrics: dict) -> None:
                pass
            
            async def delete_asset(self, symbol: str) -> None:
                pass
            
            async def asset_exists(self, symbol: str) -> bool:
                pass
        
        # Should be able to instantiate the mock implementation
        repository = MockAssetRepository()
        assert isinstance(repository, AssetRepository)


class TestPortfolioRepositoryInterface:
    """Test cases for PortfolioRepository interface contract."""

    def test_portfolio_repository_is_abstract(self):
        """Test that PortfolioRepository is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            PortfolioRepository()  # Should raise TypeError

    def test_portfolio_repository_required_methods(self):
        """Test that PortfolioRepository defines required abstract methods."""
        required_methods = [
            'save_portfolio',
            'get_portfolio',
            'get_all_portfolios',
            'delete_portfolio',
            'save_trade',
            'get_trade',
            'get_trades_for_portfolio',
            'save_position',
            'get_position',
            'get_positions_for_portfolio',
            'delete_position',
            'portfolio_exists'
        ]
        
        for method_name in required_methods:
            assert hasattr(PortfolioRepository, method_name)
            method = getattr(PortfolioRepository, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    @pytest.mark.asyncio
    async def test_portfolio_repository_method_signatures(self):
        """Test that PortfolioRepository methods have correct signatures."""
        
        class MockPortfolioRepository(PortfolioRepository):
            async def save_portfolio(self, portfolio: Portfolio) -> None:
                pass
            
            async def get_portfolio(self, portfolio_id: UUID) -> Optional[Portfolio]:
                pass
            
            async def get_all_portfolios(self) -> List[Portfolio]:
                pass
            
            async def delete_portfolio(self, portfolio_id: UUID) -> None:
                pass
            
            async def save_trade(self, trade: Trade) -> None:
                pass
            
            async def get_trade(self, trade_id: UUID) -> Optional[Trade]:
                pass
            
            async def get_trades_for_portfolio(
                self, portfolio_id: UUID, limit: Optional[int] = None
            ) -> List[Trade]:
                pass
            
            async def save_position(self, position: Position) -> None:
                pass
            
            async def get_position(self, portfolio_id: UUID, symbol: str) -> Optional[Position]:
                pass
            
            async def get_positions_for_portfolio(self, portfolio_id: UUID) -> List[Position]:
                pass
            
            async def delete_position(self, portfolio_id: UUID, symbol: str) -> None:
                pass
            
            async def portfolio_exists(self, portfolio_id: UUID) -> bool:
                pass
        
        # Should be able to instantiate the mock implementation
        repository = MockPortfolioRepository()
        assert isinstance(repository, PortfolioRepository)


class TestDataProviderInterface:
    """Test cases for DataProvider interface contract."""

    def test_data_provider_is_abstract(self):
        """Test that DataProvider is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            DataProvider()  # Should raise TypeError

    def test_data_provider_required_methods(self):
        """Test that DataProvider defines required abstract methods."""
        required_methods = [
            'get_ohlcv_data',
            'get_fundamental_data',
            'supports_symbol',
            'get_provider_name',
            'get_rate_limit_info'
        ]
        
        for method_name in required_methods:
            assert hasattr(DataProvider, method_name)
            method = getattr(DataProvider, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    @pytest.mark.asyncio
    async def test_data_provider_method_signatures(self):
        """Test that DataProvider methods have correct signatures."""
        
        class MockDataProvider(DataProvider):
            async def get_ohlcv_data(
                self, symbol: str, start_date: datetime, end_date: datetime
            ) -> List[AssetSnapshot]:
                return []
            
            async def get_fundamental_data(self, symbol: str) -> dict:
                return {}
            
            def supports_symbol(self, symbol: str) -> bool:
                return True
            
            def get_provider_name(self) -> str:
                return "Mock Provider"
            
            def get_rate_limit_info(self) -> dict:
                return {"requests_per_minute": 60}
        
        # Should be able to instantiate the mock implementation
        provider = MockDataProvider()
        assert isinstance(provider, DataProvider)


class TestStrategyCalculatorInterface:
    """Test cases for StrategyCalculator interface contract."""

    def test_strategy_calculator_is_abstract(self):
        """Test that StrategyCalculator is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            StrategyCalculator()  # Should raise TypeError

    def test_strategy_calculator_required_methods(self):
        """Test that StrategyCalculator defines required abstract methods."""
        required_methods = [
            'calculate_score',
            'get_strategy_name',
            'get_strategy_description',
            'get_required_metrics',
            'validate_metrics'
        ]
        
        for method_name in required_methods:
            assert hasattr(StrategyCalculator, method_name)
            method = getattr(StrategyCalculator, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    def test_strategy_calculator_method_signatures(self):
        """Test that StrategyCalculator methods have correct signatures."""
        
        class MockStrategyCalculator(StrategyCalculator):
            def calculate_score(
                self, asset: Asset, snapshot: AssetSnapshot, fundamentals: dict
            ) -> Decimal:
                return Decimal("75.0")
            
            def get_strategy_name(self) -> str:
                return "Mock Strategy"
            
            def get_strategy_description(self) -> str:
                return "A mock strategy for testing"
            
            def get_required_metrics(self) -> List[str]:
                return ["pe_ratio", "dividend_yield"]
            
            def validate_metrics(self, fundamentals: dict) -> bool:
                return all(metric in fundamentals for metric in self.get_required_metrics())
        
        # Should be able to instantiate the mock implementation
        calculator = MockStrategyCalculator()
        assert isinstance(calculator, StrategyCalculator)


class TestRepositoryContractCompliance:
    """Test cases to ensure repository implementations comply with contracts."""

    class ConcreteAssetRepository(AssetRepository):
        """Concrete implementation for testing."""
        
        def __init__(self):
            self.assets = {}
            self.snapshots = {}
            self.fundamentals = {}
        
        async def save_asset(self, asset: Asset) -> None:
            self.assets[asset.symbol] = asset
        
        async def get_asset(self, symbol: str) -> Optional[Asset]:
            return self.assets.get(symbol)
        
        async def get_all_assets(self, asset_type: Optional[AssetType] = None) -> List[Asset]:
            assets = list(self.assets.values())
            if asset_type:
                assets = [a for a in assets if a.asset_type == asset_type]
            return assets
        
        async def save_snapshot(self, snapshot: AssetSnapshot) -> None:
            if snapshot.symbol not in self.snapshots:
                self.snapshots[snapshot.symbol] = []
            self.snapshots[snapshot.symbol].append(snapshot)
        
        async def get_latest_snapshot(self, symbol: str) -> Optional[AssetSnapshot]:
            snapshots = self.snapshots.get(symbol, [])
            return max(snapshots, key=lambda s: s.timestamp) if snapshots else None
        
        async def get_historical_snapshots(
            self, symbol: str, start_date: datetime, end_date: datetime
        ) -> List[AssetSnapshot]:
            snapshots = self.snapshots.get(symbol, [])
            return [
                s for s in snapshots 
                if start_date <= s.timestamp <= end_date
            ]
        
        async def get_fundamental_metrics(self, symbol: str) -> Optional[dict]:
            return self.fundamentals.get(symbol)
        
        async def save_fundamental_metrics(self, symbol: str, metrics: dict) -> None:
            self.fundamentals[symbol] = metrics
        
        async def delete_asset(self, symbol: str) -> None:
            self.assets.pop(symbol, None)
            self.snapshots.pop(symbol, None)
            self.fundamentals.pop(symbol, None)
        
        async def asset_exists(self, symbol: str) -> bool:
            return symbol in self.assets

    @pytest.mark.asyncio
    async def test_asset_repository_contract_compliance(self, sample_asset, sample_asset_snapshot):
        """Test that concrete implementation complies with AssetRepository contract."""
        repository = self.ConcreteAssetRepository()
        
        # Test asset operations
        await repository.save_asset(sample_asset)
        assert await repository.asset_exists(sample_asset.symbol)
        
        retrieved_asset = await repository.get_asset(sample_asset.symbol)
        assert retrieved_asset == sample_asset
        
        all_assets = await repository.get_all_assets()
        assert len(all_assets) == 1
        assert all_assets[0] == sample_asset
        
        # Test snapshot operations
        await repository.save_snapshot(sample_asset_snapshot)
        latest_snapshot = await repository.get_latest_snapshot(sample_asset.symbol)
        assert latest_snapshot == sample_asset_snapshot
        
        # Test fundamentals operations
        fundamentals = {"pe_ratio": Decimal("25.5"), "dividend_yield": Decimal("0.015")}
        await repository.save_fundamental_metrics(sample_asset.symbol, fundamentals)
        
        retrieved_fundamentals = await repository.get_fundamental_metrics(sample_asset.symbol)
        assert retrieved_fundamentals == fundamentals
        
        # Test deletion
        await repository.delete_asset(sample_asset.symbol)
        assert not await repository.asset_exists(sample_asset.symbol)

    class ConcretePortfolioRepository(PortfolioRepository):
        """Concrete implementation for testing."""
        
        def __init__(self):
            self.portfolios = {}
            self.trades = {}
            self.positions = {}
        
        async def save_portfolio(self, portfolio: Portfolio) -> None:
            self.portfolios[portfolio.portfolio_id] = portfolio
        
        async def get_portfolio(self, portfolio_id: UUID) -> Optional[Portfolio]:
            return self.portfolios.get(portfolio_id)
        
        async def get_all_portfolios(self) -> List[Portfolio]:
            return list(self.portfolios.values())
        
        async def delete_portfolio(self, portfolio_id: UUID) -> None:
            self.portfolios.pop(portfolio_id, None)
            # Clean up related data
            self.trades = {k: v for k, v in self.trades.items() if v.portfolio_id != portfolio_id}
            self.positions = {k: v for k, v in self.positions.items() if k[0] != portfolio_id}
        
        async def save_trade(self, trade: Trade) -> None:
            self.trades[trade.trade_id] = trade
        
        async def get_trade(self, trade_id: UUID) -> Optional[Trade]:
            return self.trades.get(trade_id)
        
        async def get_trades_for_portfolio(
            self, portfolio_id: UUID, limit: Optional[int] = None
        ) -> List[Trade]:
            trades = [t for t in self.trades.values() if t.portfolio_id == portfolio_id]
            trades.sort(key=lambda t: t.timestamp, reverse=True)
            return trades[:limit] if limit else trades
        
        async def save_position(self, position: Position) -> None:
            key = (position.portfolio_id, position.symbol)
            self.positions[key] = position
        
        async def get_position(self, portfolio_id: UUID, symbol: str) -> Optional[Position]:
            return self.positions.get((portfolio_id, symbol))
        
        async def get_positions_for_portfolio(self, portfolio_id: UUID) -> List[Position]:
            return [p for (pid, symbol), p in self.positions.items() if pid == portfolio_id]
        
        async def delete_position(self, portfolio_id: UUID, symbol: str) -> None:
            self.positions.pop((portfolio_id, symbol), None)
        
        async def portfolio_exists(self, portfolio_id: UUID) -> bool:
            return portfolio_id in self.portfolios

    @pytest.mark.asyncio
    async def test_portfolio_repository_contract_compliance(
        self, sample_portfolio, sample_buy_trade, sample_position
    ):
        """Test that concrete implementation complies with PortfolioRepository contract."""
        repository = self.ConcretePortfolioRepository()
        
        # Test portfolio operations
        await repository.save_portfolio(sample_portfolio)
        assert await repository.portfolio_exists(sample_portfolio.portfolio_id)
        
        retrieved_portfolio = await repository.get_portfolio(sample_portfolio.portfolio_id)
        assert retrieved_portfolio == sample_portfolio
        
        all_portfolios = await repository.get_all_portfolios()
        assert len(all_portfolios) == 1
        assert all_portfolios[0] == sample_portfolio
        
        # Test trade operations
        await repository.save_trade(sample_buy_trade)
        retrieved_trade = await repository.get_trade(sample_buy_trade.trade_id)
        assert retrieved_trade == sample_buy_trade
        
        portfolio_trades = await repository.get_trades_for_portfolio(sample_portfolio.portfolio_id)
        assert len(portfolio_trades) == 1
        assert portfolio_trades[0] == sample_buy_trade
        
        # Test position operations
        await repository.save_position(sample_position)
        retrieved_position = await repository.get_position(
            sample_portfolio.portfolio_id, sample_position.symbol
        )
        assert retrieved_position == sample_position
        
        portfolio_positions = await repository.get_positions_for_portfolio(sample_portfolio.portfolio_id)
        assert len(portfolio_positions) == 1
        assert portfolio_positions[0] == sample_position
        
        # Test deletion
        await repository.delete_position(sample_portfolio.portfolio_id, sample_position.symbol)
        assert await repository.get_position(sample_portfolio.portfolio_id, sample_position.symbol) is None
        
        await repository.delete_portfolio(sample_portfolio.portfolio_id)
        assert not await repository.portfolio_exists(sample_portfolio.portfolio_id)


class TestErrorHandlingContracts:
    """Test cases for error handling in repository contracts."""

    def test_repository_error_inheritance(self):
        """Test that RepositoryError is properly defined."""
        assert issubclass(RepositoryError, Exception)
        
        # Test creating and raising RepositoryError
        error = RepositoryError("Test error message")
        assert str(error) == "Test error message"
        
        with pytest.raises(RepositoryError, match="Test error message"):
            raise error

    def test_repository_error_with_cause(self):
        """Test RepositoryError with underlying cause."""
        underlying_error = ValueError("Underlying issue")
        repository_error = RepositoryError("Repository operation failed")
        repository_error.__cause__ = underlying_error
        
        assert str(repository_error) == "Repository operation failed"
        assert repository_error.__cause__ == underlying_error