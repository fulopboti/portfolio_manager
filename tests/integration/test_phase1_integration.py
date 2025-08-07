"""Integration tests for Phase 1 implementation completion."""

import tempfile
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio

from stockapp.domain.entities import Asset, AssetSnapshot, AssetType, Portfolio, Trade, TradeSide
from stockapp.application.services.data_ingestion import DataIngestionService
from stockapp.infrastructure.duckdb.repository_factory import create_repository_factory


class MockDataProvider:
    """Mock data provider for testing."""
    
    def __init__(self):
        self.rate_limit = {"calls_per_minute": 60}
    
    async def get_ohlcv_data(self, symbol: str, start_date, end_date):
        """Mock OHLCV data."""
        return [
            AssetSnapshot(
                symbol=symbol,
                timestamp=datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc),
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("99.50"),
                close=Decimal("102.50"),
                volume=1000000
            )
        ]
    
    async def get_fundamental_data(self, symbol: str):
        """Mock fundamental data."""
        return {
            "pe_ratio": Decimal("15.5"),
            "dividend_yield": Decimal("2.3"),
            "market_cap": Decimal("1000000000")
        }
    
    def supports_symbol(self, symbol: str) -> bool:
        return True
    
    def get_provider_name(self) -> str:
        return "MockProvider"
    
    def get_rate_limit_info(self) -> dict:
        return self.rate_limit


@pytest.mark.integration
class TestPhase1Integration:
    """Test the complete Phase 1 implementation integration."""

    @pytest_asyncio.fixture
    async def repository_factory(self):
        """Create a temporary repository factory for testing."""
        import os
        
        # Create temporary directory and path for the database
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_db.db")
        
        factory = await create_repository_factory(db_path, auto_initialize=True)
        
        yield factory
        
        await factory.shutdown()
        
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    @pytest_asyncio.fixture
    async def data_ingestion_service(self, repository_factory):
        """Create a data ingestion service with repositories."""
        asset_repo = repository_factory.create_asset_repository()
        mock_provider = MockDataProvider()
        
        return DataIngestionService(
            data_provider=mock_provider,
            asset_repository=asset_repo
        )

    @pytest.mark.asyncio
    async def test_repository_factory_initialization(self, repository_factory):
        """Test that the repository factory initializes correctly."""
        # Verify factory is healthy
        health = await repository_factory.health_check()
        assert health["status"] == "healthy"
        
        # Verify repositories can be created
        asset_repo = repository_factory.create_asset_repository()
        portfolio_repo = repository_factory.create_portfolio_repository()
        
        assert asset_repo is not None
        assert portfolio_repo is not None

    @pytest.mark.asyncio
    async def test_asset_repository_crud_operations(self, repository_factory):
        """Test basic CRUD operations on asset repository."""
        asset_repo = repository_factory.create_asset_repository()
        
        # Create test asset
        test_asset = Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )
        
        # Test save
        await asset_repo.save_asset(test_asset)
        
        # Test retrieve
        retrieved_asset = await asset_repo.get_asset("AAPL")
        assert retrieved_asset is not None
        assert retrieved_asset.symbol == "AAPL"
        assert retrieved_asset.name == "Apple Inc."
        
        # Test exists
        exists = await asset_repo.asset_exists("AAPL")
        assert exists is True
        
        # Test get all
        all_assets = await asset_repo.get_all_assets()
        assert len(all_assets) == 1
        assert all_assets[0].symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_portfolio_repository_crud_operations(self, repository_factory):
        """Test basic CRUD operations on portfolio repository."""
        portfolio_repo = repository_factory.create_portfolio_repository()
        
        # Create test portfolio
        test_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        
        # Test save
        await portfolio_repo.save_portfolio(test_portfolio)
        
        # Test retrieve
        retrieved_portfolio = await portfolio_repo.get_portfolio(test_portfolio.portfolio_id)
        assert retrieved_portfolio is not None
        assert retrieved_portfolio.name == "Test Portfolio"
        assert retrieved_portfolio.cash_balance == Decimal("10000.00")
        
        # Test exists
        exists = await portfolio_repo.portfolio_exists(test_portfolio.portfolio_id)
        assert exists is True
        
        # Test get all
        all_portfolios = await portfolio_repo.get_all_portfolios()
        assert len(all_portfolios) == 1
        assert all_portfolios[0].name == "Test Portfolio"

    @pytest.mark.asyncio
    async def test_data_ingestion_end_to_end(self, data_ingestion_service, repository_factory):
        """Test complete data ingestion workflow."""
        asset_repo = repository_factory.create_asset_repository()
        
        # Ingest data for a symbol
        result = await data_ingestion_service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc."
        )
        
        # Verify ingestion was successful
        assert result.success is True
        assert result.symbol == "AAPL"
        assert result.snapshots_count == 1
        
        # Verify asset was created
        asset = await asset_repo.get_asset("AAPL")
        assert asset is not None
        assert asset.symbol == "AAPL"
        assert asset.name == "Apple Inc."
        
        # Verify snapshot was saved
        latest_snapshot = await asset_repo.get_latest_snapshot("AAPL")
        assert latest_snapshot is not None
        assert latest_snapshot.symbol == "AAPL"
        assert latest_snapshot.close == Decimal("102.50")
        
        # Verify fundamental data was saved
        fundamentals = await asset_repo.get_fundamental_metrics("AAPL")
        assert fundamentals is not None
        assert "pe_ratio" in fundamentals

    @pytest.mark.asyncio
    async def test_trade_and_position_workflow(self, repository_factory):
        """Test complete trade execution and position tracking workflow."""
        portfolio_repo = repository_factory.create_portfolio_repository()
        asset_repo = repository_factory.create_asset_repository()
        
        # Create test asset
        test_asset = Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )
        await asset_repo.save_asset(test_asset)
        
        # Create test portfolio
        portfolio_id = uuid4()
        test_portfolio = Portfolio(
            portfolio_id=portfolio_id,
            name="Trading Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repo.save_portfolio(test_portfolio)
        
        # Execute a buy trade
        buy_trade = Trade(
            trade_id=uuid4(),
            portfolio_id=portfolio_id,
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("10"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Test buy trade"
        )
        
        await portfolio_repo.save_trade(buy_trade)
        
        # Verify trade was saved
        retrieved_trade = await portfolio_repo.get_trade(buy_trade.trade_id)
        assert retrieved_trade is not None
        assert retrieved_trade.symbol == "AAPL"
        assert retrieved_trade.side == TradeSide.BUY
        
        # Get trades for portfolio
        portfolio_trades = await portfolio_repo.get_trades_for_portfolio(portfolio_id)
        assert len(portfolio_trades) == 1
        assert portfolio_trades[0].trade_id == buy_trade.trade_id

    @pytest.mark.asyncio
    async def test_repository_adapters_compatibility(self, repository_factory):
        """Test that repository adapters properly bridge interfaces."""
        asset_repo = repository_factory.create_asset_repository()
        portfolio_repo = repository_factory.create_portfolio_repository()
        
        # Test that repositories implement the expected interfaces
        from stockapp.application.ports import AssetRepository, PortfolioRepository
        assert isinstance(asset_repo, AssetRepository)
        assert isinstance(portfolio_repo, PortfolioRepository)
        
        # Test method compatibility by calling key methods
        all_assets = await asset_repo.get_all_assets()
        assert isinstance(all_assets, list)
        
        all_portfolios = await portfolio_repo.get_all_portfolios()
        assert isinstance(all_portfolios, list)

    @pytest.mark.asyncio
    async def test_database_schema_integrity(self, repository_factory):
        """Test that database schema is properly created and functional."""
        schema_manager = repository_factory.get_schema_manager()
        
        # Verify schema exists
        schema_info = await schema_manager.get_schema_info()
        assert schema_info is not None
        
        # Verify key tables exist
        expected_tables = [
            "assets", "asset_snapshots", "portfolios", "trades", "positions"
        ]
        
        for table_name in expected_tables:
            table_exists = await schema_manager.table_exists(table_name)
            assert table_exists, f"Table {table_name} should exist"

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, repository_factory):
        """Test concurrent repository operations."""
        asset_repo = repository_factory.create_asset_repository()
        
        # Create multiple assets concurrently
        async def create_asset(symbol: str):
            asset = Asset(
                symbol=symbol,
                exchange="NYSE",
                asset_type=AssetType.STOCK,
                name=f"Test Company {symbol}"
            )
            await asset_repo.save_asset(asset)
            return symbol
        
        # Run concurrent operations
        symbols = ["TEST1", "TEST2", "TEST3", "TEST4", "TEST5"]
        tasks = [create_asset(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        
        # Verify all assets were created
        assert len(results) == 5
        for symbol in symbols:
            exists = await asset_repo.asset_exists(symbol)
            assert exists, f"Asset {symbol} should exist"

    @pytest.mark.asyncio
    async def test_error_handling_and_rollback(self, repository_factory):
        """Test error handling and transaction rollback behavior."""
        asset_repo = repository_factory.create_asset_repository()
        
        # Test saving an invalid asset (this should raise an error)
        with pytest.raises(Exception):
            invalid_asset = Asset(
                symbol="",  # Invalid empty symbol
                exchange="NYSE",
                asset_type=AssetType.STOCK,
                name="Invalid Asset"
            )
            await asset_repo.save_asset(invalid_asset)
        
        # Verify that no invalid asset was saved
        all_assets = await asset_repo.get_all_assets()
        invalid_assets = [a for a in all_assets if a.symbol == ""]
        assert len(invalid_assets) == 0

    @pytest.mark.asyncio
    async def test_data_persistence_across_sessions(self, repository_factory):
        """Test that data persists across repository sessions."""
        asset_repo = repository_factory.create_asset_repository()
        
        # Save an asset
        test_asset = Asset(
            symbol="PERSISTENT",
            exchange="NYSE",
            asset_type=AssetType.STOCK,
            name="Persistent Asset"
        )
        await asset_repo.save_asset(test_asset)
        
        # Shutdown and reinitialize factory
        db_path = repository_factory.database_path
        await repository_factory.shutdown()
        
        # Create new factory with same database
        new_factory = await create_repository_factory(db_path, auto_initialize=False)
        new_asset_repo = new_factory.create_asset_repository()
        
        # Verify asset still exists
        retrieved_asset = await new_asset_repo.get_asset("PERSISTENT")
        assert retrieved_asset is not None
        assert retrieved_asset.name == "Persistent Asset"
        
        await new_factory.shutdown()

    @pytest.mark.asyncio
    async def test_application_service_integration(self, data_ingestion_service):
        """Test that application services work with the repository implementations."""
        # Test multiple symbol ingestion
        symbols = ["AAPL", "MSFT", "GOOGL"]
        results = await data_ingestion_service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="NASDAQ"
        )
        
        # Verify all ingestions were successful
        assert len(results) == 3
        for result in results:
            assert result.success is True
            assert result.snapshots_count > 0
        
        # Test refresh all assets
        refresh_results = await data_ingestion_service.refresh_all_assets()
        assert len(refresh_results) == 3
        for result in refresh_results:
            assert result.success is True