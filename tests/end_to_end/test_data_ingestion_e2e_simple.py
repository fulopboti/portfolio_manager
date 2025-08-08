"""Simplified end-to-end tests for data ingestion with real database."""

import pytest
import tempfile
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from portfolio_manager.config.factory import ConfiguredServiceBuilder
from portfolio_manager.infrastructure.data_providers import MockDataProvider
from portfolio_manager.domain.entities import AssetType


@pytest.mark.e2e
class TestDataIngestionEndToEndSimple:
    """Simplified end-to-end tests using real database."""

    def setup_method(self):
        """Set up test environment with temporary database."""
        # Use in-memory database for cleaner isolation
        import uuid
        self.test_id = str(uuid.uuid4())

        # Override database configuration for testing
        os.environ['PORTFOLIO_MANAGER_DATABASE__CONNECTION__DATABASE_PATH'] = ':memory:'
        os.environ['PORTFOLIO_MANAGER_DATABASE__CONNECTION__MEMORY'] = 'true'

    def teardown_method(self):
        """Clean up environment variables."""
        # Clean up environment variables
        env_vars_to_clean = [
            'PORTFOLIO_MANAGER_DATABASE__CONNECTION__DATABASE_PATH',
            'PORTFOLIO_MANAGER_DATABASE__CONNECTION__MEMORY'
        ]
        for var in env_vars_to_clean:
            if var in os.environ:
                del os.environ[var]

    async def create_service_stack(self):
        """Create and initialize service stack for testing."""
        builder = ConfiguredServiceBuilder()

        # Create and initialize repository factory manually
        repo_factory = builder.factory.create_repository_factory()
        await repo_factory.initialize()

        # Create repositories
        asset_repository = repo_factory.create_asset_repository()

        return {
            'repositories': {'asset': asset_repository},
            'factory': repo_factory,
            'config': builder.factory
        }

    @pytest.mark.asyncio
    async def test_basic_ingestion_workflow(self):
        """Test basic data ingestion workflow with real database."""
        stack = await self.create_service_stack()

        provider = MockDataProvider(base_price=Decimal("100.00"))
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # Test single symbol ingestion
        result = await service.ingest_symbol(
            symbol="TEST_BASIC",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Test Basic Corp."
        )

        # Verify ingestion succeeded
        assert result.success is True
        assert result.symbol == "TEST_BASIC"
        assert result.snapshots_count > 0
        assert result.error is None

        # Verify data was persisted
        asset_repo = stack['repositories']['asset']

        # Check asset exists
        asset = await asset_repo.get_asset("TEST_BASIC")
        assert asset is not None
        assert asset.symbol == "TEST_BASIC"
        assert asset.name == "Test Basic Corp."
        assert asset.asset_type == AssetType.STOCK

        # Check snapshots were saved
        latest_snapshot = await asset_repo.get_latest_snapshot("TEST_BASIC")
        assert latest_snapshot is not None
        assert latest_snapshot.symbol == "TEST_BASIC"
        assert latest_snapshot.open > 0
        assert latest_snapshot.close > 0
        assert latest_snapshot.volume > 0

    @pytest.mark.asyncio
    async def test_batch_ingestion_workflow(self):
        """Test batch ingestion workflow."""
        stack = await self.create_service_stack()

        provider = MockDataProvider()
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # Test batch ingestion
        symbols = ["BATCH_1", "BATCH_2", "BATCH_3"]
        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="TEST"
        )

        # Verify batch results
        assert len(results) == 3
        assert all(r.success for r in results)

        # Verify all assets were created
        asset_repo = stack['repositories']['asset']
        for symbol in symbols:
            asset = await asset_repo.get_asset(symbol)
            assert asset is not None
            assert asset.symbol == symbol

    @pytest.mark.asyncio
    async def test_refresh_workflow(self):
        """Test refresh all assets workflow."""
        stack = await self.create_service_stack()

        provider = MockDataProvider()
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # First, create some assets
        await service.ingest_symbol(
            symbol="REFRESH_1",
            asset_type=AssetType.STOCK,
            exchange="TEST",
            name="Refresh 1"
        )
        await service.ingest_symbol(
            symbol="REFRESH_2",
            asset_type=AssetType.STOCK,
            exchange="TEST", 
            name="Refresh 2"
        )

        # Now refresh all
        results = await service.refresh_all_assets()

        # Verify refresh worked (should only refresh the 2 assets we created)
        refresh_symbols = {r.symbol for r in results}
        assert "REFRESH_1" in refresh_symbols
        assert "REFRESH_2" in refresh_symbols
        assert all(r.success for r in results)

        # Verify assets still exist
        asset_repo = stack['repositories']['asset']
        asset1 = await asset_repo.get_asset("REFRESH_1")
        asset2 = await asset_repo.get_asset("REFRESH_2")
        assert asset1 is not None
        assert asset2 is not None

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in ingestion."""
        stack = await self.create_service_stack()

        # Create provider that fails on specific symbols
        class FailingProvider(MockDataProvider):
            async def get_ohlcv_data(self, symbol, start_date, end_date):
                if symbol == "FAIL_ME":
                    raise Exception("Simulated failure")
                return await super().get_ohlcv_data(symbol, start_date, end_date)

        provider = FailingProvider()
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # Test mixed success/failure
        symbols = ["SUCCESS", "FAIL_ME", "ALSO_SUCCESS"]
        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="TEST"
        )

        assert len(results) == 3
        assert results[0].success is True    # SUCCESS
        assert results[1].success is False   # FAIL_ME
        assert results[2].success is True    # ALSO_SUCCESS

        # Verify only successful assets were created
        asset_repo = stack['repositories']['asset']
        success_asset = await asset_repo.get_asset("SUCCESS")
        fail_asset = await asset_repo.get_asset("FAIL_ME") 
        also_success_asset = await asset_repo.get_asset("ALSO_SUCCESS")

        assert success_asset is not None
        assert fail_asset is None  # Should not exist due to failure
        assert also_success_asset is not None

    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """Test concurrent ingestion operations."""
        import asyncio

        stack = await self.create_service_stack()

        provider = MockDataProvider()
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # Create concurrent tasks
        symbols = [f"CONCURRENT_{i}" for i in range(3)]  # Reduced for faster testing
        tasks = [
            service.ingest_symbol(
                symbol=symbol,
                asset_type=AssetType.STOCK,
                exchange="TEST",
                name=f"{symbol} Corp."
            )
            for symbol in symbols
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all succeeded
        assert len(results) == 3
        for result in results:
            assert not isinstance(result, Exception)
            assert result.success is True

        # Verify all assets were created
        asset_repo = stack['repositories']['asset']
        for symbol in symbols:
            asset = await asset_repo.get_asset(symbol)
            assert asset is not None

    @pytest.mark.asyncio
    async def test_data_persistence(self):
        """Test that data persists correctly."""
        stack = await self.create_service_stack()

        provider = MockDataProvider()
        service = stack['config'].create_data_ingestion_service(
            provider, stack['repositories']['asset']
        )

        # Ingest data
        result = await service.ingest_symbol(
            symbol="PERSIST_TEST",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Persistence Test"
        )

        assert result.success is True

        # Test various retrieval methods
        asset_repo = stack['repositories']['asset']

        # 1. Basic asset retrieval
        asset = await asset_repo.get_asset("PERSIST_TEST")
        assert asset is not None
        assert asset.symbol == "PERSIST_TEST"
        assert asset.name == "Persistence Test"

        # 2. Latest snapshot
        latest = await asset_repo.get_latest_snapshot("PERSIST_TEST")
        assert latest is not None
        assert latest.symbol == "PERSIST_TEST"

        # 3. Fundamental metrics
        fundamentals = await asset_repo.get_fundamental_metrics("PERSIST_TEST")
        assert fundamentals is not None
        assert len(fundamentals) > 0

        # 4. Asset existence
        exists = await asset_repo.asset_exists("PERSIST_TEST")
        assert exists is True

        # 5. All assets
        all_assets = await asset_repo.get_all_assets()
        asset_symbols = {a.symbol for a in all_assets}
        assert "PERSIST_TEST" in asset_symbols