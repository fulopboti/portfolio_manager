"""Simple integration test for DataIngestionService."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

from portfolio_manager.application.services.data_ingestion import DataIngestionService
from portfolio_manager.domain.entities import AssetSnapshot, AssetType


class SimpleDataProvider:
    """Simple mock data provider for basic testing."""

    async def get_ohlcv_data(self, symbol: str, start_date: datetime, end_date: datetime):
        """Return simple test data."""
        return [AssetSnapshot(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("95.00"),
            close=Decimal("102.00"),
            volume=1000000
        )]

    async def get_fundamental_data(self, symbol: str):
        """Return empty fundamental data."""
        return {}

    def supports_symbol(self, symbol: str):
        """Support all symbols."""
        return True


@pytest.mark.integration
class TestDataIngestionServiceSimple:
    """Simple integration tests for DataIngestionService."""

    @pytest.mark.asyncio
    async def test_service_instantiation_and_basic_functionality(self):
        """Test that service can be instantiated and basic functionality works."""
        # Mock dependencies
        mock_asset_repository = Mock()
        mock_asset_repository.save_asset = AsyncMock()
        mock_asset_repository.save_snapshot = AsyncMock()
        mock_asset_repository.get_asset = AsyncMock(return_value=None)
        mock_asset_repository.get_all_assets = AsyncMock(return_value=[])
        mock_asset_repository.save_fundamental_metrics = AsyncMock()

        simple_provider = SimpleDataProvider()

        # Create service
        service = DataIngestionService(
            data_provider=simple_provider,
            asset_repository=mock_asset_repository,
            batch_size=10,
            retry_attempts=3
        )

        # Test basic ingestion
        result = await service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc."
        )

        # Verify result
        assert result is not None
        assert result.success is True
        assert result.symbol == "AAPL"
        assert result.snapshots_count == 1
        assert result.error is None

        # Verify mocks were called
        mock_asset_repository.save_asset.assert_called_once()
        mock_asset_repository.save_snapshot.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_symbols_ingestion(self):
        """Test multiple symbols ingestion."""
        # Mock dependencies
        mock_asset_repository = Mock()
        mock_asset_repository.save_asset = AsyncMock()
        mock_asset_repository.save_snapshot = AsyncMock()
        mock_asset_repository.get_asset = AsyncMock(return_value=None)
        mock_asset_repository.save_fundamental_metrics = AsyncMock()

        simple_provider = SimpleDataProvider()

        # Create service
        service = DataIngestionService(
            data_provider=simple_provider,
            asset_repository=mock_asset_repository,
            batch_size=10,
            retry_attempts=3
        )

        # Test batch ingestion
        symbols = ["AAPL", "MSFT", "GOOGL"]
        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="NASDAQ"
        )

        # Verify results
        assert len(results) == 3
        assert all(result.success for result in results)
        assert [result.symbol for result in results] == symbols
        assert all(result.snapshots_count == 1 for result in results)

        # Verify mocks were called correct number of times
        assert mock_asset_repository.save_asset.call_count == 3
        assert mock_asset_repository.save_snapshot.call_count == 3

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in ingestion service."""
        # Mock asset repository that fails on save
        mock_asset_repository = Mock()
        mock_asset_repository.get_asset = AsyncMock(return_value=None)
        mock_asset_repository.save_asset = AsyncMock()
        mock_asset_repository.save_snapshot = AsyncMock(side_effect=Exception("Database error"))

        simple_provider = SimpleDataProvider()

        # Create service
        service = DataIngestionService(
            data_provider=simple_provider,
            asset_repository=mock_asset_repository,
            batch_size=10,
            retry_attempts=3
        )

        # Test ingestion with error
        result = await service.ingest_symbol(
            symbol="FAIL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Fail Corp."
        )

        # Verify error result
        assert result is not None
        assert result.success is False
        assert result.symbol == "FAIL"
        assert result.snapshots_count == 0
        assert "Database error" in result.error