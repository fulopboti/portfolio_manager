"""Simplified integration tests for DataIngestionService."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from typing import List
from unittest.mock import AsyncMock, Mock

from portfolio_manager.application.services.data_ingestion import DataIngestionService
from portfolio_manager.domain.entities import Asset, AssetSnapshot, AssetType


class IntegrationMockProvider:
    """Mock data provider for integration testing."""

    def __init__(self):
        self.call_count = 0
        self.symbols_requested = []

    async def get_ohlcv_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[AssetSnapshot]:
        """Mock OHLCV data generation."""
        self.call_count += 1
        self.symbols_requested.append(symbol)

        # Generate 3 days of test data
        snapshots = []
        base_price = Decimal("100.00")

        for i in range(3):
            price = base_price + Decimal(str(i * 0.5))
            snapshot = AssetSnapshot(
                symbol=symbol,
                timestamp=start_date,
                open=price,
                high=price + Decimal("2.50"),
                low=price - Decimal("1.25"),
                close=price + Decimal("1.00"),
                volume=1000000 + (i * 100000)
            )
            snapshots.append(snapshot)

        return snapshots

    async def get_fundamental_data(self, symbol: str) -> dict:
        """Mock fundamental data."""
        return {
            "pe_ratio": Decimal("20.5"),
            "dividend_yield": Decimal("0.02")
        }

    def supports_symbol(self, symbol: str) -> bool:
        """Support all symbols except specific ones."""
        return symbol not in ["INVALID", "UNSUPPORTED"]


@pytest.mark.integration
class TestDataIngestionIntegrationSimplified:
    """Simplified integration tests that focus on service behavior."""

    @pytest.mark.asyncio
    async def test_service_with_mock_components(self):
        """Test service with mocked components to verify integration."""
        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.get_all_assets = AsyncMock(return_value=[])
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        mock_provider = IntegrationMockProvider()

        # Create service
        service = DataIngestionService(
            data_provider=mock_provider,
            asset_repository=mock_asset_repo,
            batch_size=10,
            retry_attempts=3
        )

        # Test single symbol ingestion
        result = await service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc.",
            start_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 17, tzinfo=timezone.utc)
        )

        # Verify result
        assert result.success is True
        assert result.symbol == "AAPL"
        assert result.snapshots_count == 3
        assert result.error is None

        # Verify interactions
        mock_asset_repo.save_asset.assert_called_once()
        assert mock_asset_repo.save_snapshot.call_count == 3
        mock_asset_repo.save_fundamental_metrics.assert_called_once()

        assert mock_provider.call_count == 1
        assert "AAPL" in mock_provider.symbols_requested

    @pytest.mark.asyncio
    async def test_batch_ingestion_integration(self):
        """Test batch ingestion with multiple symbols."""
        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        mock_provider = IntegrationMockProvider()

        # Create service
        service = DataIngestionService(
            data_provider=mock_provider,
            asset_repository=mock_asset_repo,
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

        # Verify interactions
        assert mock_asset_repo.save_asset.call_count == 3
        assert mock_asset_repo.save_snapshot.call_count == 9  # 3 symbols * 3 snapshots each
        assert mock_asset_repo.save_fundamental_metrics.call_count == 3

        assert mock_provider.call_count == 3
        assert set(mock_provider.symbols_requested) == set(symbols)

    @pytest.mark.asyncio
    async def test_refresh_all_assets_integration(self):
        """Test refresh all assets functionality."""
        # Create existing assets
        existing_assets = [
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple Inc."),
            Asset(symbol="MSFT", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Microsoft Corp."),
        ]

        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.get_all_assets = AsyncMock(return_value=existing_assets)
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        mock_provider = IntegrationMockProvider()

        # Create service
        service = DataIngestionService(
            data_provider=mock_provider,
            asset_repository=mock_asset_repo,
            batch_size=10,
            retry_attempts=3
        )

        # Test refresh
        results = await service.refresh_all_assets()

        # Verify results
        assert len(results) == 2
        assert all(result.success for result in results)
        assert set(result.symbol for result in results) == {"AAPL", "MSFT"}

        # Verify interactions
        mock_asset_repo.get_all_assets.assert_called_once()
        # Assets exist, so no save_asset calls
        # But snapshots should be saved
        assert mock_asset_repo.save_snapshot.call_count == 6  # 2 symbols * 3 snapshots each

        assert mock_provider.call_count == 2
        assert set(mock_provider.symbols_requested) == {"AAPL", "MSFT"}

    @pytest.mark.asyncio
    async def test_error_handling_integration(self):
        """Test error handling in integration scenarios."""

        class FailingProvider(IntegrationMockProvider):
            async def get_ohlcv_data(self, symbol, start_date, end_date):
                if symbol == "FAIL":
                    raise Exception("Provider error")
                return await super().get_ohlcv_data(symbol, start_date, end_date)

        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        failing_provider = FailingProvider()

        # Create service
        service = DataIngestionService(
            data_provider=failing_provider,
            asset_repository=mock_asset_repo,
            batch_size=10,
            retry_attempts=3
        )

        # Test batch with one failing symbol
        symbols = ["AAPL", "FAIL", "MSFT"]
        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="NASDAQ"
        )

        # Verify results
        assert len(results) == 3
        assert results[0].success is True   # AAPL
        assert results[1].success is False  # FAIL
        assert results[2].success is True   # MSFT

        # Verify error result
        assert results[1].symbol == "FAIL"
        assert results[1].snapshots_count == 0
        assert "Provider error" in results[1].error

        # Verify successful symbols were processed
        assert mock_asset_repo.save_asset.call_count == 2  # Only AAPL and MSFT
        assert mock_asset_repo.save_snapshot.call_count == 6  # 2 symbols * 3 snapshots each

    @pytest.mark.asyncio
    async def test_service_configuration_integration(self):
        """Test service configuration in integration context."""
        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        mock_provider = IntegrationMockProvider()

        # Test different configurations
        configs = [
            {"batch_size": 5, "retry_attempts": 2},
            {"batch_size": 50, "retry_attempts": 5},
            {"batch_size": None, "retry_attempts": None},  # Test defaults
        ]

        for config in configs:
            service = DataIngestionService(
                data_provider=mock_provider,
                asset_repository=mock_asset_repo,
                batch_size=config["batch_size"],
                retry_attempts=config["retry_attempts"]
            )

            # Verify configuration was applied
            expected_batch_size = config["batch_size"] if config["batch_size"] is not None else 100
            expected_retry_attempts = config["retry_attempts"] if config["retry_attempts"] is not None else 3

            assert service.batch_size == expected_batch_size
            assert service.retry_attempts == expected_retry_attempts

    @pytest.mark.asyncio 
    async def test_concurrent_operations_integration(self):
        """Test concurrent operations don't interfere."""
        import asyncio

        # Create mock dependencies
        mock_asset_repo = Mock()
        mock_asset_repo.save_asset = AsyncMock()
        mock_asset_repo.save_snapshot = AsyncMock()
        mock_asset_repo.get_asset = AsyncMock(return_value=None)
        mock_asset_repo.save_fundamental_metrics = AsyncMock()

        mock_provider = IntegrationMockProvider()

        # Create service
        service = DataIngestionService(
            data_provider=mock_provider,
            asset_repository=mock_asset_repo,
            batch_size=10,
            retry_attempts=3
        )

        # Create concurrent tasks
        symbols = [f"STOCK{i}" for i in range(5)]
        tasks = [
            service.ingest_symbol(
                symbol=symbol,
                asset_type=AssetType.STOCK,
                exchange="NASDAQ",
                name=f"{symbol} Corp."
            )
            for symbol in symbols
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks)

        # Verify all succeeded
        assert len(results) == 5
        assert all(result.success for result in results)
        assert all(result.snapshots_count == 3 for result in results)

        # Verify all symbols were processed
        result_symbols = {result.symbol for result in results}
        assert result_symbols == set(symbols)

        # Verify repository calls
        assert mock_asset_repo.save_asset.call_count == 5
        assert mock_asset_repo.save_snapshot.call_count == 15  # 5 symbols * 3 snapshots each