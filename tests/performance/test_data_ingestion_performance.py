"""Performance tests for data ingestion operations."""

import pytest
import asyncio
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List
from unittest.mock import AsyncMock, Mock

from portfolio_manager.application.services.data_ingestion import DataIngestionService
from portfolio_manager.infrastructure.data_providers import MockDataProvider
from portfolio_manager.domain.entities import AssetSnapshot, AssetType


class HighThroughputProvider(MockDataProvider):
    """High-throughput mock provider for performance testing."""

    def __init__(self, snapshots_per_request: int = 100):
        super().__init__()
        self.snapshots_per_request = snapshots_per_request
        self.request_count = 0
        self.total_snapshots_generated = 0

    async def get_ohlcv_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> List[AssetSnapshot]:
        """Generate large number of snapshots for performance testing."""
        self.request_count += 1

        snapshots = []
        base_price = self.base_price
        base_volume = 1000000

        for i in range(self.snapshots_per_request):
            price_variation = Decimal(str(i * 0.01))  # Small incremental changes
            current_price = base_price + price_variation

            snapshot = AssetSnapshot(
                symbol=symbol,
                timestamp=start_date + timedelta(hours=i),
                open=current_price,
                high=current_price * Decimal("1.005"),
                low=current_price * Decimal("0.995"),
                close=current_price * Decimal("1.002"),
                volume=base_volume + (i * 1000)
            )
            snapshots.append(snapshot)

        self.total_snapshots_generated += len(snapshots)
        return snapshots


@pytest.mark.performance
class TestDataIngestionServicePerformance:
    """Performance tests for DataIngestionService."""

    @pytest.fixture
    def high_performance_mock_repo(self):
        """Create high-performance mock repository."""
        mock_repo = Mock()
        mock_repo.save_asset = AsyncMock()
        mock_repo.save_snapshot = AsyncMock()
        mock_repo.get_asset = AsyncMock(return_value=None)
        mock_repo.get_all_assets = AsyncMock(return_value=[])
        mock_repo.save_fundamental_metrics = AsyncMock()
        return mock_repo

    @pytest.mark.asyncio
    async def test_single_symbol_large_dataset_performance(self, high_performance_mock_repo):
        """Test performance with large dataset for single symbol."""
        # Setup provider with large dataset
        provider = HighThroughputProvider(snapshots_per_request=1000)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo,
            batch_size=100
        )

        # Time the operation
        start_time = time.time()

        result = await service.ingest_symbol(
            symbol="LARGE_DATASET",
            asset_type=AssetType.STOCK,
            exchange="PERF_TEST",
            name="Large Dataset Corp."
        )

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Performance assertions
        assert result.success is True
        assert result.snapshots_count == 1000
        assert elapsed_time < 2.0  # Should complete within 2 seconds

        # Verify repository calls
        assert high_performance_mock_repo.save_asset.call_count == 1
        assert high_performance_mock_repo.save_snapshot.call_count == 1000

    @pytest.mark.asyncio
    async def test_batch_ingestion_throughput(self, high_performance_mock_repo):
        """Test throughput of batch ingestion operations."""
        provider = HighThroughputProvider(snapshots_per_request=50)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo,
            batch_size=10
        )

        # Create batch of symbols
        symbols = [f"BATCH_{i:03d}" for i in range(50)]

        start_time = time.time()

        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="BATCH_TEST"
        )

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Performance assertions
        assert len(results) == 50
        assert all(r.success for r in results)

        total_snapshots = sum(r.snapshots_count for r in results)
        assert total_snapshots == 2500  # 50 symbols * 50 snapshots each

        # Throughput calculations
        snapshots_per_second = total_snapshots / elapsed_time
        assert snapshots_per_second > 500  # At least 500 snapshots per second

        symbols_per_second = len(symbols) / elapsed_time
        assert symbols_per_second > 10  # At least 10 symbols per second

    @pytest.mark.asyncio
    async def test_concurrent_ingestion_scalability(self, high_performance_mock_repo):
        """Test scalability with concurrent ingestion operations."""
        provider = HighThroughputProvider(snapshots_per_request=20)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo,
            batch_size=10
        )

        # Create concurrent tasks
        num_concurrent_tasks = 10
        symbols_per_task = 5

        async def ingest_batch(batch_id: int):
            symbols = [f"CONCURRENT_{batch_id}_{i}" for i in range(symbols_per_task)]
            return await service.ingest_multiple_symbols(
                symbols=symbols,
                asset_type=AssetType.STOCK,
                exchange="CONCURRENT_TEST"
            )

        start_time = time.time()

        # Execute concurrent tasks
        tasks = [ingest_batch(i) for i in range(num_concurrent_tasks)]
        results = await asyncio.gather(*tasks)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Verify results
        assert len(results) == num_concurrent_tasks
        total_symbols_processed = 0
        total_snapshots_processed = 0

        for batch_results in results:
            assert len(batch_results) == symbols_per_task
            assert all(r.success for r in batch_results)
            total_symbols_processed += len(batch_results)
            total_snapshots_processed += sum(r.snapshots_count for r in batch_results)

        # Performance assertions
        assert total_symbols_processed == num_concurrent_tasks * symbols_per_task
        assert total_snapshots_processed == num_concurrent_tasks * symbols_per_task * 20

        # Concurrent throughput should be better than sequential
        concurrent_throughput = total_snapshots_processed / elapsed_time
        assert concurrent_throughput > 200  # Reasonable concurrent throughput

    @pytest.mark.asyncio
    async def test_memory_efficiency_large_batch(self, high_performance_mock_repo):
        """Test memory efficiency with large batch operations."""
        provider = HighThroughputProvider(snapshots_per_request=100)

        # Use smaller batch size to test memory management
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo,
            batch_size=5  # Small batch size for memory efficiency
        )

        # Process large number of symbols
        symbols = [f"MEMORY_{i:04d}" for i in range(100)]

        start_time = time.time()

        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="MEMORY_TEST"
        )

        end_time = time.time()

        # Verify all processed successfully
        assert len(results) == 100
        assert all(r.success for r in results)

        total_snapshots = sum(r.snapshots_count for r in results)
        assert total_snapshots == 10000  # 100 symbols * 100 snapshots each

        # Should still complete in reasonable time despite small batch size
        assert (end_time - start_time) < 5.0

    @pytest.mark.asyncio
    async def test_provider_call_optimization(self, high_performance_mock_repo):
        """Test that provider calls are optimized and not duplicated."""
        provider = HighThroughputProvider(snapshots_per_request=10)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo
        )

        symbols = ["OPT_A", "OPT_B", "OPT_C"]

        # Process symbols
        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="OPT_TEST"
        )

        # Verify efficient provider usage
        assert provider.request_count == 3  # One request per symbol
        assert provider.total_snapshots_generated == 30  # 3 symbols * 10 snapshots each

        # Verify results
        assert len(results) == 3
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_refresh_operation_performance(self, high_performance_mock_repo):
        """Test performance of refresh all assets operation."""
        # Setup existing assets
        from portfolio_manager.domain.entities import Asset
        existing_assets = [
            Asset(symbol=f"REFRESH_{i}", name=f"Refresh Corp {i}", 
                  asset_type=AssetType.STOCK, exchange="REFRESH_TEST")
            for i in range(20)
        ]

        high_performance_mock_repo.get_all_assets = AsyncMock(return_value=existing_assets)

        provider = HighThroughputProvider(snapshots_per_request=25)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=high_performance_mock_repo
        )

        start_time = time.time()

        results = await service.refresh_all_assets()

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Performance assertions
        assert len(results) == 20
        assert all(r.success for r in results)

        total_snapshots = sum(r.snapshots_count for r in results)
        assert total_snapshots == 500  # 20 assets * 25 snapshots each

        # Refresh should be efficient
        assert elapsed_time < 3.0
        assert provider.request_count == 20  # One request per existing asset


@pytest.mark.performance
class TestDataIngestionScalabilityLimits:
    """Tests for understanding scalability limits and bottlenecks."""

    @pytest.fixture
    def scalability_mock_repo(self):
        """Mock repository that simulates database latency."""
        mock_repo = Mock()

        async def slow_save_asset(*args, **kwargs):
            await asyncio.sleep(0.001)  # 1ms latency

        async def slow_save_snapshot(*args, **kwargs):
            await asyncio.sleep(0.0005)  # 0.5ms latency

        async def slow_save_fundamentals(*args, **kwargs):
            await asyncio.sleep(0.001)  # 1ms latency

        mock_repo.save_asset = slow_save_asset
        mock_repo.save_snapshot = slow_save_snapshot
        mock_repo.get_asset = AsyncMock(return_value=None)
        mock_repo.get_all_assets = AsyncMock(return_value=[])
        mock_repo.save_fundamental_metrics = slow_save_fundamentals

        return mock_repo

    @pytest.mark.asyncio
    async def test_latency_impact_on_throughput(self, scalability_mock_repo):
        """Test how database latency affects ingestion throughput."""
        provider = HighThroughputProvider(snapshots_per_request=10)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=scalability_mock_repo,
            batch_size=5
        )

        symbols = [f"LATENCY_{i}" for i in range(10)]

        start_time = time.time()

        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="LATENCY_TEST"
        )

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Verify functionality still works with latency
        assert len(results) == 10
        assert all(r.success for r in results)

        total_snapshots = sum(r.snapshots_count for r in results)
        assert total_snapshots == 100

        # Time should reflect latency impact but still be reasonable
        # 10 assets * (1ms + 10*0.5ms + 1ms) = 10 * 6ms = 60ms minimum
        assert elapsed_time > 0.06  # At least 60ms due to latency
        assert elapsed_time < 2.0   # But still reasonable overall

    @pytest.mark.asyncio
    async def test_maximum_concurrent_operations(self, scalability_mock_repo):
        """Test behavior with maximum concurrent operations."""
        provider = HighThroughputProvider(snapshots_per_request=5)
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=scalability_mock_repo,
            batch_size=3
        )

        # Create many concurrent operations
        num_concurrent = 20
        symbols_per_operation = 3

        async def concurrent_operation(op_id: int):
            symbols = [f"MAXCON_{op_id}_{i}" for i in range(symbols_per_operation)]
            return await service.ingest_multiple_symbols(
                symbols=symbols,
                asset_type=AssetType.STOCK,
                exchange="MAXCON_TEST"
            )

        start_time = time.time()

        tasks = [concurrent_operation(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        end_time = time.time()

        # Verify no exceptions occurred
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Found exceptions: {exceptions}"

        # Verify all operations completed
        successful_results = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_results) == num_concurrent

        total_processed = sum(len(batch) for batch in successful_results)
        assert total_processed == num_concurrent * symbols_per_operation

    @pytest.mark.asyncio
    async def test_error_recovery_performance(self):
        """Test performance impact of error recovery mechanisms."""

        class FlakyProvider(HighThroughputProvider):
            def __init__(self):
                super().__init__(snapshots_per_request=10)
                self.call_count = 0

            async def get_ohlcv_data(self, symbol, start_date, end_date):
                self.call_count += 1
                # Fail every 5th call
                if self.call_count % 5 == 0:
                    raise Exception(f"Simulated failure for call {self.call_count}")
                return await super().get_ohlcv_data(symbol, start_date, end_date)

        mock_repo = Mock()
        mock_repo.save_asset = AsyncMock()
        mock_repo.save_snapshot = AsyncMock()
        mock_repo.get_asset = AsyncMock(return_value=None)
        mock_repo.save_fundamental_metrics = AsyncMock()

        provider = FlakyProvider()
        service = DataIngestionService(
            data_provider=provider,
            asset_repository=mock_repo,
            retry_attempts=3
        )

        # Process symbols that will trigger some failures
        symbols = [f"FLAKY_{i}" for i in range(20)]

        start_time = time.time()

        results = await service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="FLAKY_TEST"
        )

        end_time = time.time()

        # Analyze results
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        # Should have mix of success and failures
        assert len(successful) > 0
        assert len(failed) > 0
        assert len(results) == 20

        # Performance should still be reasonable despite errors
        assert (end_time - start_time) < 5.0

    @pytest.mark.asyncio
    async def test_data_volume_scaling(self):
        """Test how performance scales with data volume."""
        mock_repo = Mock()
        mock_repo.save_asset = AsyncMock()
        mock_repo.save_snapshot = AsyncMock()  
        mock_repo.get_asset = AsyncMock(return_value=None)
        mock_repo.save_fundamental_metrics = AsyncMock()

        # Test different data volumes
        test_cases = [
            (5, 10),    # 5 symbols, 10 snapshots each = 50 total
            (10, 20),   # 10 symbols, 20 snapshots each = 200 total  
            (20, 25),   # 20 symbols, 25 snapshots each = 500 total
        ]

        performance_data = []

        for num_symbols, snapshots_per_symbol in test_cases:
            provider = HighThroughputProvider(snapshots_per_request=snapshots_per_symbol)
            service = DataIngestionService(
                data_provider=provider,
                asset_repository=mock_repo
            )

            symbols = [f"SCALE_{num_symbols}_{i}" for i in range(num_symbols)]

            start_time = time.time()

            results = await service.ingest_multiple_symbols(
                symbols=symbols,
                asset_type=AssetType.STOCK,
                exchange="SCALE_TEST"
            )

            end_time = time.time()
            elapsed_time = end_time - start_time

            total_snapshots = sum(r.snapshots_count for r in results)
            throughput = total_snapshots / elapsed_time

            performance_data.append({
                'symbols': num_symbols,
                'snapshots_per_symbol': snapshots_per_symbol,
                'total_snapshots': total_snapshots,
                'elapsed_time': elapsed_time,
                'throughput': throughput
            })

            # Verify results
            assert len(results) == num_symbols
            assert all(r.success for r in results)
            assert total_snapshots == num_symbols * snapshots_per_symbol

        # Analyze scaling characteristics
        # Performance should be reasonable across all test cases
        throughputs = [p['throughput'] for p in performance_data]

        # All test cases should achieve reasonable throughput
        # Minimum 1000 snapshots per second for any test case
        min_throughput = min(throughputs)
        assert min_throughput > 1000, f"Throughput too low: {throughputs}"

        # Verify that all test cases completed successfully
        for data in performance_data:
            assert data['elapsed_time'] < 10.0, f"Test case took too long: {data}"
            assert data['total_snapshots'] > 0, f"No snapshots processed: {data}"

        # Log performance data for analysis
        print(f"Performance data: {performance_data}")
        print(f"Throughputs: {throughputs}")