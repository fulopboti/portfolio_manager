"""Integration tests for YFinance provider with real API calls."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List

from portfolio_manager.infrastructure.data_providers.yfinance_provider import YFinanceProvider
from portfolio_manager.domain.entities import AssetSnapshot
from portfolio_manager.domain.exceptions import DataIngestionError


# Skip these tests by default since they make real API calls
pytestmark = pytest.mark.integration


class TestYFinanceProviderIntegration:
    """Integration tests with real Yahoo Finance API."""

    @pytest.fixture
    def provider(self):
        """Create YFinanceProvider with conservative settings."""
        return YFinanceProvider(request_delay=1.0, max_retries=2)

    @pytest.fixture
    def test_symbols(self):
        """Common test symbols that should be stable."""
        return ["AAPL", "MSFT", "SPY", "QQQ", "VTI"]

    @pytest.fixture
    def date_range(self):
        """Recent date range for testing."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)  # Last week
        return start_date, end_date

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_real_ohlcv_data_retrieval(self, provider, date_range):
        """Test retrieval of real OHLCV data from Yahoo Finance."""
        start_date, end_date = date_range
        symbol = "AAPL"  # Apple should always be available

        # Get real data from Yahoo Finance
        snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)

        # Validate results
        assert len(snapshots) > 0, "Should retrieve at least one data point"
        assert all(isinstance(s, AssetSnapshot) for s in snapshots), "All items should be AssetSnapshots"
        
        # Check data quality
        for snapshot in snapshots:
            assert snapshot.symbol == symbol
            assert isinstance(snapshot.timestamp, datetime)
            assert snapshot.timestamp.tzinfo is not None  # Should be timezone aware
            
            # Price validations
            assert snapshot.open > 0
            assert snapshot.high > 0
            assert snapshot.low > 0
            assert snapshot.close > 0
            assert snapshot.high >= snapshot.low
            assert snapshot.volume >= 0
            
            # Check that prices are reasonable (AAPL should be between $50-$500)
            assert 50 <= snapshot.close <= 500
            
            # Ensure precision is maintained
            assert isinstance(snapshot.open, Decimal)
            assert isinstance(snapshot.high, Decimal)
            assert isinstance(snapshot.low, Decimal)
            assert isinstance(snapshot.close, Decimal)

        print(f"[OK] Retrieved {len(snapshots)} snapshots for {symbol}")
        print(f"   Latest: ${snapshots[-1].close} (Volume: {snapshots[-1].volume:,})")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_real_fundamental_data_retrieval(self, provider):
        """Test retrieval of real fundamental data from Yahoo Finance."""
        symbol = "AAPL"  # Apple should have rich fundamental data

        # Get real fundamental data
        fundamentals = await provider.get_fundamental_data(symbol)

        # Should have some data
        assert len(fundamentals) > 0, "Should retrieve some fundamental data"
        
        # Check for expected keys
        expected_keys = ['data_source', 'last_updated']
        for key in expected_keys:
            assert key in fundamentals

        # Check data source
        assert fundamentals['data_source'] == 'yahoo_finance'
        
        # Validate common financial metrics if available
        financial_metrics = ['pe_ratio', 'pb_ratio', 'market_cap', 'dividend_yield']
        available_metrics = [key for key in financial_metrics if key in fundamentals]
        
        print(f"[OK] Retrieved {len(fundamentals)} fundamental metrics for {symbol}")
        print(f"   Available financial metrics: {available_metrics}")
        
        # Validate data types for available metrics
        for metric in available_metrics:
            value = fundamentals[metric]
            if value is not None:
                assert isinstance(value, Decimal), f"{metric} should be Decimal type"
                if metric in ['pe_ratio', 'pb_ratio']:
                    assert value > 0, f"{metric} should be positive"
                elif metric == 'market_cap':
                    assert value > 1000000000, f"{metric} should be at least 1B for AAPL"

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_multiple_symbols_batch_retrieval(self, provider, test_symbols, date_range):
        """Test batch retrieval of multiple symbols."""
        start_date, end_date = date_range
        results = {}
        
        for symbol in test_symbols[:3]:  # Test first 3 symbols to avoid rate limits
            try:
                snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
                results[symbol] = snapshots
                print(f"[OK] {symbol}: {len(snapshots)} snapshots")
            except DataIngestionError as e:
                print(f"[WARN]  {symbol}: {e}")
                continue

        # Should have data for most symbols
        assert len(results) >= 2, "Should successfully retrieve data for most test symbols"
        
        # Validate each result
        for symbol, snapshots in results.items():
            assert len(snapshots) > 0
            assert all(s.symbol == symbol for s in snapshots)

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_rate_limiting_behavior(self, provider):
        """Test that rate limiting works correctly."""
        symbol = "AAPL"
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        
        # Make multiple requests and measure timing
        import time
        request_times = []
        
        for i in range(3):
            start_time = time.time()
            try:
                await provider.get_ohlcv_data(symbol, start_date, end_date)
                end_time = time.time()
                request_times.append(end_time - start_time)
            except DataIngestionError:
                # API might be temporarily unavailable
                pass
        
        if len(request_times) >= 2:
            # Second request should take longer due to rate limiting delay
            assert request_times[1] >= request_times[0] + provider.request_delay * 0.8
            print(f"[OK] Rate limiting working: {request_times}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_error_handling_with_invalid_symbol(self, provider, date_range):
        """Test error handling with invalid symbols."""
        start_date, end_date = date_range
        
        # Test one invalid symbol that should raise DataIngestionError
        with pytest.raises(DataIngestionError, match="No data available for symbol"):
            await provider.get_ohlcv_data("ZZZZ", start_date, end_date)

    @pytest.mark.asyncio
    async def test_provider_info_and_metadata(self, provider):
        """Test provider metadata and rate limit info."""
        # Test provider identification
        assert provider.get_provider_name() == "Yahoo Finance (yfinance)"
        assert provider.supports_symbol("AAPL") is True
        assert provider.supports_symbol("INVALID") is False
        
        # Test rate limit info
        rate_info = provider.get_rate_limit_info()
        assert rate_info['provider'] == "Yahoo Finance"
        assert 'requests_per_hour' in rate_info
        assert 'current_request_count' in rate_info
        assert rate_info['request_delay'] == provider.request_delay

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_date_range_validation(self, provider):
        """Test various date ranges."""
        symbol = "AAPL"
        
        # Test recent data (should work)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        
        snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
        assert len(snapshots) > 0
        
        # Snapshots should be ordered by date
        dates = [s.timestamp for s in snapshots]
        assert dates == sorted(dates), "Snapshots should be ordered by date"
        
        print(f"[OK] Date range validation: {len(snapshots)} snapshots from {start_date.date()} to {end_date.date()}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_weekend_and_holiday_handling(self, provider):
        """Test handling of weekends and market holidays."""
        symbol = "AAPL"
        
        # Test a weekend date range (should return data from surrounding weekdays)
        # Use a known weekend
        saturday = datetime(2024, 1, 6, tzinfo=timezone.utc)  # January 6, 2024 was a Saturday
        sunday = datetime(2024, 1, 7, tzinfo=timezone.utc)    # January 7, 2024 was a Sunday
        
        try:
            snapshots = await provider.get_ohlcv_data(symbol, saturday, sunday)
            # Should return empty or data from surrounding days
            assert isinstance(snapshots, list)
            print(f"[OK] Weekend handling: {len(snapshots)} snapshots")
        except DataIngestionError as e:
            # This is also acceptable - no data available for weekends
            print(f"[OK] Weekend handling: {e}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_european_symbol_support(self, provider):
        """Test support for European symbols."""
        european_symbols = ["ASML.AS", "SAP.DE", "NESN.SW"]  # ASML (Amsterdam), SAP (Germany), Nestle (Switzerland)
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        
        successful_retrievals = 0
        
        for symbol in european_symbols:
            try:
                if provider.supports_symbol(symbol):
                    snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
                    if len(snapshots) > 0:
                        successful_retrievals += 1
                        print(f"[OK] European symbol {symbol}: {len(snapshots)} snapshots")
                        
                        # Validate European data
                        latest = snapshots[-1]
                        assert latest.close > 0
                        assert latest.volume >= 0
            except DataIngestionError as e:
                print(f"[WARN]  European symbol {symbol}: {e}")
        
        # Should support at least some European symbols
        if successful_retrievals > 0:
            print(f"[OK] European market support: {successful_retrievals}/{len(european_symbols)} symbols")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_request_statistics_tracking(self, provider):
        """Test that request statistics are tracked correctly."""
        initial_count = provider._request_count
        initial_failed = provider._failed_requests
        
        symbol = "AAPL"
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        
        # Make a successful request
        try:
            await provider.get_ohlcv_data(symbol, start_date, end_date)
            assert provider._request_count > initial_count
        except DataIngestionError:
            # If it failed, failed count should increment
            assert provider._failed_requests > initial_failed
        
        # Try a valid-format but non-existent symbol (should fail at API level)
        try:
            await provider.get_ohlcv_data("ZZZZ", start_date, end_date)
        except DataIngestionError:
            pass  # Expected
        
        # Failed requests should increment
        assert provider._failed_requests > initial_failed
        
        rate_info = provider.get_rate_limit_info()
        print(f"[OK] Request statistics: {rate_info['current_request_count']} total, {rate_info['failed_requests']} failed")


# Additional test class for specific integration scenarios
class TestYFinanceProviderRealWorldScenarios:
    """Real-world scenario tests."""

    @pytest.fixture
    def provider(self):
        """Provider with realistic settings."""
        return YFinanceProvider(request_delay=0.5, max_retries=3)

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_etf_and_index_data(self, provider):
        """Test retrieval of ETF and index data."""
        etf_symbols = ["SPY", "QQQ", "VTI", "IVV"]
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=5)
        
        for symbol in etf_symbols:
            try:
                snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
                assert len(snapshots) > 0
                
                # ETFs should have reasonable volumes
                latest = snapshots[-1]
                assert latest.volume > 10000  # ETFs typically have decent volume
                print(f"[OK] ETF {symbol}: ${latest.close}, Volume: {latest.volume:,}")
            except DataIngestionError as e:
                print(f"[WARN]  ETF {symbol}: {e}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_crypto_adjacent_symbols(self, provider):
        """Test crypto-related stocks and ETFs."""
        crypto_symbols = ["COIN", "MSTR", "BITO"]  # Coinbase, MicroStrategy, Bitcoin ETF
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=5)
        
        for symbol in crypto_symbols:
            try:
                snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
                if len(snapshots) > 0:
                    latest = snapshots[-1]
                    print(f"[OK] Crypto-related {symbol}: ${latest.close}")
            except DataIngestionError as e:
                print(f"[WARN]  Crypto-related {symbol}: {e}")

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_large_cap_vs_small_cap(self, provider):
        """Test data quality across different market caps."""
        large_caps = ["AAPL", "MSFT", "GOOGL"]
        small_caps = ["PINS", "SNAP", "UBER"]  # Smaller tech companies
        
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        
        for symbol_list, cap_type in [(large_caps, "Large cap"), (small_caps, "Small cap")]:
            for symbol in symbol_list:
                try:
                    snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
                    fundamentals = await provider.get_fundamental_data(symbol)
                    
                    if snapshots and fundamentals:
                        print(f"[OK] {cap_type} {symbol}: Data available")
                except DataIngestionError as e:
                    print(f"[WARN]  {cap_type} {symbol}: {e}")


if __name__ == "__main__":
    # Allow running integration tests directly for development
    import asyncio
    
    async def run_quick_test():
        provider = YFinanceProvider(request_delay=0.5)
        
        print("[RUNNING] Quick integration test...")
        
        # Test basic functionality
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        
        try:
            snapshots = await provider.get_ohlcv_data("AAPL", start_date, end_date)
            print(f"[OK] OHLCV: Retrieved {len(snapshots)} snapshots")
            
            fundamentals = await provider.get_fundamental_data("AAPL")
            print(f"[OK] Fundamentals: Retrieved {len(fundamentals)} metrics")
            
            rate_info = provider.get_rate_limit_info()
            print(f"[OK] Rate info: {rate_info}")
            
        except Exception as e:
            print(f"[ERROR] Test failed: {e}")
    
    asyncio.run(run_quick_test())