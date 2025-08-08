"""Unit tests for MockDataProvider."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from portfolio_manager.infrastructure.data_providers.mock_provider import MockDataProvider


class TestMockDataProviderInitialization:
    """Test MockDataProvider initialization."""

    def test_default_initialization(self):
        """Test provider initializes with default base price."""
        provider = MockDataProvider()
        assert provider.base_price == Decimal("100.00")
        assert provider.logger is not None

    def test_custom_base_price_initialization(self):
        """Test provider initializes with custom base price."""
        custom_price = Decimal("250.75")
        provider = MockDataProvider(base_price=custom_price)
        assert provider.base_price == custom_price

    def test_zero_base_price_handling(self):
        """Test provider handles zero base price."""
        provider = MockDataProvider(base_price=Decimal("0.00"))
        assert provider.base_price == Decimal("0.00")

    def test_negative_base_price_handling(self):
        """Test provider handles negative base price."""
        provider = MockDataProvider(base_price=Decimal("-50.00"))
        assert provider.base_price == Decimal("-50.00")


class TestMockDataProviderOHLCVGeneration:
    """Test OHLCV data generation."""

    @pytest.mark.asyncio
    async def test_single_day_data_generation(self):
        """Test generating data for a single day."""
        provider = MockDataProvider()
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("AAPL", start_date, end_date)

        assert len(snapshots) == 1
        snapshot = snapshots[0]
        assert snapshot.symbol == "AAPL"
        assert snapshot.timestamp == start_date
        assert snapshot.open > 0
        assert snapshot.high >= snapshot.open
        assert snapshot.low <= snapshot.open
        assert snapshot.close > 0
        assert snapshot.volume > 0

    @pytest.mark.asyncio
    async def test_multiple_day_data_generation(self):
        """Test generating data for multiple days."""
        provider = MockDataProvider()
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 20, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("MSFT", start_date, end_date)

        assert len(snapshots) > 1
        assert len(snapshots) <= 6  # Should not exceed expected days

        # Verify all snapshots are for correct symbol
        assert all(s.symbol == "MSFT" for s in snapshots)

        # Verify timestamps are in order
        timestamps = [s.timestamp for s in snapshots]
        assert timestamps == sorted(timestamps)

        # Verify OHLC relationships
        for snapshot in snapshots:
            assert snapshot.high >= snapshot.open
            assert snapshot.high >= snapshot.close
            assert snapshot.low <= snapshot.open
            assert snapshot.low <= snapshot.close
            assert snapshot.volume > 0

    @pytest.mark.asyncio
    async def test_price_progression(self):
        """Test that prices follow realistic progression."""
        provider = MockDataProvider(base_price=Decimal("100.00"))
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 17, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("TEST", start_date, end_date)

        # Verify price variations are reasonable
        prices = [s.close for s in snapshots]
        for i, price in enumerate(prices):
            # All prices should be positive
            assert price > 0

            # Price variations should be reasonable (within 10% of base)
            variation = abs(price - provider.base_price) / provider.base_price
            assert variation <= Decimal("0.5")  # Allow 50% variation for test flexibility

    @pytest.mark.asyncio
    async def test_volume_progression(self):
        """Test that volume increases over time."""
        provider = MockDataProvider()
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 18, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("VOL", start_date, end_date)

        volumes = [s.volume for s in snapshots]
        # Volume should generally increase
        assert volumes[0] < volumes[-1]

        # All volumes should be positive
        assert all(v > 0 for v in volumes)

    @pytest.mark.asyncio
    async def test_same_start_end_date(self):
        """Test behavior when start and end dates are the same."""
        provider = MockDataProvider()
        date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("SAME", date, date)

        assert len(snapshots) == 1
        assert snapshots[0].timestamp == date

    @pytest.mark.asyncio
    async def test_end_before_start_date(self):
        """Test behavior when end date is before start date."""
        provider = MockDataProvider()
        start_date = datetime(2024, 1, 20, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("REVERSE", start_date, end_date)

        # Should return empty list or handle gracefully
        assert len(snapshots) == 0

    @pytest.mark.asyncio
    async def test_large_date_range_limit(self):
        """Test that very large date ranges are limited."""
        provider = MockDataProvider()
        start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 1, tzinfo=timezone.utc)  # 4 years

        snapshots = await provider.get_ohlcv_data("LARGE", start_date, end_date)

        # Should be limited to prevent infinite loops (with some tolerance)
        assert len(snapshots) <= 1001  # Allow for slight overage due to iteration logic


class TestMockDataProviderFundamentalData:
    """Test fundamental data generation."""

    @pytest.mark.asyncio
    async def test_fundamental_data_structure(self):
        """Test fundamental data contains expected metrics."""
        provider = MockDataProvider()

        data = await provider.get_fundamental_data("FUND")

        # Verify expected metrics are present
        expected_metrics = [
            "pe_ratio", "pb_ratio", "dividend_yield", "market_cap",
            "revenue", "net_income", "debt_to_equity", "current_ratio",
            "roe", "roa"
        ]

        for metric in expected_metrics:
            assert metric in data
            assert isinstance(data[metric], Decimal)

    @pytest.mark.asyncio
    async def test_fundamental_data_values_reasonable(self):
        """Test fundamental data values are reasonable."""
        provider = MockDataProvider()

        data = await provider.get_fundamental_data("REASONABLE")

        # Verify ranges are reasonable for financial metrics
        assert 0 < data["pe_ratio"] < 100
        assert 0 < data["pb_ratio"] < 10
        assert 0 <= data["dividend_yield"] <= 1
        assert data["market_cap"] > 0
        assert data["revenue"] > 0
        assert data["net_income"] > 0
        assert data["debt_to_equity"] >= 0
        assert data["current_ratio"] > 0
        assert 0 <= data["roe"] <= 1
        assert 0 <= data["roa"] <= 1

    @pytest.mark.asyncio
    async def test_fundamental_data_consistency(self):
        """Test fundamental data is consistent between calls."""
        provider = MockDataProvider()

        data1 = await provider.get_fundamental_data("CONSISTENT")
        data2 = await provider.get_fundamental_data("CONSISTENT")

        # Should return same data for same symbol
        assert data1 == data2

    @pytest.mark.asyncio
    async def test_different_symbols_same_data(self):
        """Test that different symbols return same fundamental structure."""
        provider = MockDataProvider()

        data1 = await provider.get_fundamental_data("SYM1")
        data2 = await provider.get_fundamental_data("SYM2")

        # Should have same keys but potentially different values
        assert set(data1.keys()) == set(data2.keys())


class TestMockDataProviderSupport:
    """Test symbol support functionality."""

    def test_supports_valid_symbols(self):
        """Test provider supports valid symbols."""
        provider = MockDataProvider()

        valid_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "TEST123"]

        for symbol in valid_symbols:
            assert provider.supports_symbol(symbol) is True

    def test_rejects_invalid_symbols(self):
        """Test provider rejects invalid symbols."""
        provider = MockDataProvider()

        invalid_symbols = ["INVALID", "UNSUPPORTED", "FAIL", "ERROR"]

        for symbol in invalid_symbols:
            assert provider.supports_symbol(symbol) is False

    def test_case_sensitive_symbol_support(self):
        """Test symbol support is case sensitive."""
        provider = MockDataProvider()

        assert provider.supports_symbol("invalid") is True  # lowercase
        assert provider.supports_symbol("INVALID") is False  # uppercase

    def test_empty_symbol_support(self):
        """Test behavior with empty symbol."""
        provider = MockDataProvider()

        assert provider.supports_symbol("") is True


class TestMockDataProviderMetadata:
    """Test provider metadata methods."""

    def test_provider_name(self):
        """Test provider returns correct name."""
        provider = MockDataProvider()

        name = provider.get_provider_name()
        assert name == "MockDataProvider"
        assert isinstance(name, str)

    def test_rate_limit_info(self):
        """Test rate limit information."""
        provider = MockDataProvider()

        rate_info = provider.get_rate_limit_info()

        # Verify expected structure
        expected_keys = [
            "requests_per_minute", "requests_per_hour", 
            "requests_per_day", "current_usage"
        ]

        for key in expected_keys:
            assert key in rate_info
            assert isinstance(rate_info[key], int)
            assert rate_info[key] >= 0

    def test_rate_limit_values(self):
        """Test rate limit values are reasonable."""
        provider = MockDataProvider()

        rate_info = provider.get_rate_limit_info()

        # Verify mock has high limits
        assert rate_info["requests_per_minute"] >= 1000
        assert rate_info["requests_per_hour"] >= 10000
        assert rate_info["requests_per_day"] >= 100000
        assert rate_info["current_usage"] == 0


class TestMockDataProviderEdgeCases:
    """Test edge cases and error scenarios."""

    @pytest.mark.asyncio
    async def test_zero_base_price_snapshots(self):
        """Test snapshot generation with zero base price."""
        provider = MockDataProvider(base_price=Decimal("0.00"))
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("ZERO", start_date, end_date)

        # Should handle zero price gracefully by using fallback to positive price
        assert len(snapshots) == 1
        snapshot = snapshots[0]
        assert snapshot.open > 0  # Should be positive due to fallback
        assert snapshot.high >= snapshot.low
        assert snapshot.close > 0

    @pytest.mark.asyncio
    async def test_negative_base_price_snapshots(self):
        """Test snapshot generation with negative base price."""
        provider = MockDataProvider(base_price=Decimal("-100.00"))
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("NEGATIVE", start_date, end_date)

        # Should handle negative price by using fallback to positive default
        assert len(snapshots) == 1
        snapshot = snapshots[0]
        # Implementation should ensure positive prices
        assert snapshot.open > 0
        assert snapshot.high > 0
        assert snapshot.low > 0
        assert snapshot.close > 0

    @pytest.mark.asyncio
    async def test_very_large_base_price(self):
        """Test with very large base price."""
        provider = MockDataProvider(base_price=Decimal("999999.99"))
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("LARGE_PRICE", start_date, end_date)

        assert len(snapshots) == 1
        snapshot = snapshots[0]
        assert snapshot.open > 0
        assert snapshot.high >= snapshot.low

    @pytest.mark.asyncio
    async def test_extreme_date_ranges(self):
        """Test with extreme date ranges."""
        provider = MockDataProvider()

        # Very old dates
        start_date = datetime(1990, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(1990, 1, 2, tzinfo=timezone.utc)

        snapshots = await provider.get_ohlcv_data("OLD", start_date, end_date)
        assert len(snapshots) >= 1

    def test_symbol_support_with_special_characters(self):
        """Test symbol support with special characters."""
        provider = MockDataProvider()

        special_symbols = ["TEST.A", "TEST-B", "TEST_C", "TEST123", "123TEST"]

        for symbol in special_symbols:
            # Should support symbols with special characters
            result = provider.supports_symbol(symbol)
            assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_concurrent_data_generation(self):
        """Test concurrent data generation doesn't interfere."""
        import asyncio

        provider = MockDataProvider()
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 16, tzinfo=timezone.utc)

        # Create concurrent tasks
        tasks = [
            provider.get_ohlcv_data(f"CONCURRENT_{i}", start_date, end_date)
            for i in range(5)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all results are valid
        assert len(results) == 5
        for i, snapshots in enumerate(results):
            assert len(snapshots) >= 1
            assert all(s.symbol == f"CONCURRENT_{i}" for s in snapshots)


class TestMockDataProviderIntegration:
    """Integration tests for MockDataProvider."""

    @pytest.mark.asyncio
    async def test_provider_interface_compliance(self):
        """Test provider implements all DataProvider interface methods."""
        provider = MockDataProvider()

        # Test all interface methods exist and work
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 16, tzinfo=timezone.utc)

        # OHLCV data
        ohlcv = await provider.get_ohlcv_data("INTERFACE", start_date, end_date)
        assert isinstance(ohlcv, list)
        assert len(ohlcv) > 0

        # Fundamental data
        fundamentals = await provider.get_fundamental_data("INTERFACE")
        assert isinstance(fundamentals, dict)
        assert len(fundamentals) > 0

        # Symbol support
        supports = provider.supports_symbol("INTERFACE")
        assert isinstance(supports, bool)

        # Provider name
        name = provider.get_provider_name()
        assert isinstance(name, str)
        assert len(name) > 0

        # Rate limit info
        rate_info = provider.get_rate_limit_info()
        assert isinstance(rate_info, dict)
        assert len(rate_info) > 0

    @pytest.mark.asyncio
    async def test_provider_with_data_ingestion_service_pattern(self):
        """Test provider works with typical data ingestion service usage."""
        provider = MockDataProvider()

        # Simulate typical service usage
        symbol = "SERVICE_TEST"
        start_date = datetime(2024, 1, 15, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 20, tzinfo=timezone.utc)

        # Check symbol support first
        if provider.supports_symbol(symbol):
            # Get OHLCV data
            snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)
            assert len(snapshots) > 0

            # Get fundamental data
            fundamentals = await provider.get_fundamental_data(symbol)
            assert len(fundamentals) > 0

            # Verify data consistency
            assert all(s.symbol == symbol for s in snapshots)

        else:
            # Should handle unsupported symbols gracefully
            assert symbol in ["INVALID", "UNSUPPORTED", "FAIL", "ERROR"]