"""Tests for YFinance data provider."""

import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pandas as pd

from portfolio_manager.infrastructure.data_providers.yfinance_provider import YFinanceProvider
from portfolio_manager.domain.entities import AssetSnapshot
from portfolio_manager.domain.exceptions import DataIngestionError


class TestYFinanceProvider:
    """Test cases for YFinanceProvider."""

    @pytest.fixture
    def provider(self):
        """Create YFinanceProvider instance for testing."""
        return YFinanceProvider(request_delay=0.01, max_retries=2)  # Fast for testing

    @pytest.fixture
    def mock_ticker_data(self):
        """Create mock pandas DataFrame with ticker data."""
        dates = pd.date_range('2024-01-01', '2024-01-05', freq='D')
        data = {
            'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
            'High': [102.0, 103.0, 104.0, 105.0, 106.0],
            'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
            'Close': [101.0, 102.0, 103.0, 104.0, 105.0],
            'Volume': [1000000, 1100000, 1200000, 1300000, 1400000]
        }
        return pd.DataFrame(data, index=dates)

    @pytest.fixture
    def mock_fundamental_data(self):
        """Create mock fundamental data."""
        return {
            'trailingPE': 15.5,
            'priceToBook': 2.3,
            'dividendYield': 0.025,
            'marketCap': 50000000000,
            'totalRevenue': 10000000000,
            'netIncomeToCommon': 1500000000,
            'debtToEquity': 0.4,
            'currentRatio': 1.8,
            'returnOnEquity': 0.18,
            'returnOnAssets': 0.12,
            'revenueGrowth': 0.05,
            'earningsGrowth': 0.08
        }

    def test_provider_initialization(self, provider):
        """Test provider initialization."""
        assert provider.request_delay == 0.01
        assert provider.max_retries == 2
        assert provider._request_count == 0
        assert provider._failed_requests == 0

    def test_get_provider_name(self, provider):
        """Test provider name."""
        assert provider.get_provider_name() == "Yahoo Finance (yfinance)"

    def test_get_rate_limit_info(self, provider):
        """Test rate limit information."""
        info = provider.get_rate_limit_info()
        assert info['provider'] == "Yahoo Finance"
        assert info['requests_per_hour'] == 2000
        assert info['current_request_count'] == 0
        assert info['failed_requests'] == 0

    def test_supports_symbol_valid(self, provider):
        """Test symbol validation for valid symbols."""
        valid_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'SPY', 'QQQ']
        for symbol in valid_symbols:
            assert provider.supports_symbol(symbol) is True

    def test_supports_symbol_invalid(self, provider):
        """Test symbol validation for invalid symbols."""
        invalid_symbols = ['', 'INVALID', 'TEST', 'MOCK', 'FAIL', 'ERROR', None]
        for symbol in invalid_symbols:
            assert provider.supports_symbol(symbol) is False

    def test_supports_symbol_edge_cases(self, provider):
        """Test symbol validation edge cases."""
        # Too long symbol
        assert provider.supports_symbol('VERYLONGSYMBOL') is False
        
        # Non-string input
        assert provider.supports_symbol(123) is False
        assert provider.supports_symbol([]) is False

    @pytest.mark.asyncio
    async def test_get_ohlcv_data_success(self, provider, mock_ticker_data):
        """Test successful OHLCV data retrieval."""
        symbol = "AAPL"
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        with patch.object(provider, '_fetch_ticker_data', return_value=mock_ticker_data):
            snapshots = await provider.get_ohlcv_data(symbol, start_date, end_date)

        assert len(snapshots) == 5
        assert all(isinstance(snapshot, AssetSnapshot) for snapshot in snapshots)
        
        # Check first snapshot
        first_snapshot = snapshots[0]
        assert first_snapshot.symbol == symbol
        assert first_snapshot.open == Decimal('100.0')
        assert first_snapshot.high == Decimal('102.0')
        assert first_snapshot.low == Decimal('99.0')
        assert first_snapshot.close == Decimal('101.0')
        assert first_snapshot.volume == 1000000

    @pytest.mark.asyncio
    async def test_get_ohlcv_data_unsupported_symbol(self, provider):
        """Test OHLCV data retrieval with unsupported symbol."""
        symbol = "INVALID"
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        with pytest.raises(DataIngestionError, match="not supported"):
            await provider.get_ohlcv_data(symbol, start_date, end_date)

    @pytest.mark.asyncio
    async def test_get_ohlcv_data_empty_response(self, provider):
        """Test OHLCV data retrieval with empty response."""
        symbol = "AAPL"
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        with patch.object(provider, '_fetch_ticker_data', return_value=pd.DataFrame()):
            with pytest.raises(DataIngestionError, match="No data available"):
                await provider.get_ohlcv_data(symbol, start_date, end_date)

    @pytest.mark.asyncio
    async def test_get_ohlcv_data_api_error(self, provider):
        """Test OHLCV data retrieval with API error."""
        symbol = "AAPL"
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        with patch.object(provider, '_fetch_ticker_data', side_effect=Exception("API Error")):
            with pytest.raises(DataIngestionError, match="Failed to fetch data"):
                await provider.get_ohlcv_data(symbol, start_date, end_date)

    @pytest.mark.asyncio
    async def test_get_fundamental_data_success(self, provider, mock_fundamental_data):
        """Test successful fundamental data retrieval."""
        symbol = "AAPL"

        # Mock the _fetch_fundamental_data to return the processed data
        processed_data = {
            'pe_ratio': Decimal('15.5'),
            'pb_ratio': Decimal('2.3'),
            'dividend_yield': Decimal('0.025'),
            'market_cap': Decimal('50000000000'),
            'data_source': 'yahoo_finance',
            'last_updated': '2024-01-01T00:00:00+00:00'
        }

        with patch.object(provider, '_fetch_fundamental_data', return_value=processed_data):
            result = await provider.get_fundamental_data(symbol)

        assert result['pe_ratio'] == Decimal('15.5')
        assert result['pb_ratio'] == Decimal('2.3')
        assert result['dividend_yield'] == Decimal('0.025')
        assert result['market_cap'] == Decimal('50000000000')
        assert result['data_source'] == 'yahoo_finance'
        assert 'last_updated' in result

    @pytest.mark.asyncio
    async def test_get_fundamental_data_empty_response(self, provider):
        """Test fundamental data retrieval with empty response."""
        symbol = "AAPL"

        with patch.object(provider, '_fetch_fundamental_data', return_value={}):
            result = await provider.get_fundamental_data(symbol)

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_fundamental_data_unsupported_symbol(self, provider):
        """Test fundamental data retrieval with unsupported symbol."""
        symbol = "INVALID"

        with pytest.raises(DataIngestionError, match="not supported"):
            await provider.get_fundamental_data(symbol)

    @pytest.mark.asyncio
    async def test_request_delay(self, provider):
        """Test that request delay is applied."""
        symbol = "AAPL"
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)

        # First request should not have delay
        with patch.object(provider, '_fetch_ticker_data', return_value=pd.DataFrame()):
            start_time = asyncio.get_event_loop().time()
            try:
                await provider.get_ohlcv_data(symbol, start_date, end_date)
            except DataIngestionError:
                pass  # Expected due to empty DataFrame
            first_request_time = asyncio.get_event_loop().time() - start_time

        # Second request should have delay
        with patch.object(provider, '_fetch_ticker_data', return_value=pd.DataFrame()):
            start_time = asyncio.get_event_loop().time()
            try:
                await provider.get_ohlcv_data(symbol, start_date, end_date)
            except DataIngestionError:
                pass  # Expected due to empty DataFrame
            second_request_time = asyncio.get_event_loop().time() - start_time

        # Second request should take longer due to delay
        assert second_request_time >= provider.request_delay

    def test_safe_decimal_valid_values(self, provider):
        """Test _safe_decimal with valid values."""
        assert provider._safe_decimal(100.0) == Decimal('100.0')
        assert provider._safe_decimal(100.50) == Decimal('100.5')
        assert provider._safe_decimal('100.0') == Decimal('100.0')
        assert provider._safe_decimal(0) == Decimal('0')

    def test_safe_decimal_invalid_values(self, provider):
        """Test _safe_decimal with invalid values."""
        assert provider._safe_decimal(None) is None
        assert provider._safe_decimal(pd.NA) is None
        assert provider._safe_decimal('invalid') is None
        assert provider._safe_decimal([]) is None

    def test_create_asset_snapshot_valid_data(self, provider):
        """Test _create_asset_snapshot with valid data."""
        symbol = "AAPL"
        date = pd.Timestamp('2024-01-01')
        row = pd.Series({
            'Open': 100.0,
            'High': 102.0,
            'Low': 99.0,
            'Close': 101.0,
            'Volume': 1000000
        })

        snapshot = provider._create_asset_snapshot(symbol, date, row)

        assert snapshot is not None
        assert snapshot.symbol == symbol
        assert snapshot.open == Decimal('100.0')
        assert snapshot.high == Decimal('102.0')
        assert snapshot.low == Decimal('99.0')
        assert snapshot.close == Decimal('101.0')
        assert snapshot.volume == 1000000

    def test_create_asset_snapshot_invalid_data(self, provider):
        """Test _create_asset_snapshot with invalid data."""
        symbol = "AAPL"
        date = pd.Timestamp('2024-01-01')
        
        # Missing required fields
        row = pd.Series({'Volume': 1000000})
        snapshot = provider._create_asset_snapshot(symbol, date, row)
        assert snapshot is None

        # Negative prices
        row = pd.Series({
            'Open': -100.0,
            'High': 102.0,
            'Low': 99.0,
            'Close': 101.0,
            'Volume': 1000000
        })
        snapshot = provider._create_asset_snapshot(symbol, date, row)
        assert snapshot is None

    def test_create_asset_snapshot_negative_volume(self, provider):
        """Test _create_asset_snapshot with negative volume."""
        symbol = "AAPL"
        date = pd.Timestamp('2024-01-01')
        row = pd.Series({
            'Open': 100.0,
            'High': 102.0,
            'Low': 99.0,
            'Close': 101.0,
            'Volume': -1000000  # Negative volume
        })

        snapshot = provider._create_asset_snapshot(symbol, date, row)

        # Should create snapshot with volume set to 0
        assert snapshot is not None
        assert snapshot.volume == 0

    @patch('yfinance.Ticker')
    def test_fetch_ticker_data_success(self, mock_ticker_class, provider, mock_ticker_data):
        """Test _fetch_ticker_data with successful response."""
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_ticker_data
        mock_ticker_class.return_value = mock_ticker

        result = provider._fetch_ticker_data("AAPL", datetime(2024, 1, 1), datetime(2024, 1, 5))

        assert result is not None
        assert len(result) == 5
        mock_ticker.history.assert_called_once()

    @patch('yfinance.Ticker')
    def test_fetch_ticker_data_empty_response(self, mock_ticker_class, provider):
        """Test _fetch_ticker_data with empty response."""
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_class.return_value = mock_ticker

        result = provider._fetch_ticker_data("INVALID", datetime(2024, 1, 1), datetime(2024, 1, 5))

        assert result is None

    @patch('yfinance.Ticker')
    def test_fetch_fundamental_data_success(self, mock_ticker_class, provider, mock_fundamental_data):
        """Test _fetch_fundamental_data with successful response."""
        mock_ticker = MagicMock()
        mock_ticker.info = mock_fundamental_data
        mock_ticker_class.return_value = mock_ticker

        result = provider._fetch_fundamental_data("AAPL")

        assert result is not None
        assert 'pe_ratio' in result
        assert 'data_source' in result
        assert result['data_source'] == 'yahoo_finance'

    @patch('yfinance.Ticker')
    def test_fetch_fundamental_data_empty_response(self, mock_ticker_class, provider):
        """Test _fetch_fundamental_data with empty response."""
        mock_ticker = MagicMock()
        mock_ticker.info = {}
        mock_ticker_class.return_value = mock_ticker

        result = provider._fetch_fundamental_data("AAPL")

        assert result == {}

    @pytest.mark.asyncio
    async def test_request_statistics_tracking(self, provider):
        """Test that request statistics are properly tracked."""
        # Reset provider statistics for clean test
        provider._request_count = 0
        provider._failed_requests = 0

        # Successful request (though it will fail due to minimal data)
        with patch.object(provider, '_fetch_ticker_data', return_value=pd.DataFrame({'Open': [100]})):
            try:
                await provider.get_ohlcv_data("AAPL", datetime(2024, 1, 1), datetime(2024, 1, 2))
            except DataIngestionError:
                pass  # Expected due to minimal DataFrame

        assert provider._request_count == 1

        # Failed request - exception happens before request count increment
        with patch.object(provider, '_fetch_ticker_data', side_effect=Exception("API Error")):
            try:
                await provider.get_ohlcv_data("AAPL", datetime(2024, 1, 1), datetime(2024, 1, 2))
            except DataIngestionError:
                pass  # Expected

        # Request count doesn't increment on exception before the API call
        assert provider._request_count == 1  
        # Failed count includes both the minimal data case and the API error case
        assert provider._failed_requests == 2