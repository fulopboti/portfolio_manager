"""Lightweight integration tests for YFinance provider."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch, Mock
import pandas as pd

from portfolio_manager.infrastructure.data_providers.yfinance_provider import YFinanceProvider
from portfolio_manager.domain.exceptions import DataIngestionError


class TestYFinanceProviderLightIntegration:
    """Lightweight integration tests that can run in CI/CD."""

    @pytest.fixture
    def provider(self):
        """Create YFinanceProvider for testing."""
        return YFinanceProvider(request_delay=0.1, max_retries=2)

    @pytest.fixture
    def sample_yahoo_data(self):
        """Sample data that mimics Yahoo Finance response."""
        dates = pd.date_range('2024-01-01', '2024-01-05', freq='D')
        return pd.DataFrame({
            'Open': [150.0, 151.0, 152.0, 153.0, 154.0],
            'High': [152.0, 153.0, 154.0, 155.0, 156.0],
            'Low': [149.0, 150.0, 151.0, 152.0, 153.0],
            'Close': [151.0, 152.0, 153.0, 154.0, 155.0],
            'Volume': [50000000, 45000000, 55000000, 40000000, 60000000]
        }, index=dates)

    @pytest.fixture
    def sample_yahoo_info(self):
        """Sample info data that mimics Yahoo Finance ticker.info."""
        return {
            'trailingPE': 25.5,
            'priceToBook': 3.2,
            'dividendYield': 0.015,
            'marketCap': 2500000000000,
            'totalRevenue': 400000000000,
            'netIncomeToCommon': 100000000000,
            'debtToEquity': 0.3,
            'currentRatio': 1.5,
            'returnOnEquity': 0.20,
            'returnOnAssets': 0.15,
            'revenueGrowth': 0.08,
            'earningsGrowth': 0.12
        }

    def test_provider_initialization_and_metadata(self, provider):
        """Test provider basic setup and metadata."""
        assert provider.get_provider_name() == "Yahoo Finance (yfinance)"
        assert provider.request_delay == 0.1
        assert provider.max_retries == 2
        
        # Test symbol validation
        assert provider.supports_symbol("AAPL") is True
        assert provider.supports_symbol("MSFT") is True
        assert provider.supports_symbol("INVALID") is False
        assert provider.supports_symbol("") is False
        assert provider.supports_symbol(None) is False
        
        # Test rate limit info structure
        rate_info = provider.get_rate_limit_info()
        expected_keys = [
            'provider', 'requests_per_minute', 'requests_per_hour', 
            'requests_per_day', 'current_request_count', 'failed_requests',
            'request_delay', 'max_retries', 'notes'
        ]
        for key in expected_keys:
            assert key in rate_info

    @pytest.mark.asyncio
    @patch('yfinance.Ticker')
    async def test_ohlcv_data_processing_integration(self, mock_ticker_class, provider, sample_yahoo_data):
        """Test integration with yfinance for OHLCV data processing."""
        # Setup mock ticker
        mock_ticker = Mock()
        mock_ticker.history.return_value = sample_yahoo_data
        mock_ticker_class.return_value = mock_ticker
        
        # Test data retrieval
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)
        
        snapshots = await provider.get_ohlcv_data("AAPL", start_date, end_date)
        
        # Verify yfinance integration
        mock_ticker_class.assert_called_once_with("AAPL")
        mock_ticker.history.assert_called_once()
        
        # Verify data processing
        assert len(snapshots) == 5
        
        # Check first snapshot
        first_snapshot = snapshots[0]
        assert first_snapshot.symbol == "AAPL"
        assert first_snapshot.open == Decimal('150.0')
        assert first_snapshot.high == Decimal('152.0')
        assert first_snapshot.low == Decimal('149.0')
        assert first_snapshot.close == Decimal('151.0')
        assert first_snapshot.volume == 50000000
        
        # Verify timezone handling
        assert first_snapshot.timestamp.tzinfo is not None

    @pytest.mark.asyncio
    @patch('yfinance.Ticker')
    async def test_fundamental_data_processing_integration(self, mock_ticker_class, provider, sample_yahoo_info):
        """Test integration with yfinance for fundamental data processing."""
        # Setup mock ticker
        mock_ticker = Mock()
        mock_ticker.info = sample_yahoo_info
        mock_ticker_class.return_value = mock_ticker
        
        # Test fundamental data retrieval
        fundamentals = await provider.get_fundamental_data("AAPL")
        
        # Verify yfinance integration
        mock_ticker_class.assert_called_once_with("AAPL")
        
        # Verify data processing and conversion
        assert 'pe_ratio' in fundamentals
        assert isinstance(fundamentals['pe_ratio'], Decimal)
        assert fundamentals['pe_ratio'] == Decimal('25.5')
        
        assert 'market_cap' in fundamentals
        assert fundamentals['market_cap'] == Decimal('2500000000000')
        
        # Check metadata
        assert fundamentals['data_source'] == 'yahoo_finance'
        assert 'last_updated' in fundamentals

    @pytest.mark.asyncio
    @patch('yfinance.Ticker')
    async def test_error_handling_integration(self, mock_ticker_class, provider):
        """Test error handling integration with yfinance."""
        # Test API error simulation
        mock_ticker = Mock()
        mock_ticker.history.side_effect = Exception("Network error")
        mock_ticker_class.return_value = mock_ticker
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)
        
        with pytest.raises(DataIngestionError, match="Failed to fetch data"):
            await provider.get_ohlcv_data("AAPL", start_date, end_date)
        
        # Verify error statistics tracking
        assert provider._failed_requests > 0

    @pytest.mark.asyncio
    @patch('yfinance.Ticker')
    async def test_empty_data_handling_integration(self, mock_ticker_class, provider):
        """Test handling of empty responses from yfinance."""
        # Test empty DataFrame response
        mock_ticker = Mock()
        mock_ticker.history.return_value = pd.DataFrame()  # Empty DataFrame
        mock_ticker_class.return_value = mock_ticker
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 5, tzinfo=timezone.utc)
        
        with pytest.raises(DataIngestionError, match="No data available"):
            await provider.get_ohlcv_data("AAPL", start_date, end_date)

    @pytest.mark.asyncio
    async def test_request_delay_integration(self, provider):
        """Test that request delays are applied in real scenarios."""
        import time
        import asyncio
        
        # Mock to avoid real API calls but test delay mechanism
        with patch.object(provider, '_fetch_ticker_data') as mock_fetch:
            mock_fetch.return_value = pd.DataFrame({
                'Open': [100], 'High': [102], 'Low': [99], 
                'Close': [101], 'Volume': [1000000]
            })
            
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
            
            # First request should be fast
            start_time = time.time()
            await provider.get_ohlcv_data("AAPL", start_date, end_date)
            first_duration = time.time() - start_time
            
            # Second request should have delay
            start_time = time.time()
            await provider.get_ohlcv_data("MSFT", start_date, end_date)
            second_duration = time.time() - start_time
            
            # Second request should be slower due to delay
            assert second_duration >= first_duration + provider.request_delay * 0.8

    def test_symbol_validation_edge_cases(self, provider):
        """Test symbol validation with various edge cases."""
        # Valid symbols
        valid_symbols = ["AAPL", "MSFT", "GOOGL", "BRK.A", "SPY"]
        for symbol in valid_symbols:
            assert provider.supports_symbol(symbol) is True
        
        # Invalid symbols
        invalid_symbols = [
            "", "INVALID", "TEST", "MOCK", "FAIL", "ERROR", 
            None, 123, [], {}, "VERYLONGSYMBOLNAME"
        ]
        for symbol in invalid_symbols:
            assert provider.supports_symbol(symbol) is False

    @pytest.mark.asyncio
    async def test_configuration_integration(self, provider):
        """Test that provider respects configuration settings."""
        # Test delay configuration
        assert provider.request_delay == 0.1
        assert provider.max_retries == 2
        
        # Test rate limit info reflects configuration
        rate_info = provider.get_rate_limit_info()
        assert rate_info['request_delay'] == 0.1
        assert rate_info['max_retries'] == 2
        
        # Test request statistics initialization
        assert provider._request_count >= 0
        assert provider._failed_requests >= 0

    @pytest.mark.asyncio
    @patch('yfinance.Ticker')
    async def test_data_type_conversions(self, mock_ticker_class, provider):
        """Test proper data type conversions from yfinance."""
        # Test with various numeric types from pandas
        test_data = pd.DataFrame({
            'Open': [150.50, 151.75],
            'High': [152.25, 153.00],
            'Low': [149.75, 150.50],
            'Close': [151.25, 152.50],
            'Volume': [50000000, 45000000]
        }, index=pd.date_range('2024-01-01', '2024-01-02'))
        
        mock_ticker = Mock()
        mock_ticker.history.return_value = test_data
        mock_ticker_class.return_value = mock_ticker
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
        
        snapshots = await provider.get_ohlcv_data("AAPL", start_date, end_date)
        
        # Verify proper decimal conversion
        assert snapshots[0].open == Decimal('150.50')
        assert snapshots[0].high == Decimal('152.25')
        assert snapshots[1].close == Decimal('152.50')
        
        # Verify integer volume
        assert snapshots[0].volume == 50000000
        assert isinstance(snapshots[0].volume, int)


# Test that can be run to validate real integration
async def validate_real_integration():
    """Quick validation that can be run manually."""
    provider = YFinanceProvider(request_delay=1.0)
    
    print("[RUNNING] Validating yfinance integration...")
    
    try:
        # Test basic functionality
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=2)
        
        snapshots = await provider.get_ohlcv_data("AAPL", start_date, end_date)
        print(f"[OK] Retrieved {len(snapshots)} OHLCV snapshots")
        
        if snapshots:
            latest = snapshots[-1]
            print(f"   Latest AAPL: ${latest.close} (Volume: {latest.volume:,})")
        
        # Test rate limit info
        rate_info = provider.get_rate_limit_info()
        print(f"[OK] Rate info: {rate_info['current_request_count']} requests made")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Integration validation failed: {e}")
        return False


if __name__ == "__main__":
    import asyncio
    asyncio.run(validate_real_integration())