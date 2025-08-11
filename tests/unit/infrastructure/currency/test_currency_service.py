"""Unit tests for CurrencyService."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from portfolio_manager.infrastructure.currency.currency_service import CurrencyService
from portfolio_manager.domain.services.symbol_mapping import CurrencyCode
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError


class TestCurrencyService:
    """Test cases for CurrencyService."""

    @pytest.fixture
    def mock_exchange_rate_provider(self):
        """Create mock exchange rate provider."""
        mock_provider = Mock()
        mock_provider.get_exchange_rate = AsyncMock()
        mock_provider.get_historical_rates = AsyncMock()
        mock_provider.get_supported_currencies = AsyncMock()
        return mock_provider

    @pytest.fixture
    def mock_cache_repository(self):
        """Create mock cache repository."""
        mock_repo = Mock()
        mock_repo.get_cached_rate = AsyncMock()
        mock_repo.cache_rate = AsyncMock()
        mock_repo.get_cached_rates = AsyncMock()
        mock_repo.clear_expired_cache = AsyncMock()
        return mock_repo

    @pytest.fixture
    def currency_service(self, mock_exchange_rate_provider, mock_cache_repository):
        """Create CurrencyService with mock dependencies."""
        return CurrencyService(
            exchange_rate_provider=mock_exchange_rate_provider,
            cache_repository=mock_cache_repository,
            cache_duration_minutes=60,
            fallback_rates_file="test_fallback.json"
        )

    def test_initialization(self, mock_exchange_rate_provider, mock_cache_repository):
        """Test CurrencyService initialization."""
        service = CurrencyService(
            exchange_rate_provider=mock_exchange_rate_provider,
            cache_repository=mock_cache_repository,
            cache_duration_minutes=120,
            fallback_rates_file="custom_fallback.json"
        )
        
        assert service._exchange_rate_provider == mock_exchange_rate_provider
        assert service._cache_repository == mock_cache_repository
        assert service._cache_duration_minutes == 120
        assert service._fallback_rates_file == "custom_fallback.json"

    def test_initialization_defaults(self, mock_exchange_rate_provider, mock_cache_repository):
        """Test CurrencyService initialization with defaults."""
        service = CurrencyService(
            exchange_rate_provider=mock_exchange_rate_provider,
            cache_repository=mock_cache_repository
        )
        
        assert service._cache_duration_minutes == 60  # Default
        assert service._fallback_rates_file is None  # Default

    @pytest.mark.asyncio
    async def test_get_exchange_rate_cache_hit(self, currency_service, mock_cache_repository):
        """Test getting exchange rate from cache (cache hit)."""
        cached_rate = Decimal("1.2500")
        mock_cache_repository.get_cached_rate.return_value = cached_rate
        
        result = await currency_service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
        
        assert result == cached_rate
        mock_cache_repository.get_cached_rate.assert_called_once_with(CurrencyCode.EUR, CurrencyCode.USD)
        # Should not call external provider
        currency_service._exchange_rate_provider.get_exchange_rate.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_exchange_rate_cache_miss_provider_hit(
        self, currency_service, mock_cache_repository, mock_exchange_rate_provider
    ):
        """Test getting exchange rate with cache miss, provider hit."""
        provider_rate = Decimal("1.2500")
        mock_cache_repository.get_cached_rate.return_value = None
        mock_exchange_rate_provider.get_exchange_rate.return_value = provider_rate
        
        result = await currency_service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
        
        assert result == provider_rate
        
        # Should check cache first (both forward and reverse)
        assert mock_cache_repository.get_cached_rate.call_count == 2
        
        # Should call external provider
        currency_service._exchange_rate_provider.get_exchange_rate.assert_called_once_with(CurrencyCode.EUR, CurrencyCode.USD)
        
        # Should cache the result
        currency_service._cache_repository.cache_rate.assert_called_once_with(CurrencyCode.EUR, CurrencyCode.USD, provider_rate)

    @pytest.mark.asyncio
    async def test_get_exchange_rate_same_currency(self, currency_service):
        """Test getting exchange rate for same currency."""
        result = await currency_service.get_exchange_rate(CurrencyCode.USD, CurrencyCode.USD)
        
        assert result == Decimal("1.0000")
        
        # Should not call cache or provider
        currency_service._cache_repository.get_cached_rate.assert_not_called()
        currency_service._exchange_rate_provider.get_exchange_rate.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_exchange_rate_reverse_lookup(
        self, currency_service, mock_cache_repository, mock_exchange_rate_provider
    ):
        """Test getting exchange rate with reverse lookup."""
        # Cache miss for EUR->USD
        mock_cache_repository.get_cached_rate.side_effect = [None, Decimal("0.8000")]  # USD->EUR cached
        
        result = await currency_service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
        
        # Should return inverse of USD->EUR rate
        expected_rate = Decimal("1.0000") / Decimal("0.8000")
        assert abs(result - expected_rate) < Decimal("0.0001")
        
        # Should check both directions in cache
        assert mock_cache_repository.get_cached_rate.call_count == 2

    @pytest.mark.asyncio
    async def test_get_exchange_rate_provider_error_fallback(
        self, currency_service, mock_cache_repository, mock_exchange_rate_provider
    ):
        """Test getting exchange rate with provider error and fallback rates."""
        mock_cache_repository.get_cached_rate.return_value = None
        mock_exchange_rate_provider.get_exchange_rate.side_effect = Exception("API error")
        
        # Mock loading fallback rates
        fallback_rates = {
            "EUR_USD": "1.2000",
            "GBP_USD": "1.3000",
            "JPY_USD": "0.0090"
        }
        
        with patch.object(currency_service, '_load_fallback_rates', return_value=fallback_rates):
            result = await currency_service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
            
            assert result == Decimal("1.2000")

    @pytest.mark.asyncio
    async def test_get_exchange_rate_no_fallback_available(
        self, currency_service, mock_cache_repository, mock_exchange_rate_provider
    ):
        """Test getting exchange rate with no fallback available."""
        mock_cache_repository.get_cached_rate.return_value = None
        mock_exchange_rate_provider.get_exchange_rate.side_effect = Exception("API error")
        
        # Mock no fallback rates
        with patch.object(currency_service, '_load_fallback_rates', return_value={}):
            result = await currency_service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
            
            assert result is None

    @pytest.mark.asyncio
    async def test_convert_amount_success(self, currency_service, mock_cache_repository):
        """Test successful amount conversion."""
        cached_rate = Decimal("1.2500")
        mock_cache_repository.get_cached_rate.return_value = cached_rate
        
        amount = Decimal("1000.00")
        result = await currency_service.convert_amount(
            amount, CurrencyCode.EUR, CurrencyCode.USD
        )
        
        expected_result = amount * cached_rate
        assert result == expected_result

    @pytest.mark.asyncio
    async def test_convert_amount_same_currency(self, currency_service):
        """Test amount conversion for same currency."""
        amount = Decimal("1000.00")
        result = await currency_service.convert_amount(
            amount, CurrencyCode.USD, CurrencyCode.USD
        )
        
        assert result == amount

    @pytest.mark.asyncio
    async def test_convert_amount_zero_amount(self, currency_service):
        """Test amount conversion with zero amount."""
        result = await currency_service.convert_amount(
            Decimal("0.00"), CurrencyCode.EUR, CurrencyCode.USD
        )
        
        assert result == Decimal("0.00")

    @pytest.mark.asyncio
    async def test_convert_amount_negative_amount(self, currency_service, mock_cache_repository):
        """Test amount conversion with negative amount."""
        cached_rate = Decimal("1.2500")
        mock_cache_repository.get_cached_rate.return_value = cached_rate
        
        amount = Decimal("-500.00")
        result = await currency_service.convert_amount(
            amount, CurrencyCode.EUR, CurrencyCode.USD
        )
        
        expected_result = amount * cached_rate
        assert result == expected_result

    @pytest.mark.asyncio
    async def test_convert_amount_no_exchange_rate(self, currency_service, mock_cache_repository):
        """Test amount conversion when exchange rate unavailable."""
        mock_cache_repository.get_cached_rate.return_value = None
        currency_service._exchange_rate_provider.get_exchange_rate.return_value = None
        
        amount = Decimal("1000.00")
        result = await currency_service.convert_amount(
            amount, CurrencyCode.EUR, CurrencyCode.USD
        )
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_historical_rates_success(
        self, currency_service, mock_exchange_rate_provider
    ):
        """Test getting historical exchange rates."""
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        
        historical_rates = [
            {"date": "2024-01-01", "rate": "1.2500"},
            {"date": "2024-01-02", "rate": "1.2600"},
            {"date": "2024-01-03", "rate": "1.2400"}
        ]
        mock_exchange_rate_provider.get_historical_rates.return_value = historical_rates
        
        result = await currency_service.get_historical_rates(
            CurrencyCode.EUR, CurrencyCode.USD, start_date, end_date
        )
        
        assert len(result) == 3
        assert result[0]["rate"] == Decimal("1.2500")
        assert result[1]["rate"] == Decimal("1.2600")
        assert result[2]["rate"] == Decimal("1.2400")
        
        mock_exchange_rate_provider.get_historical_rates.assert_called_once_with(
            CurrencyCode.EUR, CurrencyCode.USD, start_date, end_date
        )

    @pytest.mark.asyncio
    async def test_get_historical_rates_same_currency(self, currency_service):
        """Test getting historical rates for same currency."""
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 3, tzinfo=timezone.utc)
        
        result = await currency_service.get_historical_rates(
            CurrencyCode.USD, CurrencyCode.USD, start_date, end_date
        )
        
        # Should generate daily rates of 1.0000
        assert len(result) == 3
        assert all(r["rate"] == Decimal("1.0000") for r in result)
        
        # Should not call external provider
        currency_service._exchange_rate_provider.get_historical_rates.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_rates_provider_error(
        self, currency_service, mock_exchange_rate_provider
    ):
        """Test getting historical rates with provider error."""
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        
        mock_exchange_rate_provider.get_historical_rates.side_effect = Exception("API error")
        
        result = await currency_service.get_historical_rates(
            CurrencyCode.EUR, CurrencyCode.USD, start_date, end_date
        )
        
        assert result == []

    @pytest.mark.asyncio
    async def test_get_supported_currencies_success(
        self, currency_service, mock_exchange_rate_provider
    ):
        """Test getting supported currencies."""
        supported_currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
        mock_exchange_rate_provider.get_supported_currencies.return_value = supported_currencies
        
        result = await currency_service.get_supported_currencies()
        
        assert len(result) == 5
        assert CurrencyCode.USD in result
        assert CurrencyCode.EUR in result
        assert CurrencyCode.GBP in result
        assert CurrencyCode.JPY in result
        
        mock_exchange_rate_provider.get_supported_currencies.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_supported_currencies_provider_error(
        self, currency_service, mock_exchange_rate_provider
    ):
        """Test getting supported currencies with provider error."""
        mock_exchange_rate_provider.get_supported_currencies.side_effect = Exception("API error")
        
        result = await currency_service.get_supported_currencies()
        
        # Should return default major currencies
        major_currencies = {CurrencyCode.USD, CurrencyCode.EUR, CurrencyCode.GBP, CurrencyCode.JPY}
        assert major_currencies.issubset(set(result))

    @pytest.mark.asyncio
    async def test_clear_cache_success(self, currency_service, mock_cache_repository):
        """Test clearing cache."""
        mock_cache_repository.clear_cache = AsyncMock(return_value=True)
        
        result = await currency_service.clear_cache()
        
        assert result is True
        mock_cache_repository.clear_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_cache_error(self, currency_service, mock_cache_repository):
        """Test clearing cache with error."""
        mock_cache_repository.clear_cache = AsyncMock(side_effect=Exception("Database error"))
        
        result = await currency_service.clear_cache()
        
        assert result is False

    @pytest.mark.asyncio
    async def test_cleanup_expired_cache(self, currency_service, mock_cache_repository):
        """Test cleanup of expired cache entries."""
        mock_cache_repository.clear_expired_cache.return_value = 5  # Deleted 5 entries
        
        result = await currency_service.cleanup_expired_cache()
        
        assert result == 5
        mock_cache_repository.clear_expired_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_cache_stats(self, currency_service, mock_cache_repository):
        """Test getting cache statistics."""
        mock_stats = {
            "total_entries": 150,
            "expired_entries": 10,
            "hit_rate": 0.85,
            "most_requested_pairs": [
                ("EUR", "USD", 50),
                ("GBP", "USD", 30),
                ("JPY", "USD", 25)
            ]
        }
        mock_cache_repository.get_cache_stats = AsyncMock(return_value=mock_stats)
        
        result = await currency_service.get_cache_stats()
        
        assert result == mock_stats
        mock_cache_repository.get_cache_stats.assert_called_once()

    def test_load_fallback_rates_file_exists(self, currency_service):
        """Test loading fallback rates from file."""
        fallback_data = {
            "EUR_USD": "1.2000",
            "GBP_USD": "1.3000",
            "JPY_USD": "0.0090"
        }
        
        with patch("builtins.open", mock_open_json(fallback_data)):
            with patch("os.path.exists", return_value=True):
                rates = currency_service._load_fallback_rates()
                
                assert rates == fallback_data

    def test_load_fallback_rates_file_not_exists(self, currency_service):
        """Test loading fallback rates when file doesn't exist."""
        with patch("os.path.exists", return_value=False):
            rates = currency_service._load_fallback_rates()
            
            assert rates == {}

    def test_load_fallback_rates_json_error(self, currency_service):
        """Test loading fallback rates with JSON error."""
        with patch("builtins.open", mock_open_text("invalid json")):
            with patch("os.path.exists", return_value=True):
                rates = currency_service._load_fallback_rates()
                
                assert rates == {}

    def test_get_fallback_rate_key_found(self, currency_service):
        """Test getting fallback rate when key found."""
        fallback_rates = {"EUR_USD": "1.2500", "USD_EUR": "0.8000"}
        
        with patch.object(currency_service, '_load_fallback_rates', return_value=fallback_rates):
            rate = currency_service._get_fallback_rate(CurrencyCode.EUR, CurrencyCode.USD)
            
            assert rate == Decimal("1.2500")

    def test_get_fallback_rate_reverse_key_found(self, currency_service):
        """Test getting fallback rate when reverse key found."""
        fallback_rates = {"USD_EUR": "0.8000"}  # Only reverse rate available
        
        with patch.object(currency_service, '_load_fallback_rates', return_value=fallback_rates):
            rate = currency_service._get_fallback_rate(CurrencyCode.EUR, CurrencyCode.USD)
            
            # Should return inverse of reverse rate
            expected_rate = Decimal("1.0000") / Decimal("0.8000")
            assert abs(rate - expected_rate) < Decimal("0.0001")

    def test_get_fallback_rate_not_found(self, currency_service):
        """Test getting fallback rate when not found."""
        fallback_rates = {"GBP_USD": "1.3000"}  # Different currency pair
        
        with patch.object(currency_service, '_load_fallback_rates', return_value=fallback_rates):
            rate = currency_service._get_fallback_rate(CurrencyCode.EUR, CurrencyCode.USD)
            
            assert rate is None

    @pytest.mark.asyncio
    async def test_batch_convert_amounts_success(self, currency_service, mock_cache_repository):
        """Test batch conversion of amounts."""
        # Mock cached rates
        mock_cache_repository.get_cached_rate.side_effect = [
            Decimal("1.2500"),  # EUR->USD
            Decimal("1.3000"),  # GBP->USD
            None  # JPY->USD not cached
        ]
        
        # Mock provider for uncached rate
        currency_service._exchange_rate_provider.get_exchange_rate.return_value = Decimal("0.0090")
        
        conversion_requests = [
            (Decimal("1000.00"), CurrencyCode.EUR, CurrencyCode.USD),
            (Decimal("500.00"), CurrencyCode.GBP, CurrencyCode.USD),
            (Decimal("100000"), CurrencyCode.JPY, CurrencyCode.USD)
        ]
        
        results = await currency_service.batch_convert_amounts(conversion_requests)
        
        assert len(results) == 3
        assert results[0] == Decimal("1000.00") * Decimal("1.2500")  # EUR->USD
        assert results[1] == Decimal("500.00") * Decimal("1.3000")   # GBP->USD
        assert results[2] == Decimal("100000") * Decimal("0.0090")   # JPY->USD

    @pytest.mark.asyncio
    async def test_batch_convert_amounts_partial_failure(self, currency_service, mock_cache_repository):
        """Test batch conversion with partial failures."""
        # Mock some rates available, some not
        mock_cache_repository.get_cached_rate.side_effect = [
            Decimal("1.2500"),  # EUR->USD available
            None  # GBP->USD not available
        ]
        
        # Mock provider failure for unavailable rate
        currency_service._exchange_rate_provider.get_exchange_rate.return_value = None
        
        conversion_requests = [
            (Decimal("1000.00"), CurrencyCode.EUR, CurrencyCode.USD),
            (Decimal("500.00"), CurrencyCode.GBP, CurrencyCode.USD)
        ]
        
        results = await currency_service.batch_convert_amounts(conversion_requests)
        
        assert len(results) == 2
        assert results[0] == Decimal("1000.00") * Decimal("1.2500")  # EUR->USD success
        assert results[1] is None  # GBP->USD failed


class TestCurrencyServiceEdgeCases:
    """Test edge cases for CurrencyService."""

    @pytest.fixture
    def service_with_mocks(self):
        """Create service with mock dependencies for edge case testing."""
        mock_provider = Mock()
        mock_provider.get_exchange_rate = AsyncMock()
        mock_provider.get_historical_rates = AsyncMock()
        mock_provider.get_supported_currencies = AsyncMock()
        
        mock_cache = Mock()
        mock_cache.get_cached_rate = AsyncMock()
        mock_cache.cache_rate = AsyncMock()
        
        return CurrencyService(
            exchange_rate_provider=mock_provider,
            cache_repository=mock_cache
        ), mock_provider, mock_cache

    @pytest.mark.asyncio
    async def test_get_exchange_rate_invalid_currencies(self, service_with_mocks):
        """Test getting exchange rate with invalid currencies."""
        service, _, _ = service_with_mocks
        
        # Test with None currencies
        result1 = await service.get_exchange_rate(None, CurrencyCode.USD)
        assert result1 is None
        
        result2 = await service.get_exchange_rate(CurrencyCode.EUR, None)
        assert result2 is None
        
        result3 = await service.get_exchange_rate(None, None)
        assert result3 is None

    @pytest.mark.asyncio
    async def test_convert_amount_invalid_params(self, service_with_mocks):
        """Test amount conversion with invalid parameters."""
        service, _, _ = service_with_mocks
        
        # Test with None amount
        result1 = await service.convert_amount(None, CurrencyCode.EUR, CurrencyCode.USD)
        assert result1 is None
        
        # Test with None currencies
        result2 = await service.convert_amount(Decimal("100"), None, CurrencyCode.USD)
        assert result2 is None
        
        result3 = await service.convert_amount(Decimal("100"), CurrencyCode.EUR, None)
        assert result3 is None

    @pytest.mark.asyncio
    async def test_get_historical_rates_invalid_dates(self, service_with_mocks):
        """Test getting historical rates with invalid dates."""
        service, _, _ = service_with_mocks
        
        # Test with None dates
        result1 = await service.get_historical_rates(
            CurrencyCode.EUR, CurrencyCode.USD, None, datetime.now()
        )
        assert result1 == []
        
        result2 = await service.get_historical_rates(
            CurrencyCode.EUR, CurrencyCode.USD, datetime.now(), None
        )
        assert result2 == []
        
        # Test with end date before start date
        start_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        result3 = await service.get_historical_rates(
            CurrencyCode.EUR, CurrencyCode.USD, start_date, end_date
        )
        assert result3 == []

    @pytest.mark.asyncio
    async def test_cache_repository_failure_graceful_handling(self, service_with_mocks):
        """Test graceful handling of cache repository failures."""
        service, mock_provider, mock_cache = service_with_mocks
        
        # Cache operations fail
        service._cache_repository.get_cached_rate.side_effect = Exception("Cache error")
        service._cache_repository.cache_rate.side_effect = Exception("Cache error")
        
        # Provider succeeds
        service._exchange_rate_provider.get_exchange_rate.return_value = Decimal("1.2500")
        
        # Should still work despite cache failures
        result = await service.get_exchange_rate(CurrencyCode.EUR, CurrencyCode.USD)
        
        assert result == Decimal("1.2500")
        
        # Should have attempted cache operations but handled errors gracefully
        assert service._cache_repository.get_cached_rate.call_count == 2  # Forward and reverse lookup
        service._exchange_rate_provider.get_exchange_rate.assert_called_once()
        service._cache_repository.cache_rate.assert_called_once()


def mock_open_json(data):
    """Helper to mock open() for JSON files."""
    import json
    from unittest.mock import mock_open
    return mock_open(read_data=json.dumps(data))


def mock_open_text(text):
    """Helper to mock open() for text files."""
    from unittest.mock import mock_open
    return mock_open(read_data=text)