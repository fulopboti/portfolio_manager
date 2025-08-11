"""Unit tests for ExternalSymbolMapper."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from decimal import Decimal
from datetime import datetime, timezone

from portfolio_manager.infrastructure.symbol_mapping.external_symbol_mapper import ExternalSymbolMapper
from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping
)
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError


class TestExternalSymbolMapper:
    """Test cases for ExternalSymbolMapper."""

    @pytest.fixture
    def mock_client(self):
        """Create mock HTTP client."""
        mock_client = Mock()
        mock_client.get = AsyncMock()
        mock_client.post = AsyncMock()
        return mock_client

    @pytest.fixture
    def mapper(self, mock_client):
        """Create ExternalSymbolMapper with mock client."""
        return ExternalSymbolMapper(
            api_base_url="https://api.example.com/v1",
            api_key="test_api_key",
            http_client=mock_client
        )

    @pytest.fixture
    def sample_api_response(self):
        """Create sample API response for testing."""
        return {
            "isin": "US0378331005",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "sector": "Technology",
            "market_cap": "2800000000000",
            "base_currency": "USD",
            "exchanges": [
                {
                    "symbol": "AAPL",
                    "exchange": "NASDAQ",
                    "country": "US",
                    "currency": "USD",
                    "trading_hours": "09:30-16:00 EST"
                },
                {
                    "symbol": "APC.DE",
                    "exchange": "XETRA",
                    "country": "DE",
                    "currency": "EUR",
                    "trading_hours": "09:00-17:30 CET"
                }
            ],
            "providers": [
                {
                    "provider": "yfinance",
                    "symbol": "AAPL",
                    "supports_fundamentals": True,
                    "supports_realtime": False
                },
                {
                    "provider": "mock",
                    "symbol": "AAPL",
                    "supports_fundamentals": False,
                    "supports_realtime": True
                }
            ]
        }

    @pytest.fixture
    def sample_search_response(self):
        """Create sample search API response."""
        return {
            "results": [
                {
                    "isin": "US0378331005",
                    "symbol": "AAPL",
                    "company_name": "Apple Inc.",
                    "sector": "Technology",
                    "base_currency": "USD",
                    "match_score": 0.95
                },
                {
                    "isin": "NL0010273215",
                    "symbol": "ASML.AS",
                    "company_name": "ASML Holding N.V.",
                    "sector": "Technology",
                    "base_currency": "EUR",
                    "match_score": 0.85
                }
            ],
            "total_count": 2
        }

    def test_initialization(self, mock_client):
        """Test ExternalSymbolMapper initialization."""
        mapper = ExternalSymbolMapper(
            api_base_url="https://api.example.com/v1",
            api_key="test_key",
            http_client=mock_client,
            request_timeout=30.0,
            max_retries=3
        )
        
        assert mapper._api_base_url == "https://api.example.com/v1"
        assert mapper._api_key == "test_key"
        assert mapper._http_client == mock_client
        assert mapper._request_timeout == 30.0
        assert mapper._max_retries == 3

    def test_initialization_defaults(self, mock_client):
        """Test ExternalSymbolMapper initialization with defaults."""
        mapper = ExternalSymbolMapper(
            api_base_url="https://api.example.com/v1",
            api_key="test_key",
            http_client=mock_client
        )
        
        assert mapper._request_timeout == 10.0  # Default
        assert mapper._max_retries == 2  # Default

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_success(self, mapper, mock_client, sample_api_response):
        """Test successful retrieval of equivalent symbols."""
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=sample_api_response)
        )
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert len(result) == 1
        mapping = result[0]
        assert isinstance(mapping, SymbolMapping)
        assert mapping.isin == "US0378331005"
        assert mapping.base_symbol == "AAPL"
        assert mapping.company_name == "Apple Inc."
        assert mapping.base_currency == CurrencyCode.USD
        assert len(mapping.exchanges) == 2
        assert len(mapping.providers) == 2
        
        # Verify API call
        expected_url = f"{mapper._api_base_url}/symbols/AAPL"
        mock_client.get.assert_called_once_with(
            expected_url,
            headers={"Authorization": f"Bearer {mapper._api_key}"},
            timeout=mapper._request_timeout
        )

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_not_found(self, mapper, mock_client):
        """Test retrieval of equivalent symbols when not found."""
        mock_client.get.return_value = Mock(
            status_code=404,
            json=Mock(return_value={"error": "Symbol not found"})
        )
        
        result = await mapper.get_equivalent_symbols("UNKNOWN")
        
        assert result == []

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_api_error(self, mapper, mock_client):
        """Test retrieval of equivalent symbols with API error."""
        mock_client.get.return_value = Mock(
            status_code=500,
            json=Mock(return_value={"error": "Internal server error"})
        )
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert result == []

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_network_error(self, mapper, mock_client):
        """Test retrieval of equivalent symbols with network error."""
        mock_client.get.side_effect = Exception("Network error")
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert result == []

    @pytest.mark.asyncio
    async def test_get_provider_symbol_success(self, mapper, mock_client):
        """Test successful retrieval of provider symbol."""
        mock_response = {
            "provider_symbol": "AAPL",
            "provider": "yfinance",
            "supports_fundamentals": True,
            "supports_realtime": False
        }
        
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=mock_response)
        )
        
        result = await mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result == "AAPL"
        
        # Verify API call
        expected_url = f"{mapper._api_base_url}/symbols/AAPL/providers/yfinance"
        mock_client.get.assert_called_once_with(
            expected_url,
            headers={"Authorization": f"Bearer {mapper._api_key}"},
            timeout=mapper._request_timeout
        )

    @pytest.mark.asyncio
    async def test_get_provider_symbol_not_found(self, mapper, mock_client):
        """Test retrieval of provider symbol when not found."""
        mock_client.get.return_value = Mock(
            status_code=404,
            json=Mock(return_value={"error": "Provider mapping not found"})
        )
        
        result = await mapper.get_provider_symbol("UNKNOWN", "yfinance")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_provider_symbol_api_error(self, mapper, mock_client):
        """Test retrieval of provider symbol with API error."""
        mock_client.get.return_value = Mock(
            status_code=500,
            json=Mock(return_value={"error": "Internal server error"})
        )
        
        result = await mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_search_by_company_success(self, mapper, mock_client, sample_search_response):
        """Test successful search by company name."""
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=sample_search_response)
        )
        
        result = await mapper.search_by_company("Apple Inc.")
        
        assert len(result) == 2
        
        # Check first result
        apple_mapping = result[0]
        assert isinstance(apple_mapping, SymbolMapping)
        assert apple_mapping.isin == "US0378331005"
        assert apple_mapping.base_symbol == "AAPL"
        assert apple_mapping.company_name == "Apple Inc."
        
        # Check second result
        asml_mapping = result[1]
        assert asml_mapping.isin == "NL0010273215"
        assert asml_mapping.base_symbol == "ASML.AS"
        assert asml_mapping.company_name == "ASML Holding N.V."
        
        # Verify API call
        expected_url = f"{mapper._api_base_url}/search"
        mock_client.get.assert_called_once_with(
            expected_url,
            params={"company": "Apple Inc.", "limit": 10},
            headers={"Authorization": f"Bearer {mapper._api_key}"},
            timeout=mapper._request_timeout
        )

    @pytest.mark.asyncio
    async def test_search_by_company_with_limit(self, mapper, mock_client, sample_search_response):
        """Test search by company name with custom limit."""
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=sample_search_response)
        )
        
        result = await mapper.search_by_company("Apple Inc.", limit=5)
        
        assert len(result) == 2
        
        # Verify API call with custom limit
        expected_url = f"{mapper._api_base_url}/search"
        mock_client.get.assert_called_once_with(
            expected_url,
            params={"company": "Apple Inc.", "limit": 5},
            headers={"Authorization": f"Bearer {mapper._api_key}"},
            timeout=mapper._request_timeout
        )

    @pytest.mark.asyncio
    async def test_search_by_company_not_found(self, mapper, mock_client):
        """Test search by company when not found."""
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"results": [], "total_count": 0})
        )
        
        result = await mapper.search_by_company("Unknown Company")
        
        assert result == []

    @pytest.mark.asyncio
    async def test_search_by_company_api_error(self, mapper, mock_client):
        """Test search by company with API error."""
        mock_client.get.return_value = Mock(
            status_code=500,
            json=Mock(return_value={"error": "Internal server error"})
        )
        
        result = await mapper.search_by_company("Apple Inc.")
        
        assert result == []

    def test_parse_symbol_mapping_success(self, mapper, sample_api_response):
        """Test parsing symbol mapping from API response."""
        mapping = mapper._parse_symbol_mapping(sample_api_response)
        
        assert isinstance(mapping, SymbolMapping)
        assert mapping.isin == "US0378331005"
        assert mapping.base_symbol == "AAPL"
        assert mapping.company_name == "Apple Inc."
        assert mapping.sector == "Technology"
        assert mapping.base_currency == CurrencyCode.USD
        assert mapping.market_cap_usd == Decimal("2800000000000")
        
        # Check exchanges
        assert len(mapping.exchanges) == 2
        assert "NASDAQ" in mapping.exchanges
        assert "XETRA" in mapping.exchanges
        
        nasdaq_exchange = mapping.exchanges["NASDAQ"]
        assert nasdaq_exchange.symbol == "AAPL"
        assert nasdaq_exchange.country == "US"
        assert nasdaq_exchange.currency == CurrencyCode.USD
        
        # Check providers
        assert len(mapping.providers) == 2
        assert "yfinance" in mapping.providers
        assert "mock" in mapping.providers
        
        yfinance_provider = mapping.providers["yfinance"]
        assert yfinance_provider.symbol == "AAPL"
        assert yfinance_provider.supports_fundamentals is True
        assert yfinance_provider.supports_realtime is False

    def test_parse_symbol_mapping_minimal_data(self, mapper):
        """Test parsing symbol mapping with minimal data."""
        minimal_response = {
            "isin": "US0378331005",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "base_currency": "USD"
        }
        
        mapping = mapper._parse_symbol_mapping(minimal_response)
        
        assert isinstance(mapping, SymbolMapping)
        assert mapping.isin == "US0378331005"
        assert mapping.base_symbol == "AAPL"
        assert mapping.company_name == "Apple Inc."
        assert mapping.base_currency == CurrencyCode.USD
        assert len(mapping.exchanges) == 0
        assert len(mapping.providers) == 0
        assert mapping.sector is None
        assert mapping.market_cap_usd is None

    def test_parse_symbol_mapping_invalid_data(self, mapper):
        """Test parsing symbol mapping with invalid data."""
        invalid_response = {
            "symbol": "AAPL",
            # Missing required fields like isin, company_name, base_currency
        }
        
        mapping = mapper._parse_symbol_mapping(invalid_response)
        
        assert mapping is None  # Should return None for invalid data

    def test_parse_exchange_info(self, mapper):
        """Test parsing exchange info from API data."""
        exchange_data = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD",
            "trading_hours": "09:30-16:00 EST",
            "lot_size": 1,
            "tick_size": "0.01"
        }
        
        exchange_info = mapper._parse_exchange_info(exchange_data)
        
        assert isinstance(exchange_info, ExchangeInfo)
        assert exchange_info.symbol == "AAPL"
        assert exchange_info.exchange == "NASDAQ"
        assert exchange_info.country == "US"
        assert exchange_info.currency == CurrencyCode.USD
        assert exchange_info.trading_hours == "09:30-16:00 EST"
        assert exchange_info.lot_size == 1
        assert exchange_info.tick_size == Decimal("0.01")

    def test_parse_exchange_info_minimal(self, mapper):
        """Test parsing exchange info with minimal data."""
        minimal_data = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "country": "US",
            "currency": "USD"
        }
        
        exchange_info = mapper._parse_exchange_info(minimal_data)
        
        assert isinstance(exchange_info, ExchangeInfo)
        assert exchange_info.symbol == "AAPL"
        assert exchange_info.exchange == "NASDAQ"
        assert exchange_info.country == "US"
        assert exchange_info.currency == CurrencyCode.USD
        assert exchange_info.trading_hours == ""  # Default empty string
        assert exchange_info.lot_size == 1  # Default
        assert exchange_info.tick_size == Decimal("0.01")  # Default

    def test_parse_provider_info(self, mapper):
        """Test parsing provider info from API data."""
        provider_data = {
            "provider": "yfinance",
            "symbol": "AAPL",
            "supports_fundamentals": True,
            "supports_realtime": False
        }
        
        provider_info = mapper._parse_provider_info(provider_data)
        
        assert isinstance(provider_info, ProviderInfo)
        assert provider_info.provider == "yfinance"
        assert provider_info.symbol == "AAPL"
        assert provider_info.supports_fundamentals is True
        assert provider_info.supports_realtime is False

    def test_parse_provider_info_minimal(self, mapper):
        """Test parsing provider info with minimal data."""
        minimal_data = {
            "provider": "yfinance",
            "symbol": "AAPL"
        }
        
        provider_info = mapper._parse_provider_info(minimal_data)
        
        assert isinstance(provider_info, ProviderInfo)
        assert provider_info.provider == "yfinance"
        assert provider_info.symbol == "AAPL"
        assert provider_info.supports_fundamentals is True  # Default
        assert provider_info.supports_realtime is False  # Default

    @pytest.mark.asyncio
    async def test_retry_logic_success_after_failure(self, mapper, mock_client, sample_api_response):
        """Test retry logic succeeds after initial failures."""
        # First call fails, second succeeds
        mock_client.get.side_effect = [
            Exception("Network timeout"),
            Mock(status_code=200, json=Mock(return_value=sample_api_response))
        ]
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert len(result) == 1
        assert result[0].base_symbol == "AAPL"
        assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_logic_max_retries_exceeded(self, mapper, mock_client):
        """Test retry logic when max retries exceeded."""
        mapper._max_retries = 2
        mock_client.get.side_effect = Exception("Persistent network error")
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert result == []
        assert mock_client.get.call_count == 3  # Initial + 2 retries

    @pytest.mark.asyncio
    async def test_request_headers_include_auth(self, mapper, mock_client):
        """Test that requests include proper authorization headers."""
        mock_client.get.return_value = Mock(
            status_code=404,
            json=Mock(return_value={"error": "Not found"})
        )
        
        await mapper.get_equivalent_symbols("AAPL")
        
        # Verify headers include authorization
        call_args = mock_client.get.call_args
        assert "headers" in call_args[1]
        assert "Authorization" in call_args[1]["headers"]
        assert call_args[1]["headers"]["Authorization"] == f"Bearer {mapper._api_key}"

    @pytest.mark.asyncio
    async def test_request_timeout_applied(self, mapper, mock_client):
        """Test that request timeout is properly applied."""
        mock_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"isin": "test", "symbol": "test", "company_name": "test", "base_currency": "USD"})
        )
        
        await mapper.get_equivalent_symbols("AAPL")
        
        # Verify timeout is included in request
        call_args = mock_client.get.call_args
        assert "timeout" in call_args[1]
        assert call_args[1]["timeout"] == mapper._request_timeout


class TestExternalSymbolMapperEdgeCases:
    """Test edge cases for ExternalSymbolMapper."""

    @pytest.fixture
    def mapper(self):
        """Create mapper with mock client for edge case testing."""
        mock_client = Mock()
        mock_client.get = AsyncMock()
        return ExternalSymbolMapper(
            api_base_url="https://api.example.com/v1",
            api_key="test_key",
            http_client=mock_client
        )

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_empty_symbol(self, mapper):
        """Test getting equivalent symbols with empty symbol."""
        result = await mapper.get_equivalent_symbols("")
        assert result == []
        
        # Should not make API call with empty symbol
        mapper._http_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_none_symbol(self, mapper):
        """Test getting equivalent symbols with None symbol."""
        result = await mapper.get_equivalent_symbols(None)
        assert result == []
        
        # Should not make API call with None symbol
        mapper._http_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_provider_symbol_empty_params(self, mapper):
        """Test getting provider symbol with empty parameters."""
        result1 = await mapper.get_provider_symbol("", "yfinance")
        assert result1 is None
        
        result2 = await mapper.get_provider_symbol("AAPL", "")
        assert result2 is None
        
        result3 = await mapper.get_provider_symbol(None, "yfinance")
        assert result3 is None
        
        result4 = await mapper.get_provider_symbol("AAPL", None)
        assert result4 is None
        
        # Should not make API calls with invalid parameters
        mapper._http_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_empty_name(self, mapper):
        """Test searching by company with empty name."""
        result = await mapper.search_by_company("")
        assert result == []
        
        # Should not make API call with empty name
        mapper._http_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_none_name(self, mapper):
        """Test searching by company with None name."""
        result = await mapper.search_by_company(None)
        assert result == []
        
        # Should not make API call with None name
        mapper._http_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_invalid_limit(self, mapper):
        """Test searching by company with invalid limit."""
        # Mock successful API response
        mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value={"results": [], "total_count": 0})
        )
        
        # Test with negative limit (should use default)
        result = await mapper.search_by_company("Apple", limit=-1)
        assert result == []
        
        # Verify API call uses default limit
        call_args = mapper._http_client.get.call_args
        assert call_args[1]["params"]["limit"] == 10  # Default limit

    def test_parse_currency_code_valid(self, mapper):
        """Test parsing valid currency codes."""
        assert mapper._parse_currency_code("USD") == CurrencyCode.USD
        assert mapper._parse_currency_code("EUR") == CurrencyCode.EUR
        assert mapper._parse_currency_code("GBP") == CurrencyCode.GBP
        assert mapper._parse_currency_code("JPY") == CurrencyCode.JPY

    def test_parse_currency_code_invalid(self, mapper):
        """Test parsing invalid currency codes."""
        assert mapper._parse_currency_code("INVALID") == CurrencyCode.USD  # Default fallback
        assert mapper._parse_currency_code("") == CurrencyCode.USD  # Default fallback
        assert mapper._parse_currency_code(None) == CurrencyCode.USD  # Default fallback

    def test_safe_decimal_conversion(self, mapper):
        """Test safe decimal conversion."""
        assert mapper._safe_decimal("123.45") == Decimal("123.45")
        assert mapper._safe_decimal("1000000") == Decimal("1000000")
        assert mapper._safe_decimal(123.45) == Decimal("123.45")
        assert mapper._safe_decimal("") is None
        assert mapper._safe_decimal("invalid") is None
        assert mapper._safe_decimal(None) is None