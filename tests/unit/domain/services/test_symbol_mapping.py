"""Unit tests for symbol mapping domain models and services."""

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping,
    SymbolMappingService
)


class TestCurrencyCode:
    """Test cases for CurrencyCode enum."""

    def test_currency_code_values(self):
        """Test that currency codes have correct string values."""
        assert CurrencyCode.USD.value == "USD"
        assert CurrencyCode.EUR.value == "EUR"
        assert CurrencyCode.GBP.value == "GBP"
        assert CurrencyCode.JPY.value == "JPY"

    def test_currency_code_membership(self):
        """Test currency code membership."""
        assert "USD" in [c.value for c in CurrencyCode]
        assert "EUR" in [c.value for c in CurrencyCode]
        assert "INVALID" not in [c.value for c in CurrencyCode]

    def test_currency_code_iteration(self):
        """Test that we can iterate over currency codes."""
        codes = list(CurrencyCode)
        assert len(codes) >= 10  # Should have at least 10 currencies
        assert CurrencyCode.USD in codes
        assert CurrencyCode.EUR in codes


class TestExchangeInfo:
    """Test cases for ExchangeInfo dataclass."""

    def test_exchange_info_creation(self):
        """Test basic ExchangeInfo creation."""
        exchange = ExchangeInfo(
            symbol="AAPL",
            exchange="NASDAQ",
            country="US",
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST"
        )
        
        assert exchange.symbol == "AAPL"
        assert exchange.exchange == "NASDAQ"
        assert exchange.country == "US"
        assert exchange.currency == CurrencyCode.USD
        assert exchange.trading_hours == "09:30-16:00 EST"
        assert exchange.lot_size == 1  # Default value
        assert exchange.tick_size == Decimal('0.01')  # Default value

    def test_exchange_info_with_custom_values(self):
        """Test ExchangeInfo with custom lot size and tick size."""
        exchange = ExchangeInfo(
            symbol="SAP.DE",
            exchange="XETRA",
            country="DE",
            currency=CurrencyCode.EUR,
            trading_hours="09:00-17:30 CET",
            lot_size=10,
            tick_size=Decimal('0.001')
        )
        
        assert exchange.lot_size == 10
        assert exchange.tick_size == Decimal('0.001')

    def test_exchange_info_equality(self):
        """Test ExchangeInfo equality comparison."""
        exchange1 = ExchangeInfo(
            symbol="AAPL",
            exchange="NASDAQ",
            country="US", 
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST"
        )
        
        exchange2 = ExchangeInfo(
            symbol="AAPL",
            exchange="NASDAQ",
            country="US",
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST"
        )
        
        exchange3 = ExchangeInfo(
            symbol="AAPL.L",
            exchange="LSE",
            country="GB",
            currency=CurrencyCode.GBP,
            trading_hours="08:00-16:30 GMT"
        )
        
        assert exchange1 == exchange2
        assert exchange1 != exchange3

    def test_exchange_info_repr(self):
        """Test ExchangeInfo string representation."""
        exchange = ExchangeInfo(
            symbol="AAPL",
            exchange="NASDAQ",
            country="US",
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST"
        )
        
        repr_str = repr(exchange)
        assert "AAPL" in repr_str
        assert "NASDAQ" in repr_str
        assert "USD" in repr_str


class TestProviderInfo:
    """Test cases for ProviderInfo dataclass."""

    def test_provider_info_creation(self):
        """Test basic ProviderInfo creation."""
        provider = ProviderInfo(
            symbol="AAPL",
            provider="yfinance"
        )
        
        assert provider.symbol == "AAPL"
        assert provider.provider == "yfinance"
        assert provider.supports_fundamentals is True  # Default
        assert provider.supports_realtime is False  # Default

    def test_provider_info_with_custom_capabilities(self):
        """Test ProviderInfo with custom capabilities."""
        provider = ProviderInfo(
            symbol="SAPG",
            provider="yfinance",
            supports_fundamentals=False,
            supports_realtime=True
        )
        
        assert provider.supports_fundamentals is False
        assert provider.supports_realtime is True

    def test_provider_info_equality(self):
        """Test ProviderInfo equality comparison."""
        provider1 = ProviderInfo(symbol="AAPL", provider="yfinance")
        provider2 = ProviderInfo(symbol="AAPL", provider="yfinance")
        provider3 = ProviderInfo(symbol="AAPL", provider="mock")
        
        assert provider1 == provider2
        assert provider1 != provider3


class TestSymbolMapping:
    """Test cases for SymbolMapping dataclass."""

    @pytest.fixture
    def sample_mapping(self):
        """Create a sample SymbolMapping for testing."""
        return SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )

    def test_symbol_mapping_creation(self, sample_mapping):
        """Test basic SymbolMapping creation."""
        assert sample_mapping.isin == "US0378331005"
        assert sample_mapping.base_symbol == "AAPL"
        assert sample_mapping.base_exchange == "NASDAQ"
        assert sample_mapping.base_country == "US"
        assert sample_mapping.base_currency == CurrencyCode.USD
        assert sample_mapping.company_name == "Apple Inc."
        assert sample_mapping.sector == "Technology"

    def test_symbol_mapping_post_init(self, sample_mapping):
        """Test that post_init properly initializes collections."""
        assert isinstance(sample_mapping.exchanges, dict)
        assert isinstance(sample_mapping.providers, dict)
        assert len(sample_mapping.exchanges) == 0
        assert len(sample_mapping.providers) == 0

    def test_symbol_mapping_with_collections(self):
        """Test SymbolMapping with pre-populated collections."""
        exchanges = {
            "XETRA": ExchangeInfo(
                symbol="APC.DE",
                exchange="XETRA", 
                country="DE",
                currency=CurrencyCode.EUR,
                trading_hours="09:00-17:30 CET"
            )
        }
        
        providers = {
            "yfinance": ProviderInfo(symbol="AAPL", provider="yfinance")
        }
        
        mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            exchanges=exchanges,
            providers=providers
        )
        
        assert len(mapping.exchanges) == 1
        assert len(mapping.providers) == 1
        assert "XETRA" in mapping.exchanges
        assert "yfinance" in mapping.providers

    def test_get_currency_for_exchange_base(self, sample_mapping):
        """Test getting currency for base exchange."""
        currency = sample_mapping.get_currency_for_exchange("NASDAQ")
        assert currency == CurrencyCode.USD

    def test_get_currency_for_exchange_other(self, sample_mapping):
        """Test getting currency for other exchanges."""
        sample_mapping.exchanges["XETRA"] = ExchangeInfo(
            symbol="APC.DE",
            exchange="XETRA",
            country="DE",
            currency=CurrencyCode.EUR,
            trading_hours="09:00-17:30 CET"
        )
        
        currency = sample_mapping.get_currency_for_exchange("XETRA")
        assert currency == CurrencyCode.EUR

    def test_get_currency_for_exchange_unknown(self, sample_mapping):
        """Test getting currency for unknown exchange."""
        currency = sample_mapping.get_currency_for_exchange("UNKNOWN")
        assert currency is None

    def test_get_all_currencies_base_only(self, sample_mapping):
        """Test getting all currencies with only base currency."""
        currencies = sample_mapping.get_all_currencies()
        assert currencies == {CurrencyCode.USD}

    def test_get_all_currencies_multiple(self, sample_mapping):
        """Test getting all currencies with multiple exchanges."""
        sample_mapping.exchanges["XETRA"] = ExchangeInfo(
            symbol="APC.DE",
            exchange="XETRA",
            country="DE", 
            currency=CurrencyCode.EUR,
            trading_hours="09:00-17:30 CET"
        )
        
        sample_mapping.exchanges["LSE"] = ExchangeInfo(
            symbol="AAPL.L",
            exchange="LSE",
            country="GB",
            currency=CurrencyCode.GBP,
            trading_hours="08:00-16:30 GMT"
        )
        
        currencies = sample_mapping.get_all_currencies()
        expected = {CurrencyCode.USD, CurrencyCode.EUR, CurrencyCode.GBP}
        assert currencies == expected

    def test_get_all_currencies_duplicate(self, sample_mapping):
        """Test getting all currencies with duplicate currency."""
        # Add another USD exchange
        sample_mapping.exchanges["NYSE"] = ExchangeInfo(
            symbol="AAPL",
            exchange="NYSE",
            country="US",
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST"
        )
        
        currencies = sample_mapping.get_all_currencies()
        assert currencies == {CurrencyCode.USD}  # Should still be just USD

    def test_symbol_mapping_with_market_cap(self):
        """Test SymbolMapping with market cap."""
        mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ", 
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            market_cap_usd=Decimal("2800000000000")  # $2.8T
        )
        
        assert mapping.market_cap_usd == Decimal("2800000000000")

    def test_symbol_mapping_equality(self):
        """Test SymbolMapping equality comparison."""
        mapping1 = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US", 
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        
        mapping2 = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        
        mapping3 = SymbolMapping(
            isin="DE0007164600",
            base_symbol="SAP.DE",
            base_exchange="XETRA",
            base_country="DE",
            base_currency=CurrencyCode.EUR,
            company_name="SAP SE",
            sector="Software"
        )
        
        assert mapping1 == mapping2
        assert mapping1 != mapping3

    def test_symbol_mapping_repr(self, sample_mapping):
        """Test SymbolMapping string representation."""
        repr_str = repr(sample_mapping)
        assert "US0378331005" in repr_str
        assert "AAPL" in repr_str
        assert "Apple Inc." in repr_str


class TestSymbolMappingServiceInterface:
    """Test cases for SymbolMappingService abstract interface."""

    def test_symbol_mapping_service_is_abstract(self):
        """Test that SymbolMappingService cannot be instantiated."""
        with pytest.raises(TypeError):
            SymbolMappingService()

    def test_symbol_mapping_service_methods_are_abstract(self):
        """Test that abstract methods are properly defined."""
        # Check that all expected methods exist and are abstract
        abstract_methods = SymbolMappingService.__abstractmethods__
        expected_methods = {
            'get_equivalent_symbols',
            'get_provider_symbol', 
            'search_by_company'
        }
        
        assert abstract_methods == expected_methods


class MockSymbolMappingService(SymbolMappingService):
    """Mock implementation for testing."""
    
    def __init__(self):
        self.mappings = {}
        self.provider_symbols = {}
        self.company_mappings = {}

    async def get_equivalent_symbols(self, symbol: str):
        return self.mappings.get(symbol, [])

    async def get_provider_symbol(self, symbol: str, provider: str):
        return self.provider_symbols.get(f"{symbol}:{provider}")

    async def search_by_company(self, company_name: str):
        return self.company_mappings.get(company_name.lower(), [])


class TestMockSymbolMappingService:
    """Test cases for mock implementation."""

    @pytest.fixture
    def mock_service(self):
        """Create mock service with test data."""
        service = MockSymbolMappingService()
        
        # Add test mapping
        apple_mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        
        service.mappings["AAPL"] = [apple_mapping]
        service.provider_symbols["AAPL:yfinance"] = "AAPL"
        service.provider_symbols["AAPL:mock"] = "AAPL"
        service.company_mappings["apple inc."] = [apple_mapping]
        
        return service

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols(self, mock_service):
        """Test getting equivalent symbols."""
        mappings = await mock_service.get_equivalent_symbols("AAPL")
        assert len(mappings) == 1
        assert mappings[0].base_symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_not_found(self, mock_service):
        """Test getting equivalent symbols for unknown symbol."""
        mappings = await mock_service.get_equivalent_symbols("UNKNOWN")
        assert mappings == []

    @pytest.mark.asyncio
    async def test_get_provider_symbol(self, mock_service):
        """Test getting provider-specific symbol."""
        symbol = await mock_service.get_provider_symbol("AAPL", "yfinance")
        assert symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_get_provider_symbol_not_found(self, mock_service):
        """Test getting provider symbol for unknown combination."""
        symbol = await mock_service.get_provider_symbol("AAPL", "unknown")
        assert symbol is None

    @pytest.mark.asyncio
    async def test_search_by_company(self, mock_service):
        """Test searching by company name."""
        mappings = await mock_service.search_by_company("Apple Inc.")
        assert len(mappings) == 1
        assert mappings[0].company_name == "Apple Inc."

    @pytest.mark.asyncio
    async def test_search_by_company_case_insensitive(self, mock_service):
        """Test that company search is case insensitive."""
        mappings = await mock_service.search_by_company("APPLE INC.")
        assert len(mappings) == 1
        assert mappings[0].company_name == "Apple Inc."

    @pytest.mark.asyncio
    async def test_search_by_company_not_found(self, mock_service):
        """Test searching for unknown company."""
        mappings = await mock_service.search_by_company("Unknown Company")
        assert mappings == []


class TestSymbolMappingEdgeCases:
    """Test edge cases and error conditions."""

    def test_symbol_mapping_empty_isin(self):
        """Test SymbolMapping with empty ISIN."""
        # Empty ISIN should be allowed (no validation in dataclass)
        mapping = SymbolMapping(
            isin="",  # Empty ISIN
            base_symbol="AAPL",
            base_exchange="NASDAQ", 
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        assert mapping.isin == ""

    def test_exchange_info_negative_lot_size(self):
        """Test ExchangeInfo with negative lot size."""
        # Should allow negative lot size (some markets allow fractional shares)
        exchange = ExchangeInfo(
            symbol="AAPL",
            exchange="NASDAQ",
            country="US",
            currency=CurrencyCode.USD,
            trading_hours="09:30-16:00 EST",
            lot_size=-1
        )
        
        assert exchange.lot_size == -1

    def test_exchange_info_zero_tick_size(self):
        """Test ExchangeInfo with zero tick size."""
        exchange = ExchangeInfo(
            symbol="CRYPTO",
            exchange="BINANCE",
            country="GLOBAL",
            currency=CurrencyCode.USD,
            trading_hours="24/7",
            tick_size=Decimal('0')
        )
        
        assert exchange.tick_size == Decimal('0')

    def test_symbol_mapping_none_market_cap(self):
        """Test SymbolMapping with None market cap."""
        mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            market_cap_usd=None
        )
        
        assert mapping.market_cap_usd is None

    def test_currency_code_comparison(self):
        """Test CurrencyCode comparison operations."""
        assert CurrencyCode.USD == CurrencyCode.USD
        assert CurrencyCode.USD != CurrencyCode.EUR
        assert CurrencyCode.USD.value == "USD"
        
        # Test in sets and dictionaries
        currency_set = {CurrencyCode.USD, CurrencyCode.EUR, CurrencyCode.USD}
        assert len(currency_set) == 2  # USD should not be duplicated
        
        currency_dict = {CurrencyCode.USD: "US Dollar", CurrencyCode.EUR: "Euro"}
        assert currency_dict[CurrencyCode.USD] == "US Dollar"