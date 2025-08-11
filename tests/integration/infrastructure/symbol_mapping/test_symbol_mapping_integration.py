"""Integration tests for symbol mapping system."""

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from portfolio_manager.infrastructure.symbol_mapping.hybrid_symbol_mapper import HybridSymbolMapper
from portfolio_manager.infrastructure.symbol_mapping.database_symbol_mapper import DatabaseSymbolMapper
from portfolio_manager.infrastructure.symbol_mapping.external_symbol_mapper import ExternalSymbolMapper
from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping
)
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError


# Skip these tests by default since they may require database setup
pytestmark = pytest.mark.integration


class TestSymbolMappingSystemIntegration:
    """Integration tests for the complete symbol mapping system."""

    @pytest.fixture
    def database_mapper(self):
        """Create database mapper with test database."""
        # This would typically use a test database
        # For now, we'll use mock to avoid database dependencies
        from unittest.mock import Mock, AsyncMock
        
        mock_repo = Mock()
        mock_repo.find_by_symbol = AsyncMock()
        mock_repo.find_by_isin = AsyncMock()
        mock_repo.find_by_company_name = AsyncMock()
        mock_repo.find_provider_symbol = AsyncMock()
        mock_repo.create = AsyncMock()
        mock_repo.update = AsyncMock()
        mock_repo.delete = AsyncMock()
        mock_repo.list_all = AsyncMock()
        
        return DatabaseSymbolMapper(repository=mock_repo)

    @pytest.fixture
    def external_mapper(self):
        """Create external mapper with test configuration."""
        from unittest.mock import Mock, AsyncMock
        
        mock_client = Mock()
        mock_client.get = AsyncMock()
        
        return ExternalSymbolMapper(
            api_base_url="https://api.test.com/v1",
            api_key="test_api_key",
            http_client=mock_client,
            request_timeout=5.0,
            max_retries=1
        )

    @pytest.fixture
    def hybrid_mapper(self, database_mapper, external_mapper):
        """Create hybrid mapper with test configuration."""
        return HybridSymbolMapper(
            database_mapper=database_mapper,
            external_mapper=external_mapper,
            cache_duration_hours=1,  # Short cache for testing
            fallback_to_external=True
        )

    @pytest.fixture
    def apple_mapping(self):
        """Create comprehensive Apple stock mapping."""
        exchanges = {
            "NASDAQ": ExchangeInfo(
                symbol="AAPL",
                exchange="NASDAQ",
                country="US",
                currency=CurrencyCode.USD,
                trading_hours="09:30-16:00 EST",
                lot_size=1,
                tick_size=Decimal("0.01")
            ),
            "XETRA": ExchangeInfo(
                symbol="APC.DE",
                exchange="XETRA",
                country="DE",
                currency=CurrencyCode.EUR,
                trading_hours="09:00-17:30 CET",
                lot_size=1,
                tick_size=Decimal("0.001")
            ),
            "LSE": ExchangeInfo(
                symbol="AAPL.L",
                exchange="LSE",
                country="GB",
                currency=CurrencyCode.GBP,
                trading_hours="08:00-16:30 GMT",
                lot_size=10,
                tick_size=Decimal("0.01")
            )
        }
        
        providers = {
            "yfinance": ProviderInfo(
                symbol="AAPL",
                provider="yfinance",
                supports_fundamentals=True,
                supports_realtime=False
            ),
            "mock": ProviderInfo(
                symbol="AAPL",
                provider="mock",
                supports_fundamentals=False,
                supports_realtime=True
            ),
            "alpha_vantage": ProviderInfo(
                symbol="AAPL",
                provider="alpha_vantage",
                supports_fundamentals=True,
                supports_realtime=True
            )
        }
        
        return SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            market_cap_usd=Decimal("2800000000000"),
            exchanges=exchanges,
            providers=providers
        )

    @pytest.mark.asyncio
    async def test_end_to_end_symbol_lookup(self, hybrid_mapper, apple_mapping):
        """Test complete end-to-end symbol lookup flow."""
        # Mock database miss, external hit
        hybrid_mapper._database_mapper._repository.find_by_symbol.return_value = []
        
        # Mock external API response
        from unittest.mock import Mock
        api_response = {
            "isin": apple_mapping.isin,
            "symbol": apple_mapping.base_symbol,
            "company_name": apple_mapping.company_name,
            "sector": apple_mapping.sector,
            "market_cap": str(apple_mapping.market_cap_usd),
            "base_currency": apple_mapping.base_currency.value,
            "exchanges": [
                {
                    "symbol": info.symbol,
                    "exchange": info.exchange,
                    "country": info.country,
                    "currency": info.currency.value,
                    "trading_hours": info.trading_hours,
                    "lot_size": info.lot_size,
                    "tick_size": str(info.tick_size)
                }
                for info in apple_mapping.exchanges.values()
            ],
            "providers": [
                {
                    "provider": info.provider,
                    "symbol": info.symbol,
                    "supports_fundamentals": info.supports_fundamentals,
                    "supports_realtime": info.supports_realtime
                }
                for info in apple_mapping.providers.values()
            ]
        }
        
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=api_response)
        )
        
        # Mock successful caching
        hybrid_mapper._database_mapper._repository.create.return_value = apple_mapping
        
        # Perform the lookup
        results = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        # Verify results
        assert len(results) == 1
        result_mapping = results[0]
        
        assert result_mapping.isin == apple_mapping.isin
        assert result_mapping.base_symbol == apple_mapping.base_symbol
        assert result_mapping.company_name == apple_mapping.company_name
        assert result_mapping.base_currency == apple_mapping.base_currency
        assert len(result_mapping.exchanges) == 3
        assert len(result_mapping.providers) == 3
        
        # Verify all currencies are represented
        currencies = result_mapping.get_all_currencies()
        expected_currencies = {CurrencyCode.USD, CurrencyCode.EUR, CurrencyCode.GBP}
        assert currencies == expected_currencies
        
        # Verify external API was called
        hybrid_mapper._external_mapper._http_client.get.assert_called_once()
        
        # Verify caching was attempted
        hybrid_mapper._database_mapper._repository.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_cross_exchange_currency_conversion_workflow(self, hybrid_mapper, apple_mapping):
        """Test workflow for cross-exchange currency conversion."""
        # Setup: mapping exists in database
        hybrid_mapper._database_mapper._repository.find_by_symbol.return_value = [apple_mapping]
        
        # Get symbol mapping
        results = await hybrid_mapper.get_equivalent_symbols("AAPL")
        mapping = results[0]
        
        # Test currency conversion scenarios
        scenarios = [
            ("NASDAQ", CurrencyCode.USD),
            ("XETRA", CurrencyCode.EUR),
            ("LSE", CurrencyCode.GBP)
        ]
        
        for exchange, expected_currency in scenarios:
            currency = mapping.get_currency_for_exchange(exchange)
            assert currency == expected_currency, f"Wrong currency for {exchange}"
        
        # Test unknown exchange
        unknown_currency = mapping.get_currency_for_exchange("UNKNOWN")
        assert unknown_currency is None

    @pytest.mark.asyncio
    async def test_provider_specific_symbol_mapping(self, hybrid_mapper, apple_mapping):
        """Test provider-specific symbol mapping functionality."""
        # Setup: mapping exists in database
        hybrid_mapper._database_mapper._repository.find_by_symbol.return_value = [apple_mapping]
        hybrid_mapper._database_mapper._repository.find_provider_symbol.side_effect = [
            "AAPL",    # yfinance
            "AAPL",    # mock
            "AAPL",    # alpha_vantage
            None       # unknown provider
        ]
        
        # Test different providers
        provider_tests = [
            ("yfinance", "AAPL"),
            ("mock", "AAPL"),
            ("alpha_vantage", "AAPL"),
            ("unknown_provider", None)
        ]
        
        for provider, expected_symbol in provider_tests:
            result = await hybrid_mapper.get_provider_symbol("AAPL", provider)
            assert result == expected_symbol, f"Wrong symbol for provider {provider}"

    @pytest.mark.asyncio
    async def test_company_search_with_multiple_results(self, hybrid_mapper):
        """Test company search returning multiple results."""
        # Create multiple Apple-related companies
        apple_main = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            market_cap_usd=Decimal("2800000000000")
        )
        
        apple_reit = SymbolMapping(
            isin="US12345678901",
            base_symbol="APLE",
            base_exchange="NYSE",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Hospitality REIT",
            sector="Real Estate",
            market_cap_usd=Decimal("5000000000")
        )
        
        # Mock database miss, external hit
        hybrid_mapper._database_mapper._repository.find_by_company_name.return_value = []
        
        # Mock external search response
        from unittest.mock import Mock
        search_response = {
            "results": [
                {
                    "isin": apple_main.isin,
                    "symbol": apple_main.base_symbol,
                    "company_name": apple_main.company_name,
                    "sector": apple_main.sector,
                    "market_cap": str(apple_main.market_cap_usd),
                    "base_currency": apple_main.base_currency.value,
                    "match_score": 0.95
                },
                {
                    "isin": apple_reit.isin,
                    "symbol": apple_reit.base_symbol,
                    "company_name": apple_reit.company_name,
                    "sector": apple_reit.sector,
                    "market_cap": str(apple_reit.market_cap_usd),
                    "base_currency": apple_reit.base_currency.value,
                    "match_score": 0.75
                }
            ],
            "total_count": 2
        }
        
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=search_response)
        )
        
        # Mock successful caching
        hybrid_mapper._database_mapper._repository.create.side_effect = [apple_main, apple_reit]
        
        # Perform search
        results = await hybrid_mapper.search_by_company("Apple")
        
        # Verify results
        assert len(results) == 2
        
        # Results should be in order of relevance (match_score)
        assert results[0].base_symbol == "AAPL"
        assert results[0].market_cap_usd == Decimal("2800000000000")
        assert results[1].base_symbol == "APLE"
        assert results[1].market_cap_usd == Decimal("5000000000")
        
        # Verify both were cached
        assert hybrid_mapper._database_mapper._repository.create.call_count == 2

    @pytest.mark.asyncio
    async def test_cache_refresh_and_invalidation(self, hybrid_mapper, apple_mapping):
        """Test cache refresh and invalidation mechanisms."""
        # Setup: old data in database
        old_mapping = SymbolMapping(
            isin=apple_mapping.isin,
            base_symbol=apple_mapping.base_symbol,
            base_exchange=apple_mapping.base_exchange,
            base_country=apple_mapping.base_country,
            base_currency=apple_mapping.base_currency,
            company_name=apple_mapping.company_name,
            sector=apple_mapping.sector,
            market_cap_usd=Decimal("2500000000000")  # Old market cap
        )
        
        # Mock external API with updated data
        from unittest.mock import Mock
        updated_response = {
            "isin": apple_mapping.isin,
            "symbol": apple_mapping.base_symbol,
            "company_name": apple_mapping.company_name,
            "sector": apple_mapping.sector,
            "market_cap": str(apple_mapping.market_cap_usd),  # Updated market cap
            "base_currency": apple_mapping.base_currency.value,
            "exchanges": [],
            "providers": []
        }
        
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=updated_response)
        )
        
        # Mock database update
        hybrid_mapper._database_mapper._repository.update.return_value = apple_mapping
        
        # Perform cache refresh
        success = await hybrid_mapper.refresh_cache("AAPL")
        
        assert success is True
        
        # Verify external API was called
        hybrid_mapper._external_mapper._http_client.get.assert_called_once()
        
        # Verify database update was called
        hybrid_mapper._database_mapper._repository.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_and_fallback_scenarios(self, hybrid_mapper):
        """Test various error scenarios and fallback mechanisms."""
        # Scenario 1: Database error, external success
        hybrid_mapper._database_mapper._repository.find_by_symbol.side_effect = DataAccessError("DB error")
        
        from unittest.mock import Mock
        api_response = {
            "isin": "US0378331005",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "base_currency": "USD"
        }
        
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=api_response)
        )
        
        results = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        # Should still get results from external API
        assert len(results) == 1
        assert results[0].base_symbol == "AAPL"
        
        # Scenario 2: Both database and external fail
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=500,
            json=Mock(return_value={"error": "Internal server error"})
        )
        
        results = await hybrid_mapper.get_equivalent_symbols("UNKNOWN")
        
        # Should return empty results gracefully
        assert results == []
        
        # Scenario 3: External API timeout
        hybrid_mapper._external_mapper._http_client.get.side_effect = Exception("Timeout")
        
        results = await hybrid_mapper.get_equivalent_symbols("TIMEOUT")
        
        # Should handle gracefully
        assert results == []

    @pytest.mark.asyncio
    async def test_performance_with_concurrent_requests(self, hybrid_mapper, apple_mapping):
        """Test system performance with concurrent requests."""
        import asyncio
        
        # Setup: data available in external API
        from unittest.mock import Mock
        api_response = {
            "isin": apple_mapping.isin,
            "symbol": apple_mapping.base_symbol,
            "company_name": apple_mapping.company_name,
            "base_currency": apple_mapping.base_currency.value,
            "base_exchange": apple_mapping.base_exchange,
            "base_country": apple_mapping.base_country,
            "sector": apple_mapping.sector
        }
        
        # Database initially empty
        hybrid_mapper._database_mapper._repository.find_by_symbol.return_value = []
        hybrid_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200, 
            json=Mock(return_value=api_response)
        )
        hybrid_mapper._database_mapper._repository.create.return_value = apple_mapping
        
        # Make concurrent requests for the same symbol
        symbols = ["AAPL"] * 5  # 5 concurrent requests for same symbol
        tasks = [hybrid_mapper.get_equivalent_symbols(symbol) for symbol in symbols]
        
        start_time = asyncio.get_event_loop().time()
        results = await asyncio.gather(*tasks)
        end_time = asyncio.get_event_loop().time()
        
        # All requests should succeed
        assert len(results) == 5
        assert all(len(result) == 1 for result in results)
        assert all(result[0].base_symbol == "AAPL" for result in results)
        
        # Should complete reasonably quickly (within 1 second)
        elapsed_time = end_time - start_time
        assert elapsed_time < 1.0
        
        # External API should be called (caching behavior depends on implementation)
        assert hybrid_mapper._external_mapper._http_client.get.call_count >= 1

    @pytest.mark.asyncio
    async def test_data_consistency_across_operations(self, hybrid_mapper, apple_mapping):
        """Test data consistency across different operations."""
        # Setup: mapping in database
        hybrid_mapper._database_mapper._repository.find_by_symbol.return_value = [apple_mapping]
        hybrid_mapper._database_mapper._repository.find_by_isin.return_value = apple_mapping
        hybrid_mapper._database_mapper._repository.find_by_company_name.return_value = [apple_mapping]
        hybrid_mapper._database_mapper._repository.find_provider_symbol.return_value = "AAPL"
        
        # Test 1: Symbol lookup
        symbol_results = await hybrid_mapper.get_equivalent_symbols("AAPL")
        assert len(symbol_results) == 1
        symbol_mapping = symbol_results[0]
        
        # Test 2: Company search should return same data
        company_results = await hybrid_mapper.search_by_company("Apple Inc.")
        assert len(company_results) == 1
        company_mapping = company_results[0]
        
        # Test 3: Provider symbol lookup
        provider_symbol = await hybrid_mapper.get_provider_symbol("AAPL", "yfinance")
        assert provider_symbol == "AAPL"
        
        # Verify consistency
        assert symbol_mapping.isin == company_mapping.isin
        assert symbol_mapping.base_symbol == company_mapping.base_symbol
        assert symbol_mapping.company_name == company_mapping.company_name
        assert symbol_mapping.base_currency == company_mapping.base_currency
        
        # Verify currency information is consistent
        symbol_currencies = symbol_mapping.get_all_currencies()
        company_currencies = company_mapping.get_all_currencies()
        assert symbol_currencies == company_currencies

    @pytest.mark.asyncio
    async def test_system_resource_cleanup(self, hybrid_mapper):
        """Test proper cleanup of system resources."""
        # This test would verify that resources are properly cleaned up
        # For now, we'll test basic cleanup operations
        
        # Test cache clearing
        from unittest.mock import Mock, AsyncMock
        hybrid_mapper._database_mapper._repository.clear_cache = AsyncMock(return_value=True)
        
        cache_cleared = await hybrid_mapper.clear_cache()
        assert cache_cleared is True
        
        # Test statistics gathering
        mock_stats = {
            "cache_hit_rate": 0.75,
            "total_requests": 1000,
            "external_api_calls": 250,
            "database_operations": 1500
        }
        
        hybrid_mapper._database_mapper._repository.get_cache_stats = AsyncMock(return_value=mock_stats)
        
        stats = await hybrid_mapper.get_cache_stats()
        assert stats["cache_hit_rate"] == 0.75
        assert stats["total_requests"] == 1000


class TestSymbolMappingSystemRealWorldScenarios:
    """Real-world scenario tests for symbol mapping system."""

    @pytest.fixture
    def production_like_mapper(self):
        """Create mapper configured for production-like scenarios."""
        from unittest.mock import Mock, AsyncMock
        
        # Database mapper with realistic repository
        mock_repo = Mock()
        mock_repo.find_by_symbol = AsyncMock()
        mock_repo.find_by_isin = AsyncMock()
        mock_repo.find_by_company_name = AsyncMock()
        mock_repo.find_provider_symbol = AsyncMock()
        mock_repo.create = AsyncMock()
        mock_repo.update = AsyncMock()
        
        database_mapper = DatabaseSymbolMapper(repository=mock_repo)
        
        # External mapper with realistic configuration
        mock_client = Mock()
        mock_client.get = AsyncMock()
        
        external_mapper = ExternalSymbolMapper(
            api_base_url="https://api.symbolmapping.com/v1",
            api_key="production_api_key",
            http_client=mock_client,
            request_timeout=30.0,
            max_retries=3
        )
        
        # Hybrid mapper with production settings
        return HybridSymbolMapper(
            database_mapper=database_mapper,
            external_mapper=external_mapper,
            cache_duration_hours=24,
            fallback_to_external=True
        )

    @pytest.mark.asyncio
    async def test_high_volume_trading_scenario(self, production_like_mapper):
        """Test scenario with high-volume trading requiring frequent symbol lookups."""
        # Simulate high-volume trading symbols
        trading_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "SPY", "QQQ", "IVV", "VTI", "ARKK"
        ]
        
        # Mock database with some cached, some not
        cached_symbols = {"AAPL", "MSFT", "SPY"}
        
        def mock_db_lookup(symbol):
            if symbol in cached_symbols:
                return [SymbolMapping(
                    isin=f"US{symbol}00001",
                    base_symbol=symbol,
                    base_exchange="NASDAQ",
                    base_country="US",
                    base_currency=CurrencyCode.USD,
                    company_name=f"{symbol} Inc.",
                    sector="Technology"
                )]
            return []
        
        production_like_mapper._database_mapper._repository.find_by_symbol.side_effect = mock_db_lookup
        
        # Mock external API for uncached symbols
        def mock_external_response(symbol):
            from unittest.mock import Mock
            return Mock(
                status_code=200,
                json=Mock(return_value={
                    "isin": f"US{symbol}00001",
                    "symbol": symbol,
                    "company_name": f"{symbol} Inc.",
                    "base_currency": "USD"
                })
            )
        
        production_like_mapper._external_mapper._http_client.get.side_effect = [
            mock_external_response(symbol) for symbol in trading_symbols if symbol not in cached_symbols
        ]
        
        # Batch lookup simulation
        import asyncio
        lookup_tasks = [
            production_like_mapper.get_equivalent_symbols(symbol)
            for symbol in trading_symbols
        ]
        
        results = await asyncio.gather(*lookup_tasks, return_exceptions=True)
        
        # Verify all lookups succeeded
        successful_results = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_results) == len(trading_symbols)
        
        # Verify cached symbols were served from cache
        db_call_count = production_like_mapper._database_mapper._repository.find_by_symbol.call_count
        assert db_call_count == len(trading_symbols)

    @pytest.mark.asyncio
    async def test_cross_border_arbitrage_scenario(self, production_like_mapper):
        """Test scenario for cross-border arbitrage requiring multi-exchange data."""
        # Create mapping for a stock traded on multiple exchanges
        multi_exchange_mapping = SymbolMapping(
            isin="DE0007164600",
            base_symbol="SAP.DE",
            base_exchange="XETRA",
            base_country="DE",
            base_currency=CurrencyCode.EUR,
            company_name="SAP SE",
            sector="Software",
            market_cap_usd=Decimal("150000000000"),
            exchanges={
                "XETRA": ExchangeInfo(
                    symbol="SAP.DE",
                    exchange="XETRA",
                    country="DE",
                    currency=CurrencyCode.EUR,
                    trading_hours="09:00-17:30 CET",
                    lot_size=1,
                    tick_size=Decimal("0.01")
                ),
                "NYSE": ExchangeInfo(
                    symbol="SAP",
                    exchange="NYSE",
                    country="US",
                    currency=CurrencyCode.USD,
                    trading_hours="09:30-16:00 EST",
                    lot_size=1,
                    tick_size=Decimal("0.01")
                ),
                "LSE": ExchangeInfo(
                    symbol="SAP.L",
                    exchange="LSE",
                    country="GB",
                    currency=CurrencyCode.GBP,
                    trading_hours="08:00-16:30 GMT",
                    lot_size=10,
                    tick_size=Decimal("0.01")
                )
            }
        )
        
        # Mock database return
        production_like_mapper._database_mapper._repository.find_by_symbol.return_value = [multi_exchange_mapping]
        
        # Simulate arbitrage analysis
        results = await production_like_mapper.get_equivalent_symbols("SAP.DE")
        assert len(results) == 1
        
        mapping = results[0]
        
        # Verify multi-exchange data
        assert len(mapping.exchanges) == 3
        assert "XETRA" in mapping.exchanges
        assert "NYSE" in mapping.exchanges
        assert "LSE" in mapping.exchanges
        
        # Verify currency diversity for arbitrage
        currencies = mapping.get_all_currencies()
        expected_currencies = {CurrencyCode.EUR, CurrencyCode.USD, CurrencyCode.GBP}
        assert currencies == expected_currencies
        
        # Test exchange-specific currency lookup
        assert mapping.get_currency_for_exchange("XETRA") == CurrencyCode.EUR
        assert mapping.get_currency_for_exchange("NYSE") == CurrencyCode.USD
        assert mapping.get_currency_for_exchange("LSE") == CurrencyCode.GBP

    @pytest.mark.asyncio
    async def test_data_provider_failover_scenario(self, production_like_mapper):
        """Test scenario where primary data provider fails and system uses fallback."""
        # Simulate primary provider failure
        production_like_mapper._database_mapper._repository.find_by_symbol.return_value = []
        production_like_mapper._external_mapper._http_client.get.side_effect = [
            Exception("Primary API down"),  # First attempt fails
            Exception("Primary API down"),  # Retry fails
            Exception("Primary API down"),  # Max retries exceeded
        ]
        
        # Test graceful degradation
        results = await production_like_mapper.get_equivalent_symbols("AAPL")
        
        # Should handle failure gracefully
        assert results == []
        
        # Verify retry logic was attempted (3 retries + 1 initial = 4 total)
        assert production_like_mapper._external_mapper._http_client.get.call_count == 4

    @pytest.mark.asyncio
    async def test_data_freshness_and_staleness_scenario(self, production_like_mapper):
        """Test scenario involving data freshness and handling of stale data."""
        # This would test cache invalidation, data refresh cycles, etc.
        # For now, test manual refresh capability
        
        # Mock external API with fresh data
        from unittest.mock import Mock
        fresh_data_response = {
            "isin": "US0378331005",
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "sector": "Technology",
            "market_cap": "3000000000000",  # Updated market cap
            "base_currency": "USD",
            "last_updated": "2024-01-15T10:00:00Z"
        }
        
        production_like_mapper._external_mapper._http_client.get.return_value = Mock(
            status_code=200,
            json=Mock(return_value=fresh_data_response)
        )
        
        # Mock successful database update
        updated_mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology",
            market_cap_usd=Decimal("3000000000000")
        )
        production_like_mapper._database_mapper._repository.update.return_value = updated_mapping
        
        # Test refresh operation
        refresh_success = await production_like_mapper.refresh_cache("AAPL")
        
        assert refresh_success is True
        
        # Verify external API was called for fresh data
        production_like_mapper._external_mapper._http_client.get.assert_called_once()
        
        # Verify database update was performed
        production_like_mapper._database_mapper._repository.update.assert_called_once()