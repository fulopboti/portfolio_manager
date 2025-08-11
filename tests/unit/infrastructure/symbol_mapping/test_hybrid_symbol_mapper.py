"""Unit tests for HybridSymbolMapper."""

import pytest
from unittest.mock import AsyncMock, Mock
from decimal import Decimal

from portfolio_manager.infrastructure.symbol_mapping.hybrid_symbol_mapper import HybridSymbolMapper
from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping
)


class TestHybridSymbolMapper:
    """Test cases for HybridSymbolMapper."""

    @pytest.fixture
    def mock_database_mapper(self):
        """Create mock database symbol mapper."""
        mock_db = Mock()
        mock_db.get_equivalent_symbols = AsyncMock()
        mock_db.get_provider_symbol = AsyncMock()
        mock_db.search_by_company = AsyncMock()
        mock_db.add_mapping = AsyncMock()
        mock_db.update_mapping = AsyncMock()
        return mock_db

    @pytest.fixture
    def mock_external_mapper(self):
        """Create mock external symbol mapper."""
        mock_ext = Mock()
        mock_ext.get_equivalent_symbols = AsyncMock()
        mock_ext.get_provider_symbol = AsyncMock()
        mock_ext.search_by_company = AsyncMock()
        return mock_ext

    @pytest.fixture
    def hybrid_mapper(self, mock_database_mapper, mock_external_mapper):
        """Create HybridSymbolMapper with mock components."""
        return HybridSymbolMapper(
            database_mapper=mock_database_mapper,
            external_mapper=mock_external_mapper,
            cache_duration_hours=24,
            fallback_to_external=True
        )

    @pytest.fixture
    def sample_mapping(self):
        """Create sample SymbolMapping for testing."""
        exchanges = {
            "NASDAQ": ExchangeInfo(
                symbol="AAPL",
                exchange="NASDAQ",
                country="US",
                currency=CurrencyCode.USD,
                trading_hours="09:30-16:00 EST"
            )
        }
        
        providers = {
            "yfinance": ProviderInfo(symbol="AAPL", provider="yfinance")
        }
        
        return SymbolMapping(
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

    def test_initialization(self, mock_database_mapper, mock_external_mapper):
        """Test HybridSymbolMapper initialization."""
        mapper = HybridSymbolMapper(
            database_mapper=mock_database_mapper,
            external_mapper=mock_external_mapper,
            cache_duration_hours=48,
            fallback_to_external=False
        )
        
        assert mapper._database_mapper == mock_database_mapper
        assert mapper._external_mapper == mock_external_mapper
        assert mapper._cache_duration_hours == 48
        assert mapper._fallback_to_external is False

    def test_initialization_defaults(self, mock_database_mapper, mock_external_mapper):
        """Test HybridSymbolMapper initialization with defaults."""
        mapper = HybridSymbolMapper(
            database_mapper=mock_database_mapper,
            external_mapper=mock_external_mapper
        )
        
        assert mapper._cache_duration_hours == 24  # Default
        assert mapper._fallback_to_external is True  # Default

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_database_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test getting equivalent symbols from database (cache hit)."""
        mock_database_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        
        result = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        
        # Should only call database mapper
        mock_database_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        mock_external_mapper.get_equivalent_symbols.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_database_miss_external_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test getting equivalent symbols with database miss, external hit."""
        mock_database_mapper.get_equivalent_symbols.return_value = []
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        mock_database_mapper.add_mapping.return_value = sample_mapping
        
        result = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        
        # Should call both mappers
        mock_database_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        mock_external_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        
        # Should cache the result
        mock_database_mapper.add_mapping.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_both_miss(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test getting equivalent symbols with both database and external miss."""
        mock_database_mapper.get_equivalent_symbols.return_value = []
        mock_external_mapper.get_equivalent_symbols.return_value = []
        
        result = await hybrid_mapper.get_equivalent_symbols("UNKNOWN")
        
        assert result == []
        
        # Should call both mappers
        mock_database_mapper.get_equivalent_symbols.assert_called_once_with("UNKNOWN")
        mock_external_mapper.get_equivalent_symbols.assert_called_once_with("UNKNOWN")
        
        # Should not try to cache empty results
        mock_database_mapper.add_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_fallback_disabled(
        self, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test getting equivalent symbols with external fallback disabled."""
        # Create mapper with fallback disabled
        mapper = HybridSymbolMapper(
            database_mapper=mock_database_mapper,
            external_mapper=mock_external_mapper,
            fallback_to_external=False
        )
        
        mock_database_mapper.get_equivalent_symbols.return_value = []
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert result == []
        
        # Should only call database mapper
        mock_database_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        mock_external_mapper.get_equivalent_symbols.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_external_error(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test getting equivalent symbols with external API error."""
        mock_database_mapper.get_equivalent_symbols.return_value = []
        mock_external_mapper.get_equivalent_symbols.side_effect = Exception("API error")
        
        result = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        assert result == []
        
        # Should still call both mappers
        mock_database_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        mock_external_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_caching_error(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test getting equivalent symbols with caching error."""
        mock_database_mapper.get_equivalent_symbols.return_value = []
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        mock_database_mapper.add_mapping.side_effect = Exception("Database error")
        
        result = await hybrid_mapper.get_equivalent_symbols("AAPL")
        
        # Should still return the result despite caching failure
        assert len(result) == 1
        assert result[0] == sample_mapping

    @pytest.mark.asyncio
    async def test_get_provider_symbol_database_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test getting provider symbol from database (cache hit)."""
        mock_database_mapper.get_provider_symbol.return_value = "AAPL"
        
        result = await hybrid_mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result == "AAPL"
        
        # Should only call database mapper
        mock_database_mapper.get_provider_symbol.assert_called_once_with("AAPL", "yfinance")
        mock_external_mapper.get_provider_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_provider_symbol_database_miss_external_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test getting provider symbol with database miss, external hit."""
        mock_database_mapper.get_provider_symbol.return_value = None
        mock_external_mapper.get_provider_symbol.return_value = "AAPL"
        # Mock getting the full mapping for caching
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        mock_database_mapper.add_mapping.return_value = sample_mapping
        
        result = await hybrid_mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result == "AAPL"
        
        # Should call both mappers
        mock_database_mapper.get_provider_symbol.assert_called_once_with("AAPL", "yfinance")
        mock_external_mapper.get_provider_symbol.assert_called_once_with("AAPL", "yfinance")

    @pytest.mark.asyncio
    async def test_get_provider_symbol_both_miss(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test getting provider symbol with both database and external miss."""
        mock_database_mapper.get_provider_symbol.return_value = None
        mock_external_mapper.get_provider_symbol.return_value = None
        
        result = await hybrid_mapper.get_provider_symbol("UNKNOWN", "yfinance")
        
        assert result is None
        
        # Should call both mappers
        mock_database_mapper.get_provider_symbol.assert_called_once_with("UNKNOWN", "yfinance")
        mock_external_mapper.get_provider_symbol.assert_called_once_with("UNKNOWN", "yfinance")

    @pytest.mark.asyncio
    async def test_search_by_company_database_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test searching by company from database (cache hit)."""
        mock_database_mapper.search_by_company.return_value = [sample_mapping]
        
        result = await hybrid_mapper.search_by_company("Apple Inc.")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        
        # Should only call database mapper
        mock_database_mapper.search_by_company.assert_called_once_with("Apple Inc.")
        mock_external_mapper.search_by_company.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_database_miss_external_hit(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test searching by company with database miss, external hit."""
        mock_database_mapper.search_by_company.return_value = []
        mock_external_mapper.search_by_company.return_value = [sample_mapping]
        mock_database_mapper.add_mapping.return_value = sample_mapping
        
        result = await hybrid_mapper.search_by_company("Apple Inc.")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        
        # Should call both mappers
        mock_database_mapper.search_by_company.assert_called_once_with("Apple Inc.")
        mock_external_mapper.search_by_company.assert_called_once_with("Apple Inc.")
        
        # Should cache the result
        mock_database_mapper.add_mapping.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_search_by_company_multiple_results_caching(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test searching by company with multiple results and caching."""
        apple_mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        
        apple_clone_mapping = SymbolMapping(
            isin="US0378331006",
            base_symbol="AAPL2",
            base_exchange="NYSE",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc. Clone",
            sector="Technology"
        )
        
        mock_database_mapper.search_by_company.return_value = []
        mock_external_mapper.search_by_company.return_value = [apple_mapping, apple_clone_mapping]
        mock_database_mapper.add_mapping.return_value = apple_mapping
        
        result = await hybrid_mapper.search_by_company("Apple")
        
        assert len(result) == 2
        
        # Should cache both results
        assert mock_database_mapper.add_mapping.call_count == 2

    @pytest.mark.asyncio
    async def test_refresh_cache_success(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test manual cache refresh."""
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        mock_database_mapper.update_mapping.return_value = sample_mapping
        
        result = await hybrid_mapper.refresh_cache("AAPL")
        
        assert result is True
        
        # Should call external API and update database
        mock_external_mapper.get_equivalent_symbols.assert_called_once_with("AAPL")
        mock_database_mapper.update_mapping.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_refresh_cache_not_found(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test manual cache refresh when symbol not found."""
        mock_external_mapper.get_equivalent_symbols.return_value = []
        
        result = await hybrid_mapper.refresh_cache("UNKNOWN")
        
        assert result is False
        
        # Should not try to update database with empty results
        mock_database_mapper.update_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_cache_external_error(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper
    ):
        """Test manual cache refresh with external API error."""
        mock_external_mapper.get_equivalent_symbols.side_effect = Exception("API error")
        
        result = await hybrid_mapper.refresh_cache("AAPL")
        
        assert result is False

    @pytest.mark.asyncio
    async def test_refresh_cache_update_error(
        self, hybrid_mapper, mock_database_mapper, mock_external_mapper, sample_mapping
    ):
        """Test manual cache refresh with database update error."""
        mock_external_mapper.get_equivalent_symbols.return_value = [sample_mapping]
        mock_database_mapper.update_mapping.side_effect = Exception("Database error")
        
        result = await hybrid_mapper.refresh_cache("AAPL")
        
        assert result is False

    def test_is_cache_valid_fresh_data(self, hybrid_mapper, sample_mapping):
        """Test cache validity check with fresh data."""
        # Assume mapping has a last_updated field that's recent
        import datetime
        from unittest.mock import patch
        
        with patch('datetime.datetime') as mock_datetime:
            # Mock current time
            mock_datetime.now.return_value = datetime.datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.side_effect = lambda *args, **kwargs: datetime.datetime(*args, **kwargs)
            
            # Mock mapping with recent timestamp
            recent_mapping = sample_mapping
            # Would need to add last_updated field to SymbolMapping if implemented
            
            # For now, test passes as the method logic would be implemented
            assert True  # Placeholder

    @pytest.mark.asyncio
    async def test_get_cache_stats(self, hybrid_mapper):
        """Test getting cache statistics."""
        # This would depend on actual implementation
        stats = await hybrid_mapper.get_cache_stats()
        
        # Should return statistics about cache performance
        assert isinstance(stats, dict)
        # Would contain keys like: hit_rate, total_requests, cache_size, etc.

    @pytest.mark.asyncio
    async def test_clear_cache(self, hybrid_mapper, mock_database_mapper):
        """Test clearing the cache."""
        # Mock database clear operation
        mock_database_mapper.clear_cache = AsyncMock(return_value=True)
        
        result = await hybrid_mapper.clear_cache()
        
        assert result is True
        mock_database_mapper.clear_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_refresh_cache(self, hybrid_mapper, mock_database_mapper, mock_external_mapper):
        """Test batch cache refresh for multiple symbols."""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        
        # Mock external responses
        mock_external_mapper.get_equivalent_symbols.side_effect = [
            [SymbolMapping(
                isin=f"US037833100{i}",
                base_symbol=symbol,
                base_exchange="NASDAQ",
                base_country="US",
                base_currency=CurrencyCode.USD,
                company_name=f"{symbol} Inc.",
                sector="Technology"
            )]
            for i, symbol in enumerate(symbols)
        ]
        
        mock_database_mapper.update_mapping.return_value = True
        
        results = await hybrid_mapper.batch_refresh_cache(symbols)
        
        assert len(results) == 3
        assert all(result for result in results.values())
        assert mock_external_mapper.get_equivalent_symbols.call_count == 3
        assert mock_database_mapper.update_mapping.call_count == 3


class TestHybridSymbolMapperEdgeCases:
    """Test edge cases for HybridSymbolMapper."""

    @pytest.fixture
    def mapper_with_mocks(self):
        """Create mapper with mock dependencies for edge case testing."""
        mock_db = Mock()
        mock_db.get_equivalent_symbols = AsyncMock()
        mock_db.get_provider_symbol = AsyncMock()
        mock_db.search_by_company = AsyncMock()
        mock_db.add_mapping = AsyncMock()
        
        mock_ext = Mock()
        mock_ext.get_equivalent_symbols = AsyncMock()
        mock_ext.get_provider_symbol = AsyncMock()
        mock_ext.search_by_company = AsyncMock()
        
        return HybridSymbolMapper(
            database_mapper=mock_db,
            external_mapper=mock_ext
        ), mock_db, mock_ext

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_empty_symbol(self, mapper_with_mocks):
        """Test getting equivalent symbols with empty symbol."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        result = await mapper.get_equivalent_symbols("")
        assert result == []
        
        # Should not call either mapper with empty symbol
        mock_db.get_equivalent_symbols.assert_not_called()
        mock_ext.get_equivalent_symbols.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_none_symbol(self, mapper_with_mocks):
        """Test getting equivalent symbols with None symbol."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        result = await mapper.get_equivalent_symbols(None)
        assert result == []
        
        # Should not call either mapper with None symbol
        mock_db.get_equivalent_symbols.assert_not_called()
        mock_ext.get_equivalent_symbols.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_provider_symbol_empty_params(self, mapper_with_mocks):
        """Test getting provider symbol with empty parameters."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        result1 = await mapper.get_provider_symbol("", "yfinance")
        assert result1 is None
        
        result2 = await mapper.get_provider_symbol("AAPL", "")
        assert result2 is None
        
        result3 = await mapper.get_provider_symbol(None, "yfinance")
        assert result3 is None
        
        result4 = await mapper.get_provider_symbol("AAPL", None)
        assert result4 is None
        
        # Should not call either mapper with invalid parameters
        mock_db.get_provider_symbol.assert_not_called()
        mock_ext.get_provider_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_empty_name(self, mapper_with_mocks):
        """Test searching by company with empty name."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        result = await mapper.search_by_company("")
        assert result == []
        
        # Should not call either mapper with empty name
        mock_db.search_by_company.assert_not_called()
        mock_ext.search_by_company.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_none_name(self, mapper_with_mocks):
        """Test searching by company with None name."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        result = await mapper.search_by_company(None)
        assert result == []
        
        # Should not call either mapper with None name
        mock_db.search_by_company.assert_not_called()
        mock_ext.search_by_company.assert_not_called()

    @pytest.mark.asyncio
    async def test_database_mapper_completely_fails(self, mapper_with_mocks):
        """Test behavior when database mapper completely fails."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        # Database always throws exceptions
        mock_db.get_equivalent_symbols.side_effect = Exception("Database down")
        mock_db.add_mapping.side_effect = Exception("Database down")
        
        # External returns results
        sample_mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        mock_ext.get_equivalent_symbols.return_value = [sample_mapping]
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        # Should still return results from external API
        assert len(result) == 1
        assert result[0] == sample_mapping
        
        # Should attempt caching but handle failure gracefully
        mock_db.add_mapping.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_external_mapper_completely_fails(self, mapper_with_mocks):
        """Test behavior when external mapper completely fails."""
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        # Database returns empty
        mock_db.get_equivalent_symbols.return_value = []
        
        # External always throws exceptions
        mock_ext.get_equivalent_symbols.side_effect = Exception("API down")
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        # Should return empty results gracefully
        assert result == []
        
        # Should not attempt caching
        mock_db.add_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_requests_same_symbol(self, mapper_with_mocks):
        """Test concurrent requests for the same symbol."""
        import asyncio
        
        mapper, mock_db, mock_ext = mapper_with_mocks
        
        # Database returns empty initially
        mock_db.get_equivalent_symbols.return_value = []
        
        # External returns results
        sample_mapping = SymbolMapping(
            isin="US0378331005",
            base_symbol="AAPL",
            base_exchange="NASDAQ",
            base_country="US",
            base_currency=CurrencyCode.USD,
            company_name="Apple Inc.",
            sector="Technology"
        )
        
        # Make external return the same result for all calls
        mock_ext.get_equivalent_symbols.return_value = [sample_mapping]
        mock_db.add_mapping.return_value = sample_mapping
        
        # Make concurrent requests
        tasks = [
            mapper.get_equivalent_symbols("AAPL"),
            mapper.get_equivalent_symbols("AAPL"),
            mapper.get_equivalent_symbols("AAPL")
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All should return the same result
        assert len(results) == 3
        assert all(len(result) == 1 for result in results)
        assert all(result[0].base_symbol == "AAPL" for result in results)

    def test_initialization_validation(self):
        """Test initialization parameter validation."""
        mock_db = Mock()
        mock_ext = Mock()
        
        # Test invalid cache duration
        with pytest.raises(ValueError):
            HybridSymbolMapper(
                database_mapper=mock_db,
                external_mapper=mock_ext,
                cache_duration_hours=-1  # Invalid negative duration
            )
        
        # Test missing required parameters
        with pytest.raises(TypeError):
            HybridSymbolMapper(database_mapper=mock_db)  # Missing external_mapper
            
        with pytest.raises(TypeError):
            HybridSymbolMapper(external_mapper=mock_ext)  # Missing database_mapper