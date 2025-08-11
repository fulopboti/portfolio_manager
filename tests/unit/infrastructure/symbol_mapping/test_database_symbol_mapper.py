"""Unit tests for DatabaseSymbolMapper."""

import pytest
from unittest.mock import AsyncMock, Mock
from decimal import Decimal
from datetime import datetime, timezone

from portfolio_manager.infrastructure.symbol_mapping.database_symbol_mapper import DatabaseSymbolMapper
from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping
)
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError


class TestDatabaseSymbolMapper:
    """Test cases for DatabaseSymbolMapper."""

    @pytest.fixture
    def mock_repository(self):
        """Create mock symbol mapping repository."""
        mock_repo = Mock()
        
        # Setup mock methods
        mock_repo.find_by_symbol = AsyncMock()
        mock_repo.find_by_isin = AsyncMock()
        mock_repo.find_by_company_name = AsyncMock()
        mock_repo.find_provider_symbol = AsyncMock()
        mock_repo.create = AsyncMock()
        mock_repo.update = AsyncMock()
        mock_repo.delete = AsyncMock()
        mock_repo.list_all = AsyncMock()
        
        return mock_repo

    @pytest.fixture
    def mapper(self, mock_repository):
        """Create DatabaseSymbolMapper with mock repository."""
        return DatabaseSymbolMapper(repository=mock_repository)

    @pytest.fixture
    def sample_mapping(self):
        """Create sample SymbolMapping for testing."""
        exchanges = {
            "XETRA": ExchangeInfo(
                symbol="APC.DE",
                exchange="XETRA",
                country="DE",
                currency=CurrencyCode.EUR,
                trading_hours="09:00-17:30 CET"
            ),
            "LSE": ExchangeInfo(
                symbol="APC.L",
                exchange="LSE",
                country="GB",
                currency=CurrencyCode.GBP,
                trading_hours="08:00-16:30 GMT"
            )
        }
        
        providers = {
            "yfinance": ProviderInfo(symbol="AAPL", provider="yfinance"),
            "mock": ProviderInfo(symbol="AAPL", provider="mock")
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
    async def test_get_equivalent_symbols_found(self, mapper, mock_repository, sample_mapping):
        """Test getting equivalent symbols when found."""
        mock_repository.find_by_symbol.return_value = [sample_mapping]
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        mock_repository.find_by_symbol.assert_called_once_with("AAPL")

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_not_found(self, mapper, mock_repository):
        """Test getting equivalent symbols when not found."""
        mock_repository.find_by_symbol.return_value = []
        
        result = await mapper.get_equivalent_symbols("UNKNOWN")
        
        assert result == []
        mock_repository.find_by_symbol.assert_called_once_with("UNKNOWN")

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_repository_error(self, mapper, mock_repository):
        """Test getting equivalent symbols with repository error."""
        mock_repository.find_by_symbol.side_effect = DataAccessError("Database error")
        
        result = await mapper.get_equivalent_symbols("AAPL")
        
        assert result == []  # Should return empty list on error

    @pytest.mark.asyncio
    async def test_get_provider_symbol_found(self, mapper, mock_repository):
        """Test getting provider symbol when found."""
        mock_repository.find_provider_symbol.return_value = "AAPL"
        
        result = await mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result == "AAPL"
        mock_repository.find_provider_symbol.assert_called_once_with("AAPL", "yfinance")

    @pytest.mark.asyncio
    async def test_get_provider_symbol_not_found(self, mapper, mock_repository):
        """Test getting provider symbol when not found."""
        mock_repository.find_provider_symbol.return_value = None
        
        result = await mapper.get_provider_symbol("UNKNOWN", "yfinance")
        
        assert result is None
        mock_repository.find_provider_symbol.assert_called_once_with("UNKNOWN", "yfinance")

    @pytest.mark.asyncio
    async def test_get_provider_symbol_repository_error(self, mapper, mock_repository):
        """Test getting provider symbol with repository error."""
        mock_repository.find_provider_symbol.side_effect = DataAccessError("Database error")
        
        result = await mapper.get_provider_symbol("AAPL", "yfinance")
        
        assert result is None  # Should return None on error

    @pytest.mark.asyncio
    async def test_search_by_company_found(self, mapper, mock_repository, sample_mapping):
        """Test searching by company name when found."""
        mock_repository.find_by_company_name.return_value = [sample_mapping]
        
        result = await mapper.search_by_company("Apple Inc.")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        mock_repository.find_by_company_name.assert_called_once_with("apple inc.")

    @pytest.mark.asyncio
    async def test_search_by_company_case_insensitive(self, mapper, mock_repository, sample_mapping):
        """Test searching by company name is case insensitive."""
        mock_repository.find_by_company_name.return_value = [sample_mapping]
        
        result = await mapper.search_by_company("APPLE INC.")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        # Should convert to lowercase for search
        mock_repository.find_by_company_name.assert_called_once_with("apple inc.")

    @pytest.mark.asyncio
    async def test_search_by_company_not_found(self, mapper, mock_repository):
        """Test searching by company name when not found."""
        mock_repository.find_by_company_name.return_value = []
        
        result = await mapper.search_by_company("Unknown Company")
        
        assert result == []
        mock_repository.find_by_company_name.assert_called_once_with("unknown company")

    @pytest.mark.asyncio
    async def test_search_by_company_repository_error(self, mapper, mock_repository):
        """Test searching by company with repository error."""
        mock_repository.find_by_company_name.side_effect = DataAccessError("Database error")
        
        result = await mapper.search_by_company("Apple Inc.")
        
        assert result == []  # Should return empty list on error

    @pytest.mark.asyncio
    async def test_get_by_isin_found(self, mapper, mock_repository, sample_mapping):
        """Test getting mapping by ISIN when found."""
        mock_repository.find_by_isin.return_value = sample_mapping
        
        result = await mapper.get_by_isin("US0378331005")
        
        assert result == sample_mapping
        mock_repository.find_by_isin.assert_called_once_with("US0378331005")

    @pytest.mark.asyncio
    async def test_get_by_isin_not_found(self, mapper, mock_repository):
        """Test getting mapping by ISIN when not found."""
        mock_repository.find_by_isin.return_value = None
        
        result = await mapper.get_by_isin("UNKNOWN")
        
        assert result is None
        mock_repository.find_by_isin.assert_called_once_with("UNKNOWN")

    @pytest.mark.asyncio
    async def test_get_by_isin_repository_error(self, mapper, mock_repository):
        """Test getting mapping by ISIN with repository error."""
        mock_repository.find_by_isin.side_effect = DataAccessError("Database error")
        
        result = await mapper.get_by_isin("US0378331005")
        
        assert result is None  # Should return None on error

    @pytest.mark.asyncio
    async def test_add_mapping_success(self, mapper, mock_repository, sample_mapping):
        """Test adding mapping successfully."""
        mock_repository.create.return_value = sample_mapping
        
        result = await mapper.add_mapping(sample_mapping)
        
        assert result == sample_mapping
        mock_repository.create.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_add_mapping_repository_error(self, mapper, mock_repository, sample_mapping):
        """Test adding mapping with repository error."""
        mock_repository.create.side_effect = DataAccessError("Database error")
        
        result = await mapper.add_mapping(sample_mapping)
        
        assert result is None  # Should return None on error

    @pytest.mark.asyncio
    async def test_update_mapping_success(self, mapper, mock_repository, sample_mapping):
        """Test updating mapping successfully."""
        mock_repository.update.return_value = sample_mapping
        
        result = await mapper.update_mapping(sample_mapping)
        
        assert result == sample_mapping
        mock_repository.update.assert_called_once_with(sample_mapping)

    @pytest.mark.asyncio
    async def test_update_mapping_repository_error(self, mapper, mock_repository, sample_mapping):
        """Test updating mapping with repository error."""
        mock_repository.update.side_effect = DataAccessError("Database error")
        
        result = await mapper.update_mapping(sample_mapping)
        
        assert result is None  # Should return None on error

    @pytest.mark.asyncio
    async def test_delete_mapping_success(self, mapper, mock_repository):
        """Test deleting mapping successfully."""
        mock_repository.delete.return_value = True
        
        result = await mapper.delete_mapping("US0378331005")
        
        assert result is True
        mock_repository.delete.assert_called_once_with("US0378331005")

    @pytest.mark.asyncio
    async def test_delete_mapping_not_found(self, mapper, mock_repository):
        """Test deleting mapping when not found."""
        mock_repository.delete.return_value = False
        
        result = await mapper.delete_mapping("UNKNOWN")
        
        assert result is False
        mock_repository.delete.assert_called_once_with("UNKNOWN")

    @pytest.mark.asyncio
    async def test_delete_mapping_repository_error(self, mapper, mock_repository):
        """Test deleting mapping with repository error."""
        mock_repository.delete.side_effect = DataAccessError("Database error")
        
        result = await mapper.delete_mapping("US0378331005")
        
        assert result is False  # Should return False on error

    @pytest.mark.asyncio
    async def test_list_all_mappings_success(self, mapper, mock_repository, sample_mapping):
        """Test listing all mappings successfully."""
        mock_repository.list_all.return_value = [sample_mapping]
        
        result = await mapper.list_all_mappings()
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        mock_repository.list_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_all_mappings_empty(self, mapper, mock_repository):
        """Test listing all mappings when empty."""
        mock_repository.list_all.return_value = []
        
        result = await mapper.list_all_mappings()
        
        assert result == []
        mock_repository.list_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_all_mappings_repository_error(self, mapper, mock_repository):
        """Test listing all mappings with repository error."""
        mock_repository.list_all.side_effect = DataAccessError("Database error")
        
        result = await mapper.list_all_mappings()
        
        assert result == []  # Should return empty list on error

    @pytest.mark.asyncio
    async def test_get_mappings_by_currency_success(self, mapper, mock_repository, sample_mapping):
        """Test getting mappings by currency successfully."""
        mock_repository.find_by_currency = AsyncMock(return_value=[sample_mapping])
        
        result = await mapper.get_mappings_by_currency(CurrencyCode.USD)
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        mock_repository.find_by_currency.assert_called_once_with(CurrencyCode.USD)

    @pytest.mark.asyncio
    async def test_get_mappings_by_exchange_success(self, mapper, mock_repository, sample_mapping):
        """Test getting mappings by exchange successfully."""
        mock_repository.find_by_exchange = AsyncMock(return_value=[sample_mapping])
        
        result = await mapper.get_mappings_by_exchange("NASDAQ")
        
        assert len(result) == 1
        assert result[0] == sample_mapping
        mock_repository.find_by_exchange.assert_called_once_with("NASDAQ")

    def test_initialization_with_custom_repository(self, mock_repository):
        """Test mapper initialization with custom repository."""
        mapper = DatabaseSymbolMapper(repository=mock_repository)
        assert mapper._repository == mock_repository

    def test_initialization_without_repository(self):
        """Test mapper initialization without repository raises appropriate error."""
        with pytest.raises(TypeError):
            DatabaseSymbolMapper()  # Should require repository parameter


class TestDatabaseSymbolMapperEdgeCases:
    """Test edge cases for DatabaseSymbolMapper."""

    @pytest.fixture
    def mapper(self):
        """Create mapper with mock repository for edge case testing."""
        mock_repo = Mock()
        mock_repo.find_by_symbol = AsyncMock()
        mock_repo.find_by_isin = AsyncMock()
        mock_repo.find_by_company_name = AsyncMock()
        mock_repo.find_provider_symbol = AsyncMock()
        return DatabaseSymbolMapper(repository=mock_repo)

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_empty_symbol(self, mapper):
        """Test getting equivalent symbols with empty symbol."""
        result = await mapper.get_equivalent_symbols("")
        assert result == []
        
        # Should not call repository with empty symbol
        mapper._repository.find_by_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_equivalent_symbols_none_symbol(self, mapper):
        """Test getting equivalent symbols with None symbol."""
        result = await mapper.get_equivalent_symbols(None)
        assert result == []
        
        # Should not call repository with None symbol
        mapper._repository.find_by_symbol.assert_not_called()

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
        
        # Should not call repository with invalid parameters
        mapper._repository.find_provider_symbol.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_empty_name(self, mapper):
        """Test searching by company with empty name."""
        result = await mapper.search_by_company("")
        assert result == []
        
        # Should not call repository with empty name
        mapper._repository.find_by_company_name.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_by_company_none_name(self, mapper):
        """Test searching by company with None name."""
        result = await mapper.search_by_company(None)
        assert result == []
        
        # Should not call repository with None name
        mapper._repository.find_by_company_name.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_by_isin_empty_isin(self, mapper):
        """Test getting mapping by empty ISIN."""
        result = await mapper.get_by_isin("")
        assert result is None
        
        # Should not call repository with empty ISIN
        mapper._repository.find_by_isin.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_by_isin_none_isin(self, mapper):
        """Test getting mapping by None ISIN."""
        result = await mapper.get_by_isin(None)
        assert result is None
        
        # Should not call repository with None ISIN
        mapper._repository.find_by_isin.assert_not_called()

    @pytest.mark.asyncio
    async def test_add_mapping_none_mapping(self, mapper):
        """Test adding None mapping."""
        result = await mapper.add_mapping(None)
        assert result is None
        
        # Should not call repository with None mapping
        mapper._repository.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_mapping_none_mapping(self, mapper):
        """Test updating None mapping."""
        result = await mapper.update_mapping(None)
        assert result is None
        
        # Should not call repository with None mapping
        mapper._repository.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_mapping_empty_isin(self, mapper):
        """Test deleting mapping with empty ISIN."""
        result = await mapper.delete_mapping("")
        assert result is False
        
        # Should not call repository with empty ISIN
        mapper._repository.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_mapping_none_isin(self, mapper):
        """Test deleting mapping with None ISIN."""
        result = await mapper.delete_mapping(None)
        assert result is False
        
        # Should not call repository with None ISIN
        mapper._repository.delete.assert_not_called()