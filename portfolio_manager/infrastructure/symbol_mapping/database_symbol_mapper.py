"""Database-based symbol mapping implementation."""

from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    SymbolMapping,
    SymbolMappingService,
)


class DatabaseSymbolMapper(SymbolMappingService):
    """Database-backed symbol mapping service."""

    def __init__(self, repository):
        """Initialize with symbol mapping repository."""
        self._repository = repository

    async def get_equivalent_symbols(self, symbol: str) -> list[SymbolMapping]:
        """Get equivalent symbols from database."""
        if not symbol or not isinstance(symbol, str):
            return []

        try:
            return await self._repository.find_by_symbol(symbol)
        except Exception:
            return []

    async def get_provider_symbol(self, symbol: str, provider: str) -> str | None:
        """Get provider-specific symbol from database."""
        if (
            not symbol
            or not provider
            or not isinstance(symbol, str)
            or not isinstance(provider, str)
        ):
            return None

        try:
            return await self._repository.find_provider_symbol(symbol, provider)
        except Exception:
            return None

    async def search_by_company(self, company_name: str) -> list[SymbolMapping]:
        """Search symbols by company name in database."""
        if not company_name or not isinstance(company_name, str):
            return []

        try:
            # Convert to lowercase for case-insensitive search
            return await self._repository.find_by_company_name(company_name.lower())
        except Exception:
            return []

    async def get_by_isin(self, isin: str) -> SymbolMapping | None:
        """Get symbol mapping by ISIN."""
        if not isin or not isinstance(isin, str):
            return None

        try:
            return await self._repository.find_by_isin(isin)
        except Exception:
            return None

    async def add_mapping(self, mapping: SymbolMapping) -> SymbolMapping | None:
        """Add new symbol mapping to database."""
        if not mapping:
            return None

        try:
            return await self._repository.create(mapping)
        except Exception:
            return None

    async def update_mapping(self, mapping: SymbolMapping) -> SymbolMapping | None:
        """Update existing symbol mapping in database."""
        if not mapping:
            return None

        try:
            return await self._repository.update(mapping)
        except Exception:
            return None

    async def delete_mapping(self, isin: str) -> bool:
        """Delete symbol mapping from database."""
        if not isin or not isinstance(isin, str):
            return False

        try:
            return await self._repository.delete(isin)
        except Exception:
            return False

    async def list_all_mappings(self) -> list[SymbolMapping]:
        """List all symbol mappings."""
        try:
            return await self._repository.list_all()
        except Exception:
            return []

    async def clear_cache(self) -> bool:
        """Clear cached mappings."""
        try:
            return await self._repository.clear_cache()
        except Exception:
            return False

    async def get_mappings_by_currency(
        self, currency: CurrencyCode
    ) -> list[SymbolMapping]:
        """Get all mappings that trade in a specific currency."""
        if not currency:
            return []

        try:
            return await self._repository.find_by_currency(currency)
        except Exception:
            return []

    async def get_mappings_by_exchange(self, exchange: str) -> list[SymbolMapping]:
        """Get all mappings that trade on a specific exchange."""
        if not exchange or not isinstance(exchange, str):
            return []

        try:
            return await self._repository.find_by_exchange(exchange)
        except Exception:
            return []

    async def get_cache_stats(self) -> dict:
        """Get cache statistics from repository."""
        try:
            return await self._repository.get_cache_stats()
        except Exception:
            return {"error": "Cache stats unavailable"}
