"""Hybrid symbol mapping implementation combining database and external sources."""

from typing import Any

from portfolio_manager.domain.services.symbol_mapping import (
    SymbolMapping,
    SymbolMappingService,
)


class HybridSymbolMapper(SymbolMappingService):
    """Hybrid symbol mapper combining database cache and external API."""

    def __init__(
        self,
        database_mapper: SymbolMappingService,
        external_mapper: SymbolMappingService,
        cache_duration_hours: int = 24,
        fallback_to_external: bool = True,
    ) -> None:
        """Initialize with database and external mappers."""
        if cache_duration_hours < 0:
            raise ValueError("Cache duration must be non-negative")

        self._database_mapper = database_mapper
        self._external_mapper = external_mapper
        self._cache_duration_hours = cache_duration_hours
        self._fallback_to_external = fallback_to_external

    async def get_equivalent_symbols(self, symbol: str) -> list[SymbolMapping]:
        """Get equivalent symbols using hybrid approach."""
        if not symbol or not isinstance(symbol, str):
            return []

        # Try database first
        try:
            db_results: list[SymbolMapping] = (
                await self._database_mapper.get_equivalent_symbols(symbol)
            )
            if db_results:
                return db_results
        except Exception:
            pass  # Continue to external fallback

        # If not in database and fallback is enabled, try external API
        if self._fallback_to_external:
            try:
                external_results: list[SymbolMapping] = (
                    await self._external_mapper.get_equivalent_symbols(symbol)
                )
                if external_results:
                    # Cache the results for future use
                    await self._cache_mappings(external_results)
                    return external_results
            except Exception:
                pass  # Return empty if external also fails

        return []

    async def get_provider_symbol(self, symbol: str, provider: str) -> str | None:
        """Get provider symbol using hybrid approach."""
        if (
            not symbol
            or not provider
            or not isinstance(symbol, str)
            or not isinstance(provider, str)
        ):
            return None

        # Try database first
        try:
            db_result: str | None = await self._database_mapper.get_provider_symbol(
                symbol, provider
            )
            if db_result:
                return db_result
        except Exception:
            pass  # Continue to external fallback

        # If not in database and fallback is enabled, try external API
        if self._fallback_to_external:
            try:
                external_result: str | None = (
                    await self._external_mapper.get_provider_symbol(symbol, provider)
                )
                if external_result:
                    # Try to cache the full mapping if we can get it
                    try:
                        full_mappings = (
                            await self._external_mapper.get_equivalent_symbols(symbol)
                        )
                        if full_mappings:
                            await self._cache_mappings(full_mappings)
                    except Exception:
                        pass  # Don't fail if caching fails

                    return str(external_result)
            except Exception:
                pass  # Return None if external also fails

        return None

    async def search_by_company(self, company_name: str) -> list[SymbolMapping]:
        """Search by company using hybrid approach."""
        if not company_name or not isinstance(company_name, str):
            return []

        # Try database first
        try:
            db_results: list[SymbolMapping] = (
                await self._database_mapper.search_by_company(company_name)
            )
            if db_results:
                return db_results
        except Exception:
            pass  # Continue to external fallback

        # If not in database and fallback is enabled, try external API
        if self._fallback_to_external:
            try:
                external_results: list[SymbolMapping] = (
                    await self._external_mapper.search_by_company(company_name)
                )
                if external_results:
                    # Cache the results for future use
                    await self._cache_mappings(external_results)
                    return list(external_results)
            except Exception:
                pass  # Return empty if external also fails

        return []

    async def refresh_cache(self, symbol: str) -> bool:
        """Manually refresh cache for a symbol."""
        if not symbol or not isinstance(symbol, str):
            return False

        try:
            # Get fresh data from external API
            external_results = await self._external_mapper.get_equivalent_symbols(
                symbol
            )
            if external_results:
                # Update database cache
                for mapping in external_results:
                    await self._database_mapper.update_mapping(mapping)
                return True
            else:
                return False
        except Exception:
            return False

    async def batch_refresh_cache(self, symbols: list[str]) -> dict[str, bool]:
        """Refresh cache for multiple symbols."""
        results = {}
        for symbol in symbols:
            results[symbol] = await self.refresh_cache(symbol)
        return results

    async def add_mapping(self, mapping: SymbolMapping) -> SymbolMapping | None:
        """Add a new symbol mapping to the database cache."""
        try:
            result: SymbolMapping | None = await self._database_mapper.add_mapping(mapping)
            return result
        except Exception:
            return None

    async def update_mapping(self, mapping: SymbolMapping) -> SymbolMapping | None:
        """Update an existing symbol mapping in the database cache."""
        try:
            result: SymbolMapping | None = await self._database_mapper.update_mapping(mapping)
            return result
        except Exception:
            return None

    async def clear_cache(self) -> bool:
        """Clear the database cache."""
        try:
            result: bool = await self._database_mapper.clear_cache()
            return result
        except Exception:
            return False

    async def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        try:
            if hasattr(self._database_mapper, "get_cache_stats"):
                result: dict[str, Any] = await self._database_mapper.get_cache_stats()
                return result if result else {}
            else:
                return {"error": "Cache stats not available"}
        except Exception:
            return {"error": "Failed to get cache stats"}

    async def _cache_mappings(self, mappings: list[SymbolMapping]) -> None:
        """Cache mappings in database."""
        for mapping in mappings:
            try:
                await self._database_mapper.add_mapping(mapping)
            except Exception:
                # Don't fail the whole operation if one mapping fails to cache
                continue
