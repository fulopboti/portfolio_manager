"""External API-based symbol mapping implementation."""

import asyncio
from decimal import Decimal, InvalidOperation
from typing import Any

from portfolio_manager.domain.services.symbol_mapping import (
    CurrencyCode,
    ExchangeInfo,
    ProviderInfo,
    SymbolMapping,
    SymbolMappingService,
)


class ExternalSymbolMapper(SymbolMappingService):
    """External API-backed symbol mapping service."""

    def __init__(
        self,
        api_base_url: str,
        api_key: str,
        http_client: Any,
        request_timeout: float = 10.0,
        max_retries: int = 2,
    ) -> None:
        """Initialize with API configuration."""
        self._api_base_url = api_base_url
        self._api_key = api_key
        self._http_client = http_client
        self._request_timeout = request_timeout
        self._max_retries = max_retries

    async def get_equivalent_symbols(self, symbol: str) -> list[SymbolMapping]:
        """Get equivalent symbols from external API."""
        if not symbol or not isinstance(symbol, str):
            return []

        url = f"{self._api_base_url}/symbols/{symbol}"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for attempt in range(self._max_retries + 1):
            try:
                response = await self._http_client.get(
                    url, headers=headers, timeout=self._request_timeout
                )

                if response.status_code == 200:
                    data = response.json()
                    mapping = self._parse_symbol_mapping(data)
                    return [mapping] if mapping else []
                elif response.status_code == 404:
                    return []
                else:
                    # API error, try next attempt if available
                    if attempt < self._max_retries:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return []

            except Exception:
                if attempt < self._max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return []

        return []

    async def get_provider_symbol(self, symbol: str, provider: str) -> str | None:
        """Get provider-specific symbol from external API."""
        if (
            not symbol
            or not provider
            or not isinstance(symbol, str)
            or not isinstance(provider, str)
        ):
            return None

        url = f"{self._api_base_url}/symbols/{symbol}/providers/{provider}"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for attempt in range(self._max_retries + 1):
            try:
                response = await self._http_client.get(
                    url, headers=headers, timeout=self._request_timeout
                )

                if response.status_code == 200:
                    data = response.json()
                    result = data.get("provider_symbol")
                    return str(result) if result is not None else None
                elif response.status_code == 404:
                    return None
                else:
                    if attempt < self._max_retries:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return None

            except Exception:
                if attempt < self._max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return None

        return None

    async def search_by_company(
        self, company_name: str, limit: int = 10
    ) -> list[SymbolMapping]:
        """Search symbols by company name via external API."""
        if not company_name or not isinstance(company_name, str):
            return []

        # Ensure limit is positive
        if limit <= 0:
            limit = 10

        url = f"{self._api_base_url}/search"
        params = {"company": company_name, "limit": limit}
        headers = {"Authorization": f"Bearer {self._api_key}"}

        try:
            response = await self._http_client.get(
                url, params=params, headers=headers, timeout=self._request_timeout
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])

                mappings = []
                for result in results:
                    mapping = self._parse_symbol_mapping(result)
                    if mapping:
                        mappings.append(mapping)

                return mappings
            else:
                return []

        except Exception:
            return []

    def _parse_symbol_mapping(self, data: dict[str, Any]) -> SymbolMapping | None:
        """Parse symbol mapping from API response data."""
        try:
            # Required fields
            required_fields = ["isin", "symbol", "company_name", "base_currency"]
            if not all(field in data for field in required_fields):
                return None

            # Parse exchanges
            exchanges = {}
            for exchange_data in data.get("exchanges", []):
                exchange_info = self._parse_exchange_info(exchange_data)
                if exchange_info:
                    exchanges[exchange_info.exchange] = exchange_info

            # Parse providers
            providers = {}
            for provider_data in data.get("providers", []):
                provider_info = self._parse_provider_info(provider_data)
                if provider_info:
                    providers[provider_info.provider] = provider_info

            return SymbolMapping(
                isin=data["isin"],
                base_symbol=data["symbol"],
                base_exchange=data.get("base_exchange", ""),
                base_country=data.get("base_country", ""),
                base_currency=self._parse_currency_code(data["base_currency"]),
                company_name=data["company_name"],
                sector=data.get("sector"),
                market_cap_usd=self._safe_decimal(data.get("market_cap")),
                exchanges=exchanges,
                providers=providers,
            )

        except Exception:
            return None

    def _parse_exchange_info(self, data: dict[str, Any]) -> ExchangeInfo | None:
        """Parse exchange information from API data."""
        try:
            required = ["symbol", "exchange", "country", "currency"]
            if not all(field in data for field in required):
                return None

            return ExchangeInfo(
                symbol=data["symbol"],
                exchange=data["exchange"],
                country=data["country"],
                currency=self._parse_currency_code(data["currency"]),
                trading_hours=data.get("trading_hours", ""),
                lot_size=data.get("lot_size", 1),
                tick_size=self._safe_decimal(data.get("tick_size", "0.01"))
                or Decimal("0.01"),
            )

        except Exception:
            return None

    def _parse_provider_info(self, data: dict[str, Any]) -> ProviderInfo | None:
        """Parse provider information from API data."""
        try:
            required = ["provider", "symbol"]
            if not all(field in data for field in required):
                return None

            return ProviderInfo(
                symbol=data["symbol"],
                provider=data["provider"],
                supports_fundamentals=data.get("supports_fundamentals", True),
                supports_realtime=data.get("supports_realtime", False),
            )

        except Exception:
            return None

    def _parse_currency_code(self, currency_str: str) -> CurrencyCode:
        """Parse currency code string to enum."""
        if not currency_str:
            return CurrencyCode.USD

        try:
            return CurrencyCode(currency_str.upper())
        except ValueError:
            return CurrencyCode.USD

    def _safe_decimal(self, value: Any) -> Decimal | None:
        """Safely convert value to Decimal."""
        if not value:
            return None

        try:
            return Decimal(str(value))
        except (ValueError, InvalidOperation):
            return None
