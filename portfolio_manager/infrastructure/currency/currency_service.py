"""Currency conversion and exchange rate service."""

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from portfolio_manager.domain.services.symbol_mapping import CurrencyCode


class CurrencyService:
    """Service for currency conversion and exchange rate management."""

    def __init__(
        self,
        exchange_rate_provider: Any,
        cache_repository: Any,
        cache_duration_minutes: int = 60,
        fallback_rates_file: str | None = None,
    ) -> None:
        """Initialize currency service with providers."""
        self._exchange_rate_provider = exchange_rate_provider
        self._cache_repository = cache_repository
        self._cache_duration_minutes = cache_duration_minutes
        self._fallback_rates_file = fallback_rates_file

    async def get_exchange_rate(
        self, from_currency: CurrencyCode | None, to_currency: CurrencyCode | None
    ) -> Decimal | None:
        """Get exchange rate between two currencies."""
        # Handle None currencies
        if from_currency is None or to_currency is None:
            return None
        
        # Same currency
        if from_currency == to_currency:
            return Decimal("1.0000")

        # Check cache first
        try:
            cached_rate: Decimal | None = await self._cache_repository.get_cached_rate(
                from_currency, to_currency
            )
            if cached_rate is not None:
                return cached_rate
        except Exception:
            pass  # Continue to provider

        # Check reverse rate in cache
        try:
            reverse_rate: Decimal | None = await self._cache_repository.get_cached_rate(
                to_currency, from_currency
            )
            if reverse_rate is not None and reverse_rate != Decimal("0"):
                forward_rate = Decimal("1.0000") / reverse_rate
                return forward_rate
        except Exception:
            pass  # Continue to provider

        # Get from provider
        try:
            provider_rate: Decimal | None = await self._exchange_rate_provider.get_exchange_rate(
                from_currency, to_currency
            )
            if provider_rate is not None:
                # Cache the result
                try:
                    await self._cache_repository.cache_rate(
                        from_currency, to_currency, provider_rate
                    )
                except Exception:
                    pass  # Don't fail if caching fails

                return provider_rate
        except Exception:
            pass  # Continue to fallback

        # Try fallback rates
        fallback_rate: Decimal | None = self._get_fallback_rate(from_currency, to_currency)
        if fallback_rate is not None:
            return fallback_rate

        return None

    async def convert_amount(
        self,
        amount: Decimal | None,
        from_currency: CurrencyCode | None,
        to_currency: CurrencyCode | None,
    ) -> Decimal | None:
        """Convert amount between currencies."""
        if amount is None or from_currency is None or to_currency is None:
            return None

        if amount == Decimal("0"):
            return Decimal("0.00")

        if from_currency == to_currency:
            return amount

        exchange_rate = await self.get_exchange_rate(from_currency, to_currency)
        if exchange_rate is not None:
            return amount * exchange_rate

        return None

    async def get_historical_rates(
        self,
        from_currency: CurrencyCode | None,
        to_currency: CurrencyCode | None,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[dict[str, Any]]:
        """Get historical exchange rates."""
        if not from_currency or not to_currency or not start_date or not end_date:
            return []

        if start_date > end_date:
            return []

        # Same currency - generate 1.0000 rates for date range
        if from_currency == to_currency:
            rates = []
            current_date = start_date
            while current_date <= end_date:
                rates.append(
                    {
                        "date": current_date.strftime("%Y-%m-%d"),
                        "rate": Decimal("1.0000"),
                    }
                )
                current_date += timedelta(days=1)
            return rates

        # Get from provider
        try:
            historical_rates = await self._exchange_rate_provider.get_historical_rates(
                from_currency, to_currency, start_date, end_date
            )

            # Convert rate strings to Decimals
            processed_rates = []
            for rate_data in historical_rates:
                try:
                    processed_rates.append(
                        {
                            "date": rate_data["date"],
                            "rate": Decimal(str(rate_data["rate"])),
                        }
                    )
                except (KeyError, InvalidOperation):
                    continue

            return processed_rates

        except Exception:
            return []

    async def get_supported_currencies(self) -> list[CurrencyCode]:
        """Get list of supported currencies."""
        try:
            currency_strings = (
                await self._exchange_rate_provider.get_supported_currencies()
            )
            currencies = []
            for curr_str in currency_strings:
                try:
                    currencies.append(CurrencyCode(curr_str))
                except ValueError:
                    continue
            return currencies
        except Exception:
            # Return default major currencies
            return [
                CurrencyCode.USD,
                CurrencyCode.EUR,
                CurrencyCode.GBP,
                CurrencyCode.JPY,
                CurrencyCode.CAD,
                CurrencyCode.AUD,
                CurrencyCode.CHF,
                CurrencyCode.CNY,
                CurrencyCode.HKD,
                CurrencyCode.SGD,
            ]

    async def batch_convert_amounts(
        self, conversion_requests: list[tuple[Decimal, CurrencyCode, CurrencyCode]]
    ) -> list[Decimal | None]:
        """Convert multiple amounts in batch."""
        results = []
        for amount, from_currency, to_currency in conversion_requests:
            result = await self.convert_amount(amount, from_currency, to_currency)
            results.append(result)
        return results

    async def clear_cache(self) -> bool:
        """Clear exchange rate cache."""
        try:
            result = await self._cache_repository.clear_cache()
            return bool(result)
        except Exception:
            return False

    async def cleanup_expired_cache(self) -> int:
        """Remove expired cache entries and return count."""
        try:
            result = await self._cache_repository.clear_expired_cache()
            return int(result) if result is not None else 0
        except Exception:
            return 0

    async def get_cache_stats(self) -> dict[str, Any]:
        """Get cache performance statistics."""
        try:
            result = await self._cache_repository.get_cache_stats()
            return dict(result) if result else {}
        except Exception:
            return {"error": "Cache stats unavailable"}

    def _load_fallback_rates(self) -> dict[str, str]:
        """Load fallback exchange rates from file."""
        if not self._fallback_rates_file or not os.path.exists(
            self._fallback_rates_file
        ):
            return {}

        try:
            with open(self._fallback_rates_file) as f:
                rates: dict[str, str] = json.load(f)
                return rates
        except (OSError, json.JSONDecodeError):
            return {}

    def _get_fallback_rate(
        self, from_currency: CurrencyCode | None, to_currency: CurrencyCode | None
    ) -> Decimal | None:
        """Get fallback exchange rate."""
        if from_currency is None or to_currency is None:
            return None
        fallback_rates = self._load_fallback_rates()

        # Try direct rate
        direct_key = f"{from_currency.value}_{to_currency.value}"
        if direct_key in fallback_rates:
            try:
                return Decimal(fallback_rates[direct_key])
            except InvalidOperation:
                pass

        # Try reverse rate
        reverse_key = f"{to_currency.value}_{from_currency.value}"
        if reverse_key in fallback_rates:
            try:
                reverse_rate = Decimal(fallback_rates[reverse_key])
                if reverse_rate != Decimal("0"):
                    return Decimal("1.0000") / reverse_rate
            except InvalidOperation:
                pass

        return None
