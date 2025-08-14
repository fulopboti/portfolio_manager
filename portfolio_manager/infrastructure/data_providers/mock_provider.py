"""Mock data provider for testing and development."""

import logging
from datetime import datetime
from decimal import Decimal

from portfolio_manager.application.ports import DataProvider
from portfolio_manager.domain.entities import AssetSnapshot


class MockDataProvider(DataProvider):
    """Mock data provider that generates synthetic market data."""

    def __init__(self, base_price: Decimal = Decimal("100.00")):
        """Initialize mock provider with base price for generation."""
        self.base_price = base_price
        self.logger = logging.getLogger(__name__)

    async def get_ohlcv_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> list[AssetSnapshot]:
        """Generate mock OHLCV data for the date range."""
        snapshots = []

        # Generate daily data points
        current_date = start_date
        price = self.base_price

        # Simple price walk simulation
        price_increment = Decimal("0.5")
        volume_base = 1000000

        day_count = 0
        while current_date <= end_date:
            # Add some variation to price
            daily_change = price_increment * Decimal(str((day_count % 10) - 5)) * Decimal("0.1")
            current_price = price + daily_change

            # Ensure positive price
            if current_price <= 0:
                current_price = self.base_price

            # Ensure positive prices for domain validation
            if current_price <= 0:
                current_price = self.base_price if self.base_price > 0 else Decimal("100.00")

            # Generate OHLCV
            open_price = current_price
            high_price = current_price * Decimal("1.02")  # 2% higher
            low_price = current_price * Decimal("0.98")   # 2% lower
            close_price = current_price * Decimal("1.005") # 0.5% up from open
            volume = volume_base + (day_count * 50000)

            snapshot = AssetSnapshot(
                symbol=symbol,
                timestamp=current_date,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume
            )
            snapshots.append(snapshot)

            # Move to next day
            current_date = current_date.replace(
                day=current_date.day + 1
            ) if current_date.day < 28 else current_date.replace(
                month=current_date.month + 1, day=1
            ) if current_date.month < 12 else current_date.replace(
                year=current_date.year + 1, month=1, day=1
            )

            day_count += 1
            price = close_price  # Update base price for next iteration

            # Prevent infinite loops and limit large date ranges
            if day_count > 1000:
                break

        return snapshots

    async def get_fundamental_data(self, symbol: str) -> dict:
        """Generate mock fundamental data."""
        # Generate basic fundamental metrics
        return {
            "pe_ratio": Decimal("15.5"),
            "pb_ratio": Decimal("2.3"),
            "dividend_yield": Decimal("0.025"),
            "market_cap": Decimal("50000000000"),
            "revenue": Decimal("10000000000"),
            "net_income": Decimal("1500000000"),
            "debt_to_equity": Decimal("0.4"),
            "current_ratio": Decimal("1.8"),
            "roe": Decimal("0.18"),
            "roa": Decimal("0.12")
        }

    def supports_symbol(self, symbol: str) -> bool:
        """Mock provider supports all symbols except explicit failures."""
        unsupported_symbols = ["INVALID", "UNSUPPORTED", "FAIL", "ERROR"]
        return symbol not in unsupported_symbols

    def get_provider_name(self) -> str:
        """Get provider name."""
        return "MockDataProvider"

    def get_rate_limit_info(self) -> dict:
        """Get rate limiting info (no limits for mock provider)."""
        return {
            "requests_per_minute": 1000,
            "requests_per_hour": 60000,
            "requests_per_day": 1000000,
            "current_usage": 0
        }
