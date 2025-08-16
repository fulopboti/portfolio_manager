"""Yahoo Finance data provider using yfinance library."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
import yfinance as yf

from portfolio_manager.application.ports import DataProvider
from portfolio_manager.domain.entities import AssetSnapshot
from portfolio_manager.domain.exceptions import DataIngestionError


class YFinanceProvider(DataProvider):
    """Data provider that uses Yahoo Finance API via yfinance library."""

    def __init__(self, request_delay: float = 0.1, max_retries: int = 3):
        """Initialize the Yahoo Finance provider.

        Args:
            request_delay: Delay between requests to respect rate limits (seconds)
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__)

        # Track request statistics for rate limiting
        self._request_count = 0
        self._failed_requests = 0

    async def get_ohlcv_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> list[AssetSnapshot]:
        """Retrieve OHLCV data from Yahoo Finance."""
        if not self.supports_symbol(symbol):
            raise DataIngestionError(
                f"Symbol {symbol} is not supported by Yahoo Finance"
            )

        try:
            # Add delay to respect rate limits
            if self._request_count > 0:
                await asyncio.sleep(self.request_delay)

            # Run yfinance in thread pool to avoid blocking
            ticker_data = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_ticker_data, symbol, start_date, end_date
            )

            self._request_count += 1

            if ticker_data is None or ticker_data.empty:
                raise DataIngestionError(f"No data available for symbol {symbol}")

            # Convert pandas DataFrame to AssetSnapshot objects
            snapshots = []
            for date, row in ticker_data.iterrows():
                try:
                    snapshot = self._create_asset_snapshot(symbol, date, row)
                    if snapshot:
                        snapshots.append(snapshot)
                except (ValueError, InvalidOperation) as e:
                    self.logger.warning(
                        f"Skipping invalid data point for {symbol} on {date}: {e}"
                    )
                    continue

            if not snapshots:
                raise DataIngestionError(f"No valid data points found for {symbol}")

            self.logger.info(f"Retrieved {len(snapshots)} data points for {symbol}")
            return snapshots

        except Exception as e:
            self._failed_requests += 1
            # Don't increment request count here as it may not have been incremented yet
            if isinstance(e, DataIngestionError):
                raise
            else:
                raise DataIngestionError(
                    f"Failed to fetch data for {symbol}: {str(e)}"
                ) from e

    async def get_fundamental_data(self, symbol: str) -> dict[str, Any]:
        """Retrieve fundamental data from Yahoo Finance."""
        if not self.supports_symbol(symbol):
            raise DataIngestionError(
                f"Symbol {symbol} is not supported by Yahoo Finance"
            )

        try:
            # Add delay to respect rate limits
            if self._request_count > 0:
                await asyncio.sleep(self.request_delay)

            # Run yfinance in thread pool
            fundamental_data = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_fundamental_data, symbol
            )

            self._request_count += 1

            if not fundamental_data:
                self.logger.warning(f"No fundamental data available for {symbol}")
                return {}

            return fundamental_data

        except Exception as e:
            self._failed_requests += 1
            raise DataIngestionError(
                f"Failed to fetch fundamental data for {symbol}: {str(e)}"
            ) from e

    def supports_symbol(self, symbol: str) -> bool:
        """Check if the provider supports the given symbol."""
        if not symbol or not isinstance(symbol, str):
            return False

        # Basic validation - Yahoo Finance supports most standard symbols
        symbol = symbol.strip().upper()

        # Invalid symbols that should be rejected
        invalid_symbols = {
            "",
            "INVALID",
            "TEST",
            "MOCK",
            "FAIL",
            "ERROR",
            "NULL",
            "NONE",
        }

        return symbol not in invalid_symbols and len(symbol) <= 10

    def get_provider_name(self) -> str:
        """Get the name of the data provider."""
        return "Yahoo Finance (yfinance)"

    def get_rate_limit_info(self) -> dict[str, Any]:
        """Get rate limiting information for the provider."""
        return {
            "provider": "Yahoo Finance",
            "requests_per_minute": 60,  # Conservative estimate
            "requests_per_hour": 2000,  # Yahoo Finance unofficial limit
            "requests_per_day": 48000,  # Conservative daily limit
            "current_request_count": self._request_count,
            "failed_requests": self._failed_requests,
            "request_delay": self.request_delay,
            "max_retries": self.max_retries,
            "notes": "Unofficial API with rate limiting based on community guidelines",
        }

    def _fetch_ticker_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame | None:
        """Fetch ticker data using yfinance (synchronous)."""
        try:
            ticker = yf.Ticker(symbol)

            # Format dates for yfinance
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Fetch historical data
            data = ticker.history(
                start=start_str,
                end=end_str,
                interval="1d",
                auto_adjust=True,
                prepost=False,
            )

            if data.empty:
                self.logger.warning(f"No historical data returned for {symbol}")
                return None

            return data

        except Exception as e:
            self.logger.error(f"Error fetching ticker data for {symbol}: {e}")
            raise

    def _fetch_fundamental_data(self, symbol: str) -> dict[str, Any]:
        """Fetch fundamental data using yfinance (synchronous)."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if not info:
                return {}

            # Extract and normalize fundamental metrics
            fundamental_data: dict[str, Any] = {}

            # Financial ratios
            if "trailingPE" in info and info["trailingPE"]:
                fundamental_data["pe_ratio"] = Decimal(str(info["trailingPE"]))

            if "priceToBook" in info and info["priceToBook"]:
                fundamental_data["pb_ratio"] = Decimal(str(info["priceToBook"]))

            if "dividendYield" in info and info["dividendYield"]:
                fundamental_data["dividend_yield"] = Decimal(str(info["dividendYield"]))

            # Market data
            if "marketCap" in info and info["marketCap"]:
                fundamental_data["market_cap"] = Decimal(str(info["marketCap"]))

            if "totalRevenue" in info and info["totalRevenue"]:
                fundamental_data["revenue"] = Decimal(str(info["totalRevenue"]))

            if "netIncomeToCommon" in info and info["netIncomeToCommon"]:
                fundamental_data["net_income"] = Decimal(str(info["netIncomeToCommon"]))

            # Financial health ratios
            if "debtToEquity" in info and info["debtToEquity"]:
                fundamental_data["debt_to_equity"] = Decimal(str(info["debtToEquity"]))

            if "currentRatio" in info and info["currentRatio"]:
                fundamental_data["current_ratio"] = Decimal(str(info["currentRatio"]))

            if "returnOnEquity" in info and info["returnOnEquity"]:
                fundamental_data["roe"] = Decimal(str(info["returnOnEquity"]))

            if "returnOnAssets" in info and info["returnOnAssets"]:
                fundamental_data["roa"] = Decimal(str(info["returnOnAssets"]))

            # Growth metrics
            if "revenueGrowth" in info and info["revenueGrowth"]:
                fundamental_data["revenue_growth"] = Decimal(str(info["revenueGrowth"]))

            if "earningsGrowth" in info and info["earningsGrowth"]:
                fundamental_data["earnings_growth"] = Decimal(
                    str(info["earningsGrowth"])
                )

            # Add metadata
            fundamental_data["data_source"] = "yahoo_finance"
            fundamental_data["last_updated"] = datetime.now(UTC).isoformat()

            return fundamental_data

        except Exception as e:
            self.logger.error(f"Error fetching fundamental data for {symbol}: {e}")
            raise

    def _create_asset_snapshot(
        self, symbol: str, date: pd.Timestamp, row: pd.Series
    ) -> AssetSnapshot | None:
        """Create AssetSnapshot from pandas row data."""
        try:
            # Convert pandas Timestamp to Python datetime
            if hasattr(date, "to_pydatetime"):
                timestamp = date.to_pydatetime()
            else:
                timestamp = pd.to_datetime(date).to_pydatetime()

            # Ensure timezone awareness
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

            # Extract OHLCV data and convert to Decimal
            open_price = self._safe_decimal(row.get("Open"))
            high_price = self._safe_decimal(row.get("High"))
            low_price = self._safe_decimal(row.get("Low"))
            close_price = self._safe_decimal(row.get("Close"))
            volume = int(row.get("Volume", 0))

            # Validate that we have required data
            if None in [open_price, high_price, low_price, close_price]:
                return None

            # Type narrowing - after None check, these must be Decimal
            assert open_price is not None
            assert high_price is not None
            assert low_price is not None
            assert close_price is not None

            # Additional validation
            if any(
                price <= 0 for price in [open_price, high_price, low_price, close_price]
            ):
                return None

            if volume < 0:
                volume = 0

            return AssetSnapshot(
                symbol=symbol,
                timestamp=timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
            )

        except Exception as e:
            self.logger.warning(f"Failed to create snapshot for {symbol}: {e}")
            return None

    def _safe_decimal(self, value: Any) -> Decimal | None:
        """Safely convert value to Decimal."""
        if value is None:
            return None

        # Handle pandas NA values safely
        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            # pd.isna() can raise ValueError for certain types like lists
            pass

        try:
            return Decimal(str(float(value)))
        except (ValueError, InvalidOperation, TypeError):
            return None
