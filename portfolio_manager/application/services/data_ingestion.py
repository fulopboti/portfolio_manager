"""Data ingestion service for collecting market data."""

from dataclasses import dataclass
from datetime import datetime

from portfolio_manager.application.ports import AssetRepository, DataProvider
from portfolio_manager.domain.entities import Asset, AssetType
from portfolio_manager.domain.exceptions import (
    DataIngestionError,
    DomainValidationError,
)
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError

from .base_service import ExceptionBasedService


@dataclass
class IngestionResult:
    """Result of a data ingestion operation."""

    symbol: str
    success: bool
    snapshots_count: int
    error: str | None = None


class DataIngestionService(ExceptionBasedService):
    """Service for ingesting market data from external providers."""

    def __init__(
        self,
        data_provider: DataProvider,
        asset_repository: AssetRepository,
        batch_size: int | None = None,
        retry_attempts: int | None = None,
    ):
        super().__init__(logger_name=f"{__name__}.{self.__class__.__name__}")
        self.data_provider = data_provider
        self.asset_repository = asset_repository

        # Use provided values or fall back to reasonable defaults
        self.batch_size = batch_size if batch_size is not None else 100
        self.retry_attempts = retry_attempts if retry_attempts is not None else 3

        # Log configuration for observability
        self._log_operation_start(
            "initialize_service",
            f"batch_size={self.batch_size}, retry_attempts={self.retry_attempts}",
        )
        self._log_operation_success(
            "initialize_service", "DataIngestionService configured"
        )

    async def ingest_symbol(
        self,
        symbol: str,
        asset_type: AssetType,
        exchange: str,
        name: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> IngestionResult:
        """Ingest data for a single symbol."""
        try:
            # Get OHLCV data from provider first (fail fast)
            if start_date is None:
                start_date = datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            if end_date is None:
                end_date = datetime.now()

            snapshots = await self.data_provider.get_ohlcv_data(
                symbol, start_date, end_date
            )

            # Only create asset if data fetching succeeds
            existing_asset = await self.asset_repository.get_asset(symbol)
            if not existing_asset:
                asset = Asset(
                    symbol=symbol,
                    exchange=exchange,
                    asset_type=asset_type,
                    name=name,
                )
                await self.asset_repository.save_asset(asset)

            # Save snapshots to repository
            snapshots_saved = 0
            for snapshot in snapshots:
                try:
                    await self.asset_repository.save_snapshot(snapshot)
                    snapshots_saved += 1
                except (DataAccessError, DomainValidationError) as e:
                    # Individual snapshot validation errors
                    return IngestionResult(
                        symbol=symbol,
                        success=False,
                        snapshots_count=0,
                        error=f"Snapshot validation error: {str(e)}",
                    )
                except Exception as e:
                    # Catch-all for unexpected errors
                    return IngestionResult(
                        symbol=symbol,
                        success=False,
                        snapshots_count=0,
                        error=f"Unexpected error during snapshot processing: {str(e)}",
                    )

            # Get and save fundamental data
            try:
                fundamentals = await self.data_provider.get_fundamental_data(symbol)
                if fundamentals:
                    await self.asset_repository.save_fundamental_metrics(
                        symbol, fundamentals
                    )
            except (DataAccessError, DataIngestionError) as e:
                # Log but don't fail - fundamental data is optional
                self._logger.warning(
                    f"Failed to save fundamental data for {symbol}: {e}"
                )
            except Exception as e:
                # Unexpected errors in fundamental data processing
                self._logger.warning(
                    f"Unexpected error saving fundamental data for {symbol}: {e}"
                )

            return IngestionResult(
                symbol=symbol,
                success=True,
                snapshots_count=snapshots_saved,
            )

        except DataIngestionError as e:
            return IngestionResult(
                symbol=symbol,
                success=False,
                snapshots_count=0,
                error=str(e),
            )
        except (DataAccessError, DomainValidationError) as e:
            return IngestionResult(
                symbol=symbol,
                success=False,
                snapshots_count=0,
                error=f"Data access error: {str(e)}",
            )
        except Exception as e:
            # Catch-all for truly unexpected errors
            return IngestionResult(
                symbol=symbol,
                success=False,
                snapshots_count=0,
                error=f"Unexpected ingestion error: {str(e)}",
            )

    async def ingest_multiple_symbols(
        self,
        symbols: list[str],
        asset_type: AssetType,
        exchange: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[IngestionResult]:
        """Ingest data for multiple symbols."""
        results = []

        for symbol in symbols:
            # For this test implementation, use symbol as name
            # In real implementation, this would come from a lookup service
            result = await self.ingest_symbol(
                symbol=symbol,
                asset_type=asset_type,
                exchange=exchange,
                name=symbol,  # Simplified for testing
                start_date=start_date,
                end_date=end_date,
            )
            results.append(result)

        return results

    async def refresh_all_assets(self) -> list[IngestionResult]:
        """Refresh data for all existing assets."""
        all_assets = await self.asset_repository.get_all_assets()
        results = []

        for asset in all_assets:
            result = await self.ingest_symbol(
                symbol=asset.symbol,
                asset_type=asset.asset_type,
                exchange=asset.exchange,
                name=asset.name,
            )
            results.append(result)

        return results
