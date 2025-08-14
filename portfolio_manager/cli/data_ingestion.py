"""CLI commands for data ingestion operations."""

import asyncio
from datetime import UTC, datetime, timedelta

import click

from portfolio_manager.config.factory import ConfiguredServiceBuilder
from portfolio_manager.domain.entities import AssetType
from portfolio_manager.infrastructure.data_providers import (
    create_data_provider_factory,
)


@click.group()
def data():
    """Data ingestion commands."""
    pass


@data.command()
@click.argument('symbol')
@click.option('--asset-type', type=click.Choice(['STOCK', 'BOND', 'ETF', 'MUTUAL_FUND']),
              default='STOCK', help='Type of asset')
@click.option('--exchange', default='NASDAQ', help='Exchange where asset is traded')
@click.option('--name', help='Asset name (defaults to symbol)')
@click.option('--days', default=30, help='Number of days of historical data to fetch')
@click.option('--provider', default=None, help='Data provider to use (defaults to configured primary)')
def ingest_symbol(symbol: str, asset_type: str, exchange: str, name: str | None, days: int, provider: str | None):
    """Ingest data for a single symbol."""
    async def _ingest():
        # Build service stack
        builder = ConfiguredServiceBuilder()
        stack = builder.build_complete_service_stack()

        # Create data provider factory
        provider_factory = create_data_provider_factory(builder.config)

        # Get the specified provider or use default
        if provider:
            data_provider = provider_factory.get_provider(provider)
        else:
            data_provider = provider_factory.get_primary_provider()

        # Create data ingestion service
        service = builder.factory.create_data_ingestion_service(
            data_provider, stack['repositories']['asset']
        )

        # Calculate date range
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)

        click.echo(f"Ingesting {symbol} ({asset_type}) from {exchange}")
        click.echo(f"Data provider: {data_provider.get_provider_name()}")
        click.echo(f"Date range: {start_date.date()} to {end_date.date()}")

        # Perform ingestion
        result = await service.ingest_symbol(
            symbol=symbol,
            asset_type=AssetType(asset_type),
            exchange=exchange,
            name=name or symbol,
            start_date=start_date,
            end_date=end_date
        )

        if result.success:
            click.echo(f"✓ Successfully ingested {result.snapshots_count} snapshots for {symbol}")
        else:
            click.echo(f"✗ Failed to ingest {symbol}: {result.error}")

    asyncio.run(_ingest())


@data.command()
@click.argument('symbols', nargs=-1, required=True)
@click.option('--asset-type', type=click.Choice(['STOCK', 'BOND', 'ETF', 'MUTUAL_FUND']),
              default='STOCK', help='Type of assets')
@click.option('--exchange', default='NASDAQ', help='Exchange where assets are traded')
@click.option('--days', default=30, help='Number of days of historical data to fetch')
@click.option('--provider', default=None, help='Data provider to use (defaults to configured primary)')
def ingest_multiple(symbols: tuple, asset_type: str, exchange: str, days: int, provider: str | None):
    """Ingest data for multiple symbols."""
    async def _ingest():
        # Build service stack
        builder = ConfiguredServiceBuilder()
        stack = builder.build_complete_service_stack()

        # Create data provider factory
        provider_factory = create_data_provider_factory(builder.config)

        # Get the specified provider or use default
        if provider:
            data_provider = provider_factory.get_provider(provider)
        else:
            data_provider = provider_factory.get_primary_provider()

        # Create data ingestion service
        service = builder.factory.create_data_ingestion_service(
            data_provider, stack['repositories']['asset']
        )

        # Calculate date range
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)

        symbols_list = list(symbols)
        click.echo(f"Ingesting {len(symbols_list)} symbols: {', '.join(symbols_list)}")
        click.echo(f"Data provider: {data_provider.get_provider_name()}")
        click.echo(f"Date range: {start_date.date()} to {end_date.date()}")

        # Perform batch ingestion
        results = await service.ingest_multiple_symbols(
            symbols=symbols_list,
            asset_type=AssetType(asset_type),
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )

        # Report results
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_snapshots = sum(r.snapshots_count for r in results if r.success)

        click.echo("\nIngestion Results:")
        click.echo(f"✓ Successful: {successful}")
        click.echo(f"✗ Failed: {failed}")
        click.echo(f"📊 Total snapshots: {total_snapshots}")

        # Show failures
        for result in results:
            if not result.success:
                click.echo(f"  ✗ {result.symbol}: {result.error}")

    asyncio.run(_ingest())


@data.command()
@click.option('--provider', default=None, help='Data provider to use (defaults to configured primary)')
def refresh_all(provider: str | None):
    """Refresh data for all existing assets."""
    async def _refresh():
        # Build service stack
        builder = ConfiguredServiceBuilder()
        stack = builder.build_complete_service_stack()

        # Create data provider factory
        provider_factory = create_data_provider_factory(builder.config)

        # Get the specified provider or use default
        if provider:
            data_provider = provider_factory.get_provider(provider)
        else:
            data_provider = provider_factory.get_primary_provider()

        # Create data ingestion service
        service = builder.factory.create_data_ingestion_service(
            data_provider, stack['repositories']['asset']
        )

        click.echo("Refreshing data for all existing assets...")
        click.echo(f"Data provider: {data_provider.get_provider_name()}")

        # Perform refresh
        results = await service.refresh_all_assets()

        if not results:
            click.echo("No assets found to refresh")
            return

        # Report results
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_snapshots = sum(r.snapshots_count for r in results if r.success)

        click.echo("\nRefresh Results:")
        click.echo(f"✓ Successful: {successful}")
        click.echo(f"✗ Failed: {failed}")
        click.echo(f"📊 Total snapshots: {total_snapshots}")

        # Show failures
        for result in results:
            if not result.success:
                click.echo(f"  ✗ {result.symbol}: {result.error}")

    asyncio.run(_refresh())


@data.command()
def list_assets():
    """List all assets in the database."""
    async def _list():
        # Build service stack
        builder = ConfiguredServiceBuilder()
        stack = builder.build_complete_service_stack()

        repo = stack['repositories']['asset']
        assets = await repo.get_all_assets()

        if not assets:
            click.echo("No assets found")
            return

        click.echo(f"Found {len(assets)} assets:")
        click.echo()

        # Group by asset type
        by_type = {}
        for asset in assets:
            if asset.asset_type not in by_type:
                by_type[asset.asset_type] = []
            by_type[asset.asset_type].append(asset)

        for asset_type, asset_list in by_type.items():
            click.echo(f"{asset_type.value}S:")
            for asset in sorted(asset_list, key=lambda a: a.symbol):
                click.echo(f"  {asset.symbol} - {asset.name} ({asset.exchange})")
            click.echo()

    asyncio.run(_list())


if __name__ == '__main__':
    data()
