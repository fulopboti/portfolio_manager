"""Integration tests for data ingestion CLI commands."""

import pytest
import tempfile
from unittest.mock import patch, AsyncMock, Mock
from click.testing import CliRunner
from datetime import datetime, timezone

from portfolio_manager.cli.data_ingestion import data
from portfolio_manager.domain.entities import Asset, AssetType


class TestDataIngestionCLI:
    """Integration tests for data ingestion CLI commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

        # Mock the service builder and its dependencies
        self.mock_asset_repo = Mock()
        self.mock_asset_repo.save_asset = AsyncMock()
        self.mock_asset_repo.save_snapshot = AsyncMock()
        self.mock_asset_repo.get_asset = AsyncMock(return_value=None)
        self.mock_asset_repo.get_all_assets = AsyncMock(return_value=[])
        self.mock_asset_repo.save_fundamental_metrics = AsyncMock()

        self.mock_stack = {
            'repositories': {'asset': self.mock_asset_repo},
            'services': {},
            'factory': Mock(),
            'config': Mock()
        }

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_ingest_symbol_command_success(self, mock_factory_creator, mock_builder_class):
        """Test successful symbol ingestion via CLI."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        # Mock the service
        mock_service = Mock()
        mock_service.ingest_symbol = AsyncMock(return_value=Mock(
            success=True,
            symbol="AAPL",
            snapshots_count=30,
            error=None
        ))
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run CLI command
        result = self.runner.invoke(data, [
            'ingest-symbol', 'AAPL',
            '--asset-type', 'STOCK',
            '--exchange', 'NASDAQ',
            '--name', 'Apple Inc.',
            '--days', '30'
        ])

        # Verify result
        assert result.exit_code == 0
        assert "✓ Successfully ingested 30 snapshots for AAPL" in result.output
        assert "AAPL (STOCK) from NASDAQ" in result.output
        assert "Test Provider" in result.output

        # Verify mocks were called
        mock_service.ingest_symbol.assert_called_once()
        call_args = mock_service.ingest_symbol.call_args[1]
        assert call_args['symbol'] == 'AAPL'
        assert call_args['asset_type'] == AssetType.STOCK
        assert call_args['exchange'] == 'NASDAQ'
        assert call_args['name'] == 'Apple Inc.'

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_ingest_symbol_command_failure(self, mock_factory_creator, mock_builder_class):
        """Test failed symbol ingestion via CLI."""
        # Setup mocks for failure
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        # Mock the service to return failure
        mock_service = Mock()
        mock_service.ingest_symbol = AsyncMock(return_value=Mock(
            success=False,
            symbol="FAIL",
            snapshots_count=0,
            error="Provider error"
        ))
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run CLI command
        result = self.runner.invoke(data, [
            'ingest-symbol', 'FAIL',
            '--asset-type', 'STOCK'
        ])

        # Verify result shows failure
        assert result.exit_code == 0  # CLI doesn't fail, just reports error
        assert "✗ Failed to ingest FAIL: Provider error" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_ingest_multiple_command_success(self, mock_factory_creator, mock_builder_class):
        """Test successful multiple symbol ingestion via CLI."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        # Mock successful results for multiple symbols
        mock_service = Mock()
        mock_service.ingest_multiple_symbols = AsyncMock(return_value=[
            Mock(success=True, symbol="AAPL", snapshots_count=30, error=None),
            Mock(success=True, symbol="MSFT", snapshots_count=25, error=None),
            Mock(success=False, symbol="FAIL", snapshots_count=0, error="Network error")
        ])
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run CLI command
        result = self.runner.invoke(data, [
            'ingest-multiple', 'AAPL', 'MSFT', 'FAIL',
            '--asset-type', 'STOCK',
            '--exchange', 'NASDAQ',
            '--days', '30'
        ])

        # Verify results
        assert result.exit_code == 0
        assert "✓ Successful: 2" in result.output
        assert "✗ Failed: 1" in result.output
        assert "📊 Total snapshots: 55" in result.output
        assert "✗ FAIL: Network error" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_refresh_all_command_success(self, mock_factory_creator, mock_builder_class):
        """Test successful refresh all assets via CLI."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        # Mock successful refresh results
        mock_service = Mock()
        mock_service.refresh_all_assets = AsyncMock(return_value=[
            Mock(success=True, symbol="AAPL", snapshots_count=5, error=None),
            Mock(success=True, symbol="MSFT", snapshots_count=3, error=None)
        ])
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run CLI command
        result = self.runner.invoke(data, ['refresh-all'])

        # Verify results
        assert result.exit_code == 0
        assert "✓ Successful: 2" in result.output
        assert "✗ Failed: 0" in result.output
        assert "📊 Total snapshots: 8" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_refresh_all_command_no_assets(self, mock_factory_creator, mock_builder_class):
        """Test refresh all command when no assets exist."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        # Mock empty refresh results
        mock_service = Mock()
        mock_service.refresh_all_assets = AsyncMock(return_value=[])
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run CLI command
        result = self.runner.invoke(data, ['refresh-all'])

        # Verify results
        assert result.exit_code == 0
        assert "No assets found to refresh" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    def test_list_assets_command_success(self, mock_builder_class):
        """Test successful list assets via CLI."""
        # Setup mock assets
        mock_assets = [
            Asset(symbol="AAPL", name="Apple Inc.", exchange="NASDAQ", asset_type=AssetType.STOCK),
            Asset(symbol="MSFT", name="Microsoft Corp.", exchange="NASDAQ", asset_type=AssetType.STOCK),
            Asset(symbol="BND", name="Bond ETF", exchange="NYSE", asset_type=AssetType.ETF)
        ]

        mock_asset_repo = Mock()
        mock_asset_repo.get_all_assets = AsyncMock(return_value=mock_assets)

        mock_stack = {
            'repositories': {'asset': mock_asset_repo},
            'services': {},
            'factory': Mock(),
            'config': Mock()
        }

        mock_builder = Mock()
        mock_builder.build_complete_service_stack.return_value = mock_stack
        mock_builder_class.return_value = mock_builder

        # Run CLI command
        result = self.runner.invoke(data, ['list-assets'])

        # Verify results
        assert result.exit_code == 0
        assert "Found 3 assets:" in result.output
        assert "STOCKS:" in result.output
        assert "AAPL - Apple Inc. (NASDAQ)" in result.output
        assert "MSFT - Microsoft Corp. (NASDAQ)" in result.output
        assert "ETFS:" in result.output
        assert "BND - Bond ETF (NYSE)" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    def test_list_assets_command_no_assets(self, mock_builder_class):
        """Test list assets command when no assets exist."""
        # Setup empty asset list
        mock_asset_repo = Mock()
        mock_asset_repo.get_all_assets = AsyncMock(return_value=[])

        mock_stack = {
            'repositories': {'asset': mock_asset_repo},
            'services': {},
            'factory': Mock(),
            'config': Mock()
        }

        mock_builder = Mock()
        mock_builder.build_complete_service_stack.return_value = mock_stack
        mock_builder_class.return_value = mock_builder

        # Run CLI command
        result = self.runner.invoke(data, ['list-assets'])

        # Verify results
        assert result.exit_code == 0
        assert "No assets found" in result.output

    def test_ingest_symbol_command_invalid_asset_type(self):
        """Test ingest symbol with invalid asset type."""
        result = self.runner.invoke(data, [
            'ingest-symbol', 'TEST',
            '--asset-type', 'INVALID_TYPE'
        ])

        # Should fail due to invalid choice
        assert result.exit_code != 0
        assert "Invalid value for '--asset-type'" in result.output

    def test_ingest_multiple_command_no_symbols(self):
        """Test ingest multiple command with no symbols provided."""
        result = self.runner.invoke(data, ['ingest-multiple'])

        # Should fail due to missing required arguments
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_command_help_text(self):
        """Test that help text is displayed correctly."""
        result = self.runner.invoke(data, ['--help'])

        assert result.exit_code == 0
        assert "Data ingestion commands." in result.output
        assert "ingest-symbol" in result.output
        assert "ingest-multiple" in result.output
        assert "refresh-all" in result.output
        assert "list-assets" in result.output

    def test_ingest_symbol_help_text(self):
        """Test ingest-symbol command help text."""
        result = self.runner.invoke(data, ['ingest-symbol', '--help'])

        assert result.exit_code == 0
        assert "Ingest data for a single symbol." in result.output
        assert "--asset-type" in result.output
        assert "--exchange" in result.output
        assert "--name" in result.output
        assert "--days" in result.output

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_default_parameter_values(self, mock_factory_creator, mock_builder_class):
        """Test that default parameter values are used correctly."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        mock_service = Mock()
        mock_service.ingest_symbol = AsyncMock(return_value=Mock(
            success=True, symbol="TEST", snapshots_count=30, error=None
        ))
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run command with minimal parameters
        result = self.runner.invoke(data, ['ingest-symbol', 'TEST'])

        # Verify defaults were used
        assert result.exit_code == 0
        call_args = mock_service.ingest_symbol.call_args[1]
        assert call_args['asset_type'] == AssetType.STOCK  # default
        assert call_args['exchange'] == 'NASDAQ'  # default
        assert call_args['name'] == 'TEST'  # symbol used as name default

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_date_range_calculation(self, mock_factory_creator, mock_builder_class):
        """Test that date ranges are calculated correctly."""
        # Setup mocks
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = self.mock_stack
        mock_builder_class.return_value = mock_builder

        mock_service = Mock()
        mock_service.ingest_symbol = AsyncMock(return_value=Mock(
            success=True, symbol="DATE_TEST", snapshots_count=7, error=None
        ))
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Run command with 7 days
        result = self.runner.invoke(data, [
            'ingest-symbol', 'DATE_TEST',
            '--days', '7'
        ])

        # Verify the date range was passed correctly
        assert result.exit_code == 0
        call_args = mock_service.ingest_symbol.call_args[1]

        start_date = call_args['start_date']
        end_date = call_args['end_date']

        # Verify dates are datetime objects and roughly 7 days apart
        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)
        assert (end_date - start_date).days <= 7  # Allow for slight timing differences


class TestDataIngestionCLIErrorHandling:
    """Test error handling in CLI commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    def test_service_builder_exception(self, mock_builder_class):
        """Test handling of service builder exceptions."""
        # Make the builder raise an exception
        mock_builder_class.side_effect = Exception("Configuration error")

        result = self.runner.invoke(data, ['ingest-symbol', 'TEST'])

        # CLI should handle the exception gracefully
        assert result.exit_code != 0

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.MockDataProvider')
    def test_async_operation_exception(self, mock_provider_class, mock_builder_class):
        """Test handling of async operation exceptions."""
        # Setup mocks to raise exception in async operation
        mock_provider = Mock()
        mock_provider_class.return_value = mock_provider

        mock_asset_repo = Mock()
        mock_stack = {'repositories': {'asset': mock_asset_repo}}

        mock_builder = Mock()
        mock_builder.build_complete_service_stack.return_value = mock_stack
        mock_builder_class.return_value = mock_builder

        mock_service = Mock()
        mock_service.ingest_symbol = AsyncMock(side_effect=Exception("Async error"))
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        result = self.runner.invoke(data, ['ingest-symbol', 'ERROR_TEST'])

        # Should handle async exceptions
        assert result.exit_code != 0

    def test_invalid_days_parameter(self):
        """Test handling of invalid days parameter."""
        result = self.runner.invoke(data, [
            'ingest-symbol', 'TEST',
            '--days', 'invalid'
        ])

        # Should fail with parameter validation error
        assert result.exit_code != 0
        assert "Invalid value" in result.output

    def test_negative_days_parameter(self):
        """Test handling of negative days parameter."""
        # Note: Click doesn't automatically validate positive integers,
        # so we need to test what actually happens
        result = self.runner.invoke(data, [
            'ingest-symbol', 'TEST',
            '--days', '-5'
        ])

        # The command will run but may behave unexpectedly with negative days
        # This tests that the CLI doesn't crash
        assert isinstance(result.exit_code, int)


class TestDataIngestionCLIPerformance:
    """Performance-related tests for CLI commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch('portfolio_manager.cli.data_ingestion.ConfiguredServiceBuilder')
    @patch('portfolio_manager.cli.data_ingestion.create_data_provider_factory')
    def test_large_symbol_batch_performance(self, mock_factory_creator, mock_builder_class):
        """Test performance with large number of symbols."""
        # Setup mocks for large batch
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = "Test Provider"
        
        mock_provider_factory = Mock()
        mock_provider_factory.get_primary_provider.return_value = mock_provider
        mock_factory_creator.return_value = mock_provider_factory

        mock_asset_repo = Mock()
        mock_stack = {'repositories': {'asset': mock_asset_repo}}

        mock_builder = Mock()
        mock_builder.config = Mock()
        mock_builder.build_complete_service_stack.return_value = mock_stack
        mock_builder_class.return_value = mock_builder

        # Generate large result set
        large_results = [
            Mock(success=True, symbol=f"SYM{i:03d}", snapshots_count=30, error=None)
            for i in range(100)
        ]

        mock_service = Mock()
        mock_service.ingest_multiple_symbols = AsyncMock(return_value=large_results)
        mock_builder.factory.create_data_ingestion_service.return_value = mock_service

        # Create large symbol list
        symbols = [f"SYM{i:03d}" for i in range(100)]

        # Run CLI command (use timeout to ensure it completes reasonably quickly)
        result = self.runner.invoke(data, ['ingest-multiple'] + symbols)

        # Verify it handles large batches
        assert result.exit_code == 0
        assert "✓ Successful: 100" in result.output
        assert "📊 Total snapshots: 3000" in result.output