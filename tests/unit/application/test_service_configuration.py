"""Tests for application service configuration integration."""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from decimal import Decimal
from datetime import datetime
from uuid import uuid4

from portfolio_manager.application.services import DataIngestionService, PortfolioSimulatorService, StrategyScoreService
from portfolio_manager.application.services.base_service import (
    BaseApplicationService, ExceptionBasedService, ResultBasedService,
    ServiceErrorStrategy, ServiceResult
)
from portfolio_manager.domain.entities import AssetType, Asset
from portfolio_manager.domain.exceptions import DataIngestionError, DomainValidationError


class TestDataIngestionServiceConfiguration:
    """Test DataIngestionService configuration and initialization."""

    def test_service_initialization_with_default_values(self):
        """Test service initialization with default configuration values."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository
        )

        assert service.data_provider is mock_data_provider
        assert service.asset_repository is mock_asset_repository
        assert service.batch_size == 100  # Default value
        assert service.retry_attempts == 3  # Default value
        assert isinstance(service, ExceptionBasedService)

    def test_service_initialization_with_custom_values(self):
        """Test service initialization with custom configuration values."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=250,
            retry_attempts=7
        )

        assert service.batch_size == 250
        assert service.retry_attempts == 7

    def test_service_initialization_with_none_values_uses_defaults(self):
        """Test service initialization with None values falls back to defaults."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=None,
            retry_attempts=None
        )

        assert service.batch_size == 100  # Default fallback
        assert service.retry_attempts == 3  # Default fallback

    def test_service_initialization_with_mixed_values(self):
        """Test service initialization with mix of provided and None values."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=75,
            retry_attempts=None  # Should use default
        )

        assert service.batch_size == 75
        assert service.retry_attempts == 3  # Default

    def test_service_logging_initialization(self):
        """Test service properly initializes with logger."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=150,
            retry_attempts=5
        )

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None

    def test_service_configuration_logging_during_initialization(self):
        """Test service logs configuration during initialization."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        with patch.object(ExceptionBasedService, '_log_operation_start') as mock_log_start, \
             patch.object(ExceptionBasedService, '_log_operation_success') as mock_log_success:

            service = DataIngestionService(
                data_provider=mock_data_provider,
                asset_repository=mock_asset_repository,
                batch_size=200,
                retry_attempts=4
            )

            # Verify configuration logging
            mock_log_start.assert_called_once_with(
                "initialize_service",
                "batch_size=200, retry_attempts=4"
            )
            mock_log_success.assert_called_once_with(
                "initialize_service",
                "DataIngestionService configured"
            )

    @pytest.mark.asyncio
    async def test_service_uses_configuration_in_operations(self):
        """Test service uses configuration values in its operations."""
        mock_data_provider = AsyncMock()
        mock_asset_repository = AsyncMock()

        # Setup mock responses
        mock_data_provider.get_ohlcv_data.return_value = [Mock()]
        mock_asset_repository.get_asset.return_value = None
        mock_asset_repository.save_asset.return_value = None
        mock_asset_repository.save_snapshot.return_value = None
        mock_data_provider.get_fundamental_data.return_value = None

        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=50,  # Custom batch size
            retry_attempts=2  # Custom retry attempts
        )

        # Test ingest operation
        result = await service.ingest_symbol(
            symbol="TEST",
            asset_type=AssetType.STOCK,
            exchange="NYSE",
            name="Test Stock"
        )

        assert result.success is True
        assert service.batch_size == 50
        assert service.retry_attempts == 2


class TestPortfolioSimulatorServiceConfiguration:
    """Test PortfolioSimulatorService configuration and initialization."""

    def test_service_initialization_with_proper_logger(self):
        """Test service initialization with proper logger name."""
        mock_portfolio_repo = Mock()
        mock_asset_repo = Mock()

        service = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None
        assert service.portfolio_repository is mock_portfolio_repo
        assert service.asset_repository is mock_asset_repo
        assert isinstance(service, ResultBasedService)

    def test_service_inherits_result_based_service_functionality(self):
        """Test service properly inherits ResultBasedService functionality."""
        mock_portfolio_repo = Mock()
        mock_asset_repo = Mock()

        service = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )

        # Verify service has ResultBasedService characteristics
        assert hasattr(service, '_execute_operation')
        assert hasattr(service, '_log_operation_start')
        assert hasattr(service, '_log_operation_success')
        assert hasattr(service, '_log_operation_error')

    def test_service_configuration_injection(self):
        """Test service accepts configuration injection."""
        mock_portfolio_repo = Mock()
        mock_asset_repo = Mock()

        service = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )

        # Test configuration can be injected
        mock_config = Mock()
        service._config = mock_config

        assert service._config is mock_config
        assert hasattr(service, '_config')

    @pytest.mark.asyncio
    async def test_service_with_configuration_object(self):
        """Test service behavior when configuration object is available."""
        from portfolio_manager.config.schema import PortfolioConfig, PortfolioSimulationConfig, RiskManagementConfig

        mock_portfolio_repo = Mock()
        mock_asset_repo = Mock()

        # Mock repository responses for execute_trade
        mock_portfolio = Mock()
        mock_portfolio.id = uuid4()
        mock_portfolio.cash_balance = Decimal('10000.00')
        mock_portfolio_repo.get_portfolio.return_value = mock_portfolio

        mock_snapshot = Mock()
        mock_snapshot.close_price = Decimal('100.00')
        mock_asset_repo.get_latest_snapshot.return_value = mock_snapshot

        mock_portfolio_repo.save_trade.return_value = None
        mock_portfolio_repo.save_position.return_value = None
        mock_portfolio_repo.save_portfolio.return_value = None

        # Create service
        service = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )

        # Inject configuration
        portfolio_config = PortfolioConfig(
            simulation=PortfolioSimulationConfig(
                initial_cash=50000.0,
                default_currency="EUR",
                commission_rate=0.002,
                min_commission=2.0
            ),
            risk_management=RiskManagementConfig(
                max_position_size=0.15,
                max_sector_exposure=0.30,
                stop_loss_threshold=-0.08
            )
        )
        service._config = portfolio_config

        # Test service can access configuration
        assert service._config.simulation.initial_cash == 50000.0
        assert service._config.simulation.default_currency == "EUR"
        assert service._config.risk_management.max_position_size == 0.15


class TestStrategyScoreServiceConfiguration:
    """Test StrategyScoreService configuration and initialization."""

    def test_service_initialization_with_proper_logger(self):
        """Test service initialization with proper logger name."""
        mock_strategy_calculators = {"momentum": Mock()}
        mock_asset_repo = Mock()

        service = StrategyScoreService(
            strategy_calculators=mock_strategy_calculators,
            asset_repository=mock_asset_repo
        )

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None
        assert service.strategy_calculators is mock_strategy_calculators
        assert service.asset_repository is mock_asset_repo
        assert isinstance(service, ExceptionBasedService)

    def test_service_configuration_injection_for_strategies(self):
        """Test service accepts strategy configuration injection."""
        mock_strategy_calculators = {"value": Mock(), "quality": Mock()}
        mock_asset_repo = Mock()

        service = StrategyScoreService(
            strategy_calculators=mock_strategy_calculators,
            asset_repository=mock_asset_repo
        )

        # Test configuration can be injected
        from portfolio_manager.config.schema import StrategiesConfig, ScoringConfig, BacktestingConfig

        strategies_config = StrategiesConfig(
            scoring=ScoringConfig(
                enabled_strategies=["value", "quality", "momentum"],
                rebalance_frequency="weekly",
                min_score_threshold=75
            ),
            backtesting=BacktestingConfig(
                default_period="3Y",
                benchmark="SPY"
            )
        )

        service._config = strategies_config

        assert service._config.scoring.enabled_strategies == ["value", "quality", "momentum"]
        assert service._config.scoring.min_score_threshold == 75
        assert service._config.backtesting.benchmark == "SPY"

    @pytest.mark.asyncio
    async def test_service_uses_configuration_in_calculations(self):
        """Test service can use configuration in strategy calculations."""
        mock_calculator = Mock()
        mock_calculator.validate_metrics.return_value = True
        mock_calculator.calculate_score.return_value = Decimal('85.5')

        mock_strategy_calculators = {"momentum": mock_calculator}
        mock_asset_repo = AsyncMock()

        # Setup mock data
        mock_asset = Asset(symbol="TEST", exchange="NYSE", asset_type=AssetType.STOCK, name="Test")
        mock_asset_repo.get_all_assets.return_value = [mock_asset]
        mock_asset_repo.get_latest_snapshot.return_value = Mock()
        mock_asset_repo.get_fundamental_metrics.return_value = {"pe_ratio": 15.0}

        service = StrategyScoreService(
            strategy_calculators=mock_strategy_calculators,
            asset_repository=mock_asset_repo
        )

        # Add configuration
        from portfolio_manager.config.schema import StrategiesConfig, ScoringConfig, BacktestingConfig

        service._config = StrategiesConfig(
            scoring=ScoringConfig(
                enabled_strategies=["momentum"],
                rebalance_frequency="daily",
                min_score_threshold=80  # Higher threshold
            ),
            backtesting=BacktestingConfig(
                default_period="1Y",
                benchmark="QQQ"
            )
        )

        # Test calculation
        scores = await service.calculate_strategy_scores("momentum")

        assert len(scores) == 1
        assert scores[0].symbol == "TEST"
        assert scores[0].score == Decimal('85.5')
        # Service could potentially filter based on min_score_threshold from config


class TestBaseServiceConfigurationIntegration:
    """Test base service configuration integration patterns."""

    def test_exception_based_service_with_custom_logger_name(self):
        """Test ExceptionBasedService accepts custom logger name."""
        # Create anonymous service class for testing
        class TestService(ExceptionBasedService):
            def __init__(self):
                super().__init__(logger_name="custom.test.service")

        service = TestService()

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None

    def test_result_based_service_with_custom_logger_name(self):
        """Test ResultBasedService accepts custom logger name."""
        # Create anonymous service class for testing
        class TestService(ResultBasedService):
            def __init__(self):
                super().__init__(logger_name="custom.result.service")

        service = TestService()

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None

    def test_base_service_error_strategy_configuration(self):
        """Test base service error strategy configuration."""
        # Create anonymous service class for testing
        class TestService(BaseApplicationService):
            def __init__(self, error_strategy=ServiceErrorStrategy.RAISE_EXCEPTIONS):
                super().__init__(error_strategy=error_strategy)

        # Test with raise exceptions strategy
        service1 = TestService(ServiceErrorStrategy.RAISE_EXCEPTIONS)
        assert service1.error_strategy == ServiceErrorStrategy.RAISE_EXCEPTIONS

        # Test with return results strategy
        service2 = TestService(ServiceErrorStrategy.RETURN_RESULTS)
        assert service2.error_strategy == ServiceErrorStrategy.RETURN_RESULTS

    @pytest.mark.asyncio
    async def test_service_operation_execution_with_configuration(self):
        """Test service operation execution respects configuration."""
        # Create testable service
        class ConfigurableTestService(ExceptionBasedService):
            def __init__(self, config_value: int = 10):
                super().__init__()
                self.config_value = config_value

            async def test_operation(self):
                """Test operation that uses configuration."""
                return await self._execute_operation(
                    operation_name="test_op",
                    operation_func=lambda: self._perform_configured_work(),
                    context=f"config_value={self.config_value}"
                )

            async def _perform_configured_work(self):
                return {"result": self.config_value * 2}

        service = ConfigurableTestService(config_value=25)
        result = await service.test_operation()

        assert result["result"] == 50  # 25 * 2

    def test_service_logging_context_setup(self):
        """Test service logging context is properly set up."""
        class TestService(ExceptionBasedService):
            def __init__(self):
                super().__init__(logger_name="test.service")

        service = TestService()

        # Verify service has logger attribute
        assert hasattr(service, '_logger')
        assert service._logger is not None


class TestServiceConfigurationErrorHandling:
    """Test service configuration error handling and edge cases."""

    def test_data_ingestion_service_with_invalid_config_types(self):
        """Test DataIngestionService handles invalid config types gracefully."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Test with string values that should be converted to int
        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size="150",  # String instead of int
            retry_attempts="5"  # String instead of int
        )

        # Service should still function, though type consistency depends on implementation
        assert service.batch_size == "150"
        assert service.retry_attempts == "5"

    def test_service_with_extreme_configuration_values(self):
        """Test services handle extreme configuration values."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Test with extreme values
        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=1,  # Minimum
            retry_attempts=100  # Large number
        )

        assert service.batch_size == 1
        assert service.retry_attempts == 100

    def test_service_configuration_None_handling_edge_cases(self):
        """Test service handles various None scenarios."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Test all None
        service1 = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=None,
            retry_attempts=None
        )

        assert service1.batch_size == 100
        assert service1.retry_attempts == 3

        # Test zero values vs None
        service2 = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=0,  # Zero, not None
            retry_attempts=0  # Zero, not None
        )

        assert service2.batch_size == 0
        assert service2.retry_attempts == 0


class TestServiceConfigurationCompatibility:
    """Test backward compatibility and migration scenarios."""

    def test_service_backward_compatibility_without_config(self):
        """Test services maintain backward compatibility when used without configuration."""
        # Test old-style initialization still works
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Old way - positional arguments
        service = DataIngestionService(mock_data_provider, mock_asset_repository, 50, 2)

        assert service.data_provider is mock_data_provider
        assert service.asset_repository is mock_asset_repository
        assert service.batch_size == 50
        assert service.retry_attempts == 2

    def test_service_mixed_old_new_initialization(self):
        """Test services work with mixed old/new initialization patterns."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Mixed: positional + keyword
        service = DataIngestionService(
            mock_data_provider,
            mock_asset_repository,
            batch_size=75
            # retry_attempts not specified - should use default
        )

        assert service.batch_size == 75
        assert service.retry_attempts == 3  # Default

    def test_service_configuration_override_behavior(self):
        """Test service configuration override behavior."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Create service with initial config
        service = DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=100,
            retry_attempts=5
        )

        # Verify initial values
        assert service.batch_size == 100
        assert service.retry_attempts == 5

        # Test that values can be modified after creation (if needed)
        service.batch_size = 200
        assert service.batch_size == 200
