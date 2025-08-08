"""
Configuration-based factory for creating application components.

This module provides factories that create services, repositories, and other
components using the centralized configuration system.
"""

import logging
from typing import Dict, Any, Optional

from .settings import config
from .schema import (
    PortfolioManagerConfig, validate_config, DatabaseConfig, EventSystemConfig,
    PortfolioConfig, DataProvidersConfig
)
from ..domain.exceptions import DomainError


class ConfigurationError(DomainError):
    """Raised when configuration is invalid or missing."""
    pass


class ConfiguredComponentFactory:
    """
    Factory for creating application components with configuration injection.

    This factory ensures all components receive appropriate configuration
    sections and handles configuration validation.
    """

    def __init__(self, config_manager=None):
        """
        Initialize the factory with configuration.

        Args:
            config_manager: Configuration manager instance (uses global if None)
        """
        self.config_manager = config_manager or config
        self._logger = logging.getLogger(__name__)
        self._validated_config: Optional[PortfolioManagerConfig] = None
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate the complete configuration against schema."""
        try:
            config_dict = self.config_manager.get_all()
            self._validated_config = validate_config(config_dict)
            self._logger.info("Configuration validation successful")
        except Exception as e:
            self._logger.error(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Invalid configuration: {e}") from e

    @property
    def validated_config(self) -> PortfolioManagerConfig:
        """Get validated configuration object."""
        if self._validated_config is None:
            raise ConfigurationError("Configuration not validated")
        return self._validated_config

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration section."""
        return self.validated_config.database

    def get_event_system_config(self) -> EventSystemConfig:
        """Get event system configuration section."""
        return self.validated_config.event_system

    def get_portfolio_config(self) -> PortfolioConfig:
        """Get portfolio configuration section."""
        return self.validated_config.portfolio

    def get_data_providers_config(self) -> DataProvidersConfig:
        """Get data providers configuration section."""
        return self.validated_config.data_providers

    def create_repository_factory(self):
        """
        Create DuckDB repository factory with database configuration.

        Returns:
            Configured DuckDBRepositoryFactory instance
        """
        from ..infrastructure.duckdb import DuckDBRepositoryFactory

        db_config = self.get_database_config()
        database_path = db_config.connection.database_path

        # Convert memory flag to special path if needed
        if db_config.connection.memory:
            database_path = ":memory:"

        self._logger.info(f"Creating repository factory with database: {database_path}")

        return DuckDBRepositoryFactory(
            database_path=database_path,
            auto_initialize=True,
            config=db_config
        )

    def create_data_ingestion_service(self, data_provider, asset_repository):
        """
        Create data ingestion service with configuration.

        Args:
            data_provider: Data provider instance
            asset_repository: Asset repository instance

        Returns:
            Configured DataIngestionService
        """
        from ..application.services import DataIngestionService

        # Get configuration values with defaults from data_providers section
        data_providers_config = self.get_data_providers_config()
        batch_size = data_providers_config.batch_size

        # Get retry attempts from event system config
        event_config = self.get_event_system_config()
        retry_attempts = event_config.handlers.retry_attempts

        self._logger.info(f"Creating data ingestion service (batch_size={batch_size}, retry_attempts={retry_attempts})")

        return DataIngestionService(
            data_provider=data_provider,
            asset_repository=asset_repository,
            batch_size=batch_size,
            retry_attempts=retry_attempts
        )

    def create_portfolio_simulator_service(self, portfolio_repository, asset_repository):
        """
        Create portfolio simulator service with configuration.

        Args:
            portfolio_repository: Portfolio repository instance
            asset_repository: Asset repository instance

        Returns:
            Configured PortfolioSimulatorService
        """
        from ..application.services import PortfolioSimulatorService

        portfolio_config = self.get_portfolio_config()
        self._logger.info(f"Creating portfolio simulator service with config: {portfolio_config.simulation}")

        service = PortfolioSimulatorService(
            portfolio_repository=portfolio_repository,
            asset_repository=asset_repository
        )

        # Inject configuration into service for use in business logic
        service._config = portfolio_config

        return service

    def create_strategy_score_service(self, strategy_calculators, asset_repository):
        """
        Create strategy score service with configuration.

        Args:
            strategy_calculators: Dictionary of strategy calculators
            asset_repository: Asset repository instance

        Returns:
            Configured StrategyScoreService
        """
        from ..application.services import StrategyScoreService

        strategies_config = self.validated_config.strategies
        self._logger.info(f"Creating strategy score service with enabled strategies: {strategies_config.scoring.enabled_strategies}")

        service = StrategyScoreService(
            strategy_calculators=strategy_calculators,
            asset_repository=asset_repository
        )

        # Inject configuration
        service._config = strategies_config

        return service

    def create_event_bus(self):
        """
        Create event bus with configuration.

        Returns:
            Configured event bus instance
        """
        # This would create an event bus when implemented
        # For now, return configuration for manual setup
        event_config = self.get_event_system_config()
        self._logger.info(f"Event bus configuration: max_concurrent={event_config.bus.max_concurrent_events}")

        return {
            'max_concurrent_events': event_config.bus.max_concurrent_events,
            'error_isolation': event_config.bus.error_isolation,
            'enable_logging': event_config.bus.enable_logging,
            'handler_timeout': event_config.handlers.timeout_seconds,
            'retry_attempts': event_config.handlers.retry_attempts,
            'retry_delay': event_config.handlers.retry_delay
        }

    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration for setup.

        Returns:
            Dictionary with logging configuration
        """
        logging_config = self.validated_config.logging

        return {
            'level': logging_config.level,
            'format': logging_config.format,
            'handlers': {
                name: {
                    'enabled': handler.enabled,
                    'path': handler.path,
                    'max_size': handler.max_size,
                    'backup_count': handler.backup_count
                }
                for name, handler in logging_config.handlers.items()
            }
        }

    def get_connection_parameters(self) -> Dict[str, Any]:
        """
        Get database connection parameters from configuration.

        Returns:
            Dictionary with connection parameters
        """
        db_config = self.get_database_config()

        return {
            'database_path': ":memory:" if db_config.connection.memory else db_config.connection.database_path,
            'read_only': db_config.connection.read_only,
            'pragmas': db_config.connection.pragmas,
            'pool_config': {
                'max_connections': db_config.pool.max_connections,
                'connection_timeout': db_config.pool.connection_timeout
            }
        }

    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.config_manager.get_environment() == "development"

    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.config_manager.is_production()

    def is_testing(self) -> bool:
        """Check if running in testing mode."""
        return self.config_manager.is_testing()


class ConfiguredServiceBuilder:
    """
    Builder pattern for creating services with configuration dependencies.

    This class provides a fluent interface for building services with
    their required dependencies injected from configuration.
    """

    def __init__(self):
        """Initialize service builder."""
        self.factory = ConfiguredComponentFactory()
        self._logger = logging.getLogger(__name__)

    def build_complete_service_stack(self):
        """
        Build complete service stack with all dependencies configured.

        Returns:
            Dictionary containing all configured services and repositories
        """
        self._logger.info("Building complete service stack")

        # Create repository factory
        repo_factory = self.factory.create_repository_factory()

        # Create repositories
        asset_repository = repo_factory.create_asset_repository()
        portfolio_repository = repo_factory.create_portfolio_repository()

        # Create services
        services = {}

        # Portfolio simulator service
        services['portfolio_simulator'] = self.factory.create_portfolio_simulator_service(
            portfolio_repository, asset_repository
        )

        # Strategy score service (with empty calculators for now)
        services['strategy_scorer'] = self.factory.create_strategy_score_service(
            {}, asset_repository
        )

        # Event bus configuration
        services['event_config'] = self.factory.create_event_bus()

        self._logger.info("Service stack built successfully")

        return {
            'repositories': {
                'asset': asset_repository,
                'portfolio': portfolio_repository
            },
            'services': services,
            'factory': repo_factory,
            'config': self.factory
        }


# Global factory instance
try:
    component_factory = ConfiguredComponentFactory()
except Exception as e:
    logging.getLogger(__name__).error(f"Failed to initialize component factory: {e}")
    component_factory = None
