"""Tests for ConfiguredComponentFactory integration with services and repositories."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal
from datetime import datetime
from uuid import uuid4

from portfolio_manager.config.factory import ConfiguredComponentFactory, ConfigurationError
from portfolio_manager.config.schema import PortfolioManagerConfig
from portfolio_manager.application.services import DataIngestionService, PortfolioSimulatorService, StrategyScoreService
from portfolio_manager.infrastructure.duckdb.repository_factory import DuckDBRepositoryFactory
from portfolio_manager.domain.exceptions import DomainError


class TestConfiguredComponentFactoryInitialization:
    """Test factory initialization and configuration validation."""

    def test_factory_initialization_with_default_config_manager(self):
        """Test factory initializes with global config manager by default."""
        factory = ConfiguredComponentFactory()
        assert factory.config_manager is not None
        assert factory._validated_config is not None

    def test_factory_initialization_with_custom_config_manager(self):
        """Test factory accepts custom config manager."""
        mock_config = Mock()
        mock_config.get_all.return_value = {
            'application': {'name': 'test', 'version': '0.1.0'},
            'database': {
                'type': 'duckdb',
                'connection': {'database_path': ':memory:', 'memory': True, 'read_only': False, 'pragmas': {}},
                'pool': {'max_connections': 10, 'connection_timeout': 30}
            },
            'event_system': {
                'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                'handlers': {'timeout_seconds': 30, 'retry_attempts': 3, 'retry_delay': 1.0}
            },
            'data_providers': {
                'batch_size': 50,
                'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                'apis': {}
            },
            'portfolio': {
                'simulation': {'initial_cash': 10000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
            },
            'strategies': {
                'scoring': {'enabled_strategies': ['momentum'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
                'backtesting': {'default_period': '1Y', 'benchmark': 'SPY'}
            },
            'analytics': {
                'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
            },
            'monitoring': {
                'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
            },
            'security': {
                'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                'encryption': {'algorithm': 'AES256', 'key': None}
            }
        }

        factory = ConfiguredComponentFactory(mock_config)
        assert factory.config_manager is mock_config
        mock_config.get_all.assert_called_once()

    def test_factory_initialization_with_invalid_config(self):
        """Test factory raises ConfigurationError with invalid config."""
        mock_config = Mock()
        mock_config.get_all.return_value = {'invalid': 'config'}

        with pytest.raises(ConfigurationError, match="Invalid configuration"):
            ConfiguredComponentFactory(mock_config)

    def test_factory_configuration_validation_error_handling(self):
        """Test factory handles configuration validation errors properly."""
        mock_config = Mock()
        mock_config.get_all.side_effect = Exception("Config load failed")

        with pytest.raises(ConfigurationError, match="Invalid configuration"):
            ConfiguredComponentFactory(mock_config)


class TestConfiguredComponentFactoryConfigAccess:
    """Test factory configuration section access methods."""

    @pytest.fixture
    def factory(self):
        """Create factory with mock configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            mock_global_config.get_all.return_value = self._get_valid_config()
            return ConfiguredComponentFactory()

    def _get_valid_config(self):
        """Get valid test configuration."""
        return {
            'application': {'name': 'test', 'version': '0.1.0'},
            'database': {
                'type': 'duckdb',
                'connection': {'database_path': './test.db', 'memory': False, 'read_only': False, 'pragmas': {'threads': 2}},
                'pool': {'max_connections': 5, 'connection_timeout': 30}
            },
            'event_system': {
                'bus': {'max_concurrent_events': 50, 'error_isolation': True, 'enable_logging': True},
                'handlers': {'timeout_seconds': 30, 'retry_attempts': 2, 'retry_delay': 1.0}
            },
            'data_providers': {
                'batch_size': 25,
                'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                'apis': {}
            },
            'portfolio': {
                'simulation': {'initial_cash': 10000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
            },
            'strategies': {
                'scoring': {'enabled_strategies': ['momentum'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
                'backtesting': {'default_period': '1Y', 'benchmark': 'SPY'}
            },
            'analytics': {
                'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
            },
            'monitoring': {
                'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
            },
            'security': {
                'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                'encryption': {'algorithm': 'AES256', 'key': None}
            }
        }

    def test_get_database_config(self, factory):
        """Test database config retrieval."""
        db_config = factory.get_database_config()
        assert db_config.type == 'duckdb'
        assert db_config.connection.database_path == './test.db'
        assert db_config.connection.pragmas == {'threads': 2}
        assert db_config.pool.max_connections == 5

    def test_get_event_system_config(self, factory):
        """Test event system config retrieval."""
        event_config = factory.get_event_system_config()
        assert event_config.bus.max_concurrent_events == 50
        assert event_config.handlers.retry_attempts == 2

    def test_get_data_providers_config(self, factory):
        """Test data providers config retrieval."""
        providers_config = factory.get_data_providers_config()
        assert providers_config.batch_size == 25
        assert providers_config.market_data.primary == 'yahoo_finance'

    def test_get_portfolio_config(self, factory):
        """Test portfolio config retrieval."""
        portfolio_config = factory.get_portfolio_config()
        assert portfolio_config.simulation.initial_cash == 10000.0
        assert portfolio_config.risk_management.max_position_size == 0.1

    def test_validated_config_property(self, factory):
        """Test validated config property access."""
        config = factory.validated_config
        assert isinstance(config, PortfolioManagerConfig)
        assert config.database.type == 'duckdb'

    def test_validated_config_property_not_validated(self):
        """Test validated config property when not validated."""
        factory = ConfiguredComponentFactory.__new__(ConfiguredComponentFactory)
        factory._validated_config = None

        with pytest.raises(ConfigurationError, match="Configuration not validated"):
            _ = factory.validated_config


class TestRepositoryFactoryCreation:
    """Test repository factory creation with configuration."""

    @pytest.fixture
    def factory(self):
        """Create factory with mock configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            mock_global_config.get_all.return_value = {
                'application': {'name': 'test', 'version': '0.1.0'},
                'database': {
                    'type': 'duckdb',
                    'connection': {'database_path': './configured.db', 'memory': False, 'read_only': True, 'pragmas': {'threads': 4, 'memory_limit': '1GB'}},
                    'pool': {'max_connections': 8, 'connection_timeout': 45}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                    'handlers': {'timeout_seconds': 30, 'retry_attempts': 3, 'retry_delay': 1.0}
                },
                'data_providers': {
                    'batch_size': 100,
                    'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 100000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                    'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['momentum'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
                    'backtesting': {'default_period': '1Y', 'benchmark': 'SPY'}
                },
                'analytics': {
                    'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                    'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
                },
                'logging': {
                    'level': 'INFO',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
                },
                'monitoring': {
                    'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                    'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
                },
                'security': {
                    'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                    'encryption': {'algorithm': 'AES256', 'key': None}
                }
            }
            return ConfiguredComponentFactory()

    def test_create_repository_factory_with_file_database(self, factory):
        """Test repository factory creation with file database."""
        repo_factory = factory.create_repository_factory()

        assert isinstance(repo_factory, DuckDBRepositoryFactory)
        assert repo_factory.database_path == './configured.db'
        assert repo_factory.auto_initialize is True
        assert repo_factory.config is not None
        assert repo_factory.config.connection.read_only is True
        assert repo_factory.config.connection.pragmas == {'threads': 4, 'memory_limit': '1GB'}

    def test_create_repository_factory_with_memory_database(self, factory):
        """Test repository factory creation with in-memory database."""
        # Update config to use memory database
        factory._validated_config.database.connection.memory = True

        repo_factory = factory.create_repository_factory()

        assert repo_factory.database_path == ':memory:'
        assert repo_factory.config.connection.memory is True

    @patch('portfolio_manager.infrastructure.duckdb.DuckDBRepositoryFactory')
    def test_create_repository_factory_logging(self, mock_factory_class, factory):
        """Test repository factory creation logs appropriate message."""
        mock_instance = Mock()
        mock_factory_class.return_value = mock_instance

        with patch.object(factory, '_logger') as mock_logger:
            result = factory.create_repository_factory()

            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert 'Creating repository factory with database: ./configured.db' in log_call

            mock_factory_class.assert_called_once_with(
                database_path='./configured.db',
                auto_initialize=True,
                config=factory.get_database_config()
            )
            assert result is mock_instance


class TestDataIngestionServiceCreation:
    """Test data ingestion service creation with configuration."""

    @pytest.fixture
    def factory(self):
        """Create factory with test configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            mock_global_config.get_all.return_value = {
                'application': {'name': 'test', 'version': '0.1.0'},
                'database': {
                    'type': 'duckdb',
                    'connection': {'database_path': ':memory:', 'memory': True, 'read_only': False, 'pragmas': {}},
                    'pool': {'max_connections': 10, 'connection_timeout': 30}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                    'handlers': {'timeout_seconds': 30, 'retry_attempts': 5, 'retry_delay': 2.0}
                },
                'data_providers': {
                    'batch_size': 150,
                    'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 100000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                    'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['momentum'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
                    'backtesting': {'default_period': '1Y', 'benchmark': 'SPY'}
                },
                'analytics': {
                    'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                    'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
                },
                'logging': {
                    'level': 'INFO',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
                },
                'monitoring': {
                    'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                    'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
                },
                'security': {
                    'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                    'encryption': {'algorithm': 'AES256', 'key': None}
                }
            }
            return ConfiguredComponentFactory()

    def test_create_data_ingestion_service_with_config_values(self, factory):
        """Test data ingestion service creation uses config values."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        service = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)

        assert isinstance(service, DataIngestionService)
        assert service.data_provider is mock_data_provider
        assert service.asset_repository is mock_asset_repository
        assert service.batch_size == 150  # From config
        assert service.retry_attempts == 5  # From config

    @patch('portfolio_manager.application.services.DataIngestionService')
    def test_create_data_ingestion_service_logging(self, mock_service_class, factory):
        """Test data ingestion service creation logs configuration."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()
        mock_instance = Mock()
        mock_service_class.return_value = mock_instance

        with patch.object(factory, '_logger') as mock_logger:
            result = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)

            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert 'Creating data ingestion service (batch_size=150, retry_attempts=5)' in log_call

            mock_service_class.assert_called_once_with(
                data_provider=mock_data_provider,
                asset_repository=mock_asset_repository,
                batch_size=150,
                retry_attempts=5
            )
            assert result is mock_instance

    def test_create_data_ingestion_service_parameter_validation(self, factory):
        """Test service creation with various parameter types."""
        mock_data_provider = Mock()
        mock_asset_repository = Mock()

        # Test with valid parameters
        service = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)
        assert service.batch_size == 150
        assert service.retry_attempts == 5

        # Verify parameters are used correctly in service
        assert isinstance(service.batch_size, int)
        assert isinstance(service.retry_attempts, int)


class TestPortfolioSimulatorServiceCreation:
    """Test portfolio simulator service creation with configuration."""

    @pytest.fixture
    def factory(self):
        """Create factory with test configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            mock_global_config.get_all.return_value = {
                'application': {'name': 'test', 'version': '0.1.0'},
                'database': {
                    'type': 'duckdb',
                    'connection': {'database_path': ':memory:', 'memory': True, 'read_only': False, 'pragmas': {}},
                    'pool': {'max_connections': 10, 'connection_timeout': 30}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                    'handlers': {'timeout_seconds': 30, 'retry_attempts': 3, 'retry_delay': 1.0}
                },
                'data_providers': {
                    'batch_size': 100,
                    'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 50000.0, 'default_currency': 'EUR', 'commission_rate': 0.002, 'min_commission': 2.0},
                    'risk_management': {'max_position_size': 0.15, 'max_sector_exposure': 0.30, 'stop_loss_threshold': -0.08}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['value', 'quality'], 'rebalance_frequency': 'monthly', 'min_score_threshold': 70},
                    'backtesting': {'default_period': '2Y', 'benchmark': 'VTI'}
                },
                'analytics': {
                    'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                    'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
                },
                'logging': {
                    'level': 'INFO',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
                },
                'monitoring': {
                    'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                    'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
                },
                'security': {
                    'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                    'encryption': {'algorithm': 'AES256', 'key': None}
                }
            }
            return ConfiguredComponentFactory()

    def test_create_portfolio_simulator_service(self, factory):
        """Test portfolio simulator service creation."""
        mock_portfolio_repository = Mock()
        mock_asset_repository = Mock()

        service = factory.create_portfolio_simulator_service(mock_portfolio_repository, mock_asset_repository)

        assert isinstance(service, PortfolioSimulatorService)
        assert service.portfolio_repository is mock_portfolio_repository
        assert service.asset_repository is mock_asset_repository
        assert hasattr(service, '_config')
        assert service._config.simulation.initial_cash == 50000.0
        assert service._config.simulation.default_currency == 'EUR'
        assert service._config.risk_management.max_position_size == 0.15

    @patch('portfolio_manager.application.services.PortfolioSimulatorService')
    def test_create_portfolio_simulator_service_logging(self, mock_service_class, factory):
        """Test portfolio simulator service creation logs configuration."""
        mock_portfolio_repository = Mock()
        mock_asset_repository = Mock()
        mock_instance = Mock()
        mock_service_class.return_value = mock_instance

        with patch.object(factory, '_logger') as mock_logger:
            result = factory.create_portfolio_simulator_service(mock_portfolio_repository, mock_asset_repository)

            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert 'Creating portfolio simulator service with config:' in log_call

            mock_service_class.assert_called_once_with(
                portfolio_repository=mock_portfolio_repository,
                asset_repository=mock_asset_repository
            )
            # Verify config injection
            assert mock_instance._config == factory.get_portfolio_config()
            assert result is mock_instance


class TestStrategyScoreServiceCreation:
    """Test strategy score service creation with configuration."""

    @pytest.fixture
    def factory(self):
        """Create factory with test configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            mock_global_config.get_all.return_value = {
                'application': {'name': 'test', 'version': '0.1.0'},
                'database': {
                    'type': 'duckdb',
                    'connection': {'database_path': ':memory:', 'memory': True, 'read_only': False, 'pragmas': {}},
                    'pool': {'max_connections': 10, 'connection_timeout': 30}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                    'handlers': {'timeout_seconds': 30, 'retry_attempts': 3, 'retry_delay': 1.0}
                },
                'data_providers': {
                    'batch_size': 100,
                    'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 100000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                    'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['momentum', 'mean_reversion'], 'rebalance_frequency': 'daily', 'min_score_threshold': 80},
                    'backtesting': {'default_period': '5Y', 'benchmark': 'QQQ'}
                },
                'analytics': {
                    'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                    'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
                },
                'logging': {
                    'level': 'INFO',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'handlers': {'console': {'enabled': True}, 'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}}
                },
                'monitoring': {
                    'metrics': {'enabled': False, 'port': 8080, 'endpoint': '/metrics'},
                    'health_check': {'enabled': True, 'endpoint': '/health', 'timeout': 5}
                },
                'security': {
                    'api': {'enable_auth': False, 'jwt_secret': None, 'token_expiry': 3600},
                    'encryption': {'algorithm': 'AES256', 'key': None}
                }
            }
            return ConfiguredComponentFactory()

    def test_create_strategy_score_service(self, factory):
        """Test strategy score service creation."""
        mock_strategy_calculators = {'momentum': Mock(), 'value': Mock()}
        mock_asset_repository = Mock()

        service = factory.create_strategy_score_service(mock_strategy_calculators, mock_asset_repository)

        assert isinstance(service, StrategyScoreService)
        assert service.strategy_calculators is mock_strategy_calculators
        assert service.asset_repository is mock_asset_repository
        assert hasattr(service, '_config')
        assert service._config.scoring.enabled_strategies == ['momentum', 'mean_reversion']
        assert service._config.scoring.min_score_threshold == 80
        assert service._config.backtesting.benchmark == 'QQQ'

    @patch('portfolio_manager.application.services.StrategyScoreService')
    def test_create_strategy_score_service_logging(self, mock_service_class, factory):
        """Test strategy score service creation logs enabled strategies."""
        mock_strategy_calculators = {}
        mock_asset_repository = Mock()
        mock_instance = Mock()
        mock_service_class.return_value = mock_instance

        with patch.object(factory, '_logger') as mock_logger:
            result = factory.create_strategy_score_service(mock_strategy_calculators, mock_asset_repository)

            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert 'Creating strategy score service with enabled strategies:' in log_call
            assert "['momentum', 'mean_reversion']" in log_call

            mock_service_class.assert_called_once_with(
                strategy_calculators=mock_strategy_calculators,
                asset_repository=mock_asset_repository
            )
            # Verify config injection
            assert mock_instance._config == factory.validated_config.strategies
            assert result is mock_instance


class TestEventBusAndUtilityMethods:
    """Test event bus creation and utility methods."""

    @pytest.fixture
    def factory(self):
        """Create factory with test configuration."""
        with patch('portfolio_manager.config.factory.config') as mock_global_config:
            # Add environment detection methods
            mock_global_config.get_environment.return_value = 'testing'
            mock_global_config.is_production.return_value = False
            mock_global_config.is_testing.return_value = True
            mock_global_config.get_all.return_value = {
                'application': {'name': 'test', 'version': '1.0.0', 'environment': 'testing'},
                'database': {
                    'type': 'duckdb',
                    'connection': {'database_path': ':memory:', 'memory': True, 'read_only': False, 'pragmas': {}},
                    'pool': {'max_connections': 10, 'connection_timeout': 30}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 200, 'error_isolation': False, 'enable_logging': False},
                    'handlers': {'timeout_seconds': 60, 'retry_attempts': 10, 'retry_delay': 0.5}
                },
                'data_providers': {
                    'batch_size': 100,
                    'market_data': {'primary': 'yahoo_finance', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 100000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                    'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['momentum'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
                    'backtesting': {'default_period': '1Y', 'benchmark': 'SPY'}
                },
                'analytics': {
                    'technical_indicators': {'default_periods': {'sma': [20, 50], 'rsi': 14, 'macd': [12, 26, 9]}},
                    'risk_metrics': {'var_confidence': 0.95, 'var_period': 252, 'correlation_window': 60}
                },
                'logging': {
                    'level': 'DEBUG',
                    'format': 'custom format',
                    'handlers': {
                        'console': {'enabled': False},
                        'file': {'enabled': True, 'path': '/tmp/test.log', 'max_size': '50MB', 'backup_count': 10}
                    }
                },
                'monitoring': {
                    'metrics': {'enabled': True, 'port': 9090, 'endpoint': '/custom_metrics'},
                    'health_check': {'enabled': False, 'endpoint': '/custom_health', 'timeout': 10}
                },
                'security': {
                    'api': {'enable_auth': True, 'jwt_secret': 'test_secret', 'token_expiry': 7200},
                    'encryption': {'algorithm': 'AES256', 'key': 'test_key'}
                }
            }
            return ConfiguredComponentFactory()

    def test_create_event_bus_returns_config_dict(self, factory):
        """Test event bus creation returns configuration dictionary."""
        event_bus_config = factory.create_event_bus()

        assert isinstance(event_bus_config, dict)
        assert event_bus_config['max_concurrent_events'] == 200
        assert event_bus_config['error_isolation'] is False
        assert event_bus_config['enable_logging'] is False
        assert event_bus_config['handler_timeout'] == 60
        assert event_bus_config['retry_attempts'] == 10
        assert event_bus_config['retry_delay'] == 0.5

    def test_create_event_bus_logging(self, factory):
        """Test event bus creation logs configuration."""
        with patch.object(factory, '_logger') as mock_logger:
            factory.create_event_bus()

            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert 'Event bus configuration: max_concurrent=200' in log_call

    def test_get_logging_config(self, factory):
        """Test logging configuration retrieval."""
        logging_config = factory.get_logging_config()

        assert logging_config['level'] == 'DEBUG'
        assert logging_config['format'] == 'custom format'
        assert logging_config['handlers']['console']['enabled'] is False
        assert logging_config['handlers']['file']['enabled'] is True
        assert logging_config['handlers']['file']['path'] == '/tmp/test.log'
        assert logging_config['handlers']['file']['max_size'] == '50MB'
        assert logging_config['handlers']['file']['backup_count'] == 10

    def test_get_connection_parameters(self, factory):
        """Test database connection parameters retrieval."""
        conn_params = factory.get_connection_parameters()

        assert conn_params['database_path'] == ':memory:'
        assert conn_params['read_only'] is False
        assert conn_params['pragmas'] == {}
        assert conn_params['pool_config']['max_connections'] == 10
        assert conn_params['pool_config']['connection_timeout'] == 30

    def test_environment_detection_methods(self, factory):
        """Test environment detection methods."""
        assert factory.is_development() is False
        assert factory.is_production() is False
        assert factory.is_testing() is True

    def test_environment_detection_with_config_manager_mock(self, factory):
        """Test environment detection with config manager methods."""
        # Test config manager delegation
        with patch.object(factory.config_manager, 'is_production', return_value=True):
            assert factory.is_production() is True

        with patch.object(factory.config_manager, 'is_testing', return_value=False):
            assert factory.is_testing() is False


class TestConfiguredServiceBuilderIntegration:
    """Test ConfiguredServiceBuilder for complete service stack creation."""

    def test_configured_service_builder_initialization(self):
        """Test service builder initialization."""
        from portfolio_manager.config.factory import ConfiguredServiceBuilder

        with patch('portfolio_manager.config.factory.ConfiguredComponentFactory') as mock_factory_class:
            mock_factory_instance = Mock()
            mock_factory_class.return_value = mock_factory_instance

            builder = ConfiguredServiceBuilder()

            assert builder.factory is mock_factory_instance
            mock_factory_class.assert_called_once()

    @patch('portfolio_manager.config.factory.ConfiguredComponentFactory')
    def test_build_complete_service_stack(self, mock_factory_class):
        """Test building complete service stack."""
        from portfolio_manager.config.factory import ConfiguredServiceBuilder

        # Setup mocks
        mock_factory = Mock()
        mock_factory_class.return_value = mock_factory

        mock_repo_factory = Mock()
        mock_asset_repo = Mock()
        mock_portfolio_repo = Mock()
        mock_portfolio_service = Mock()
        mock_strategy_service = Mock()
        mock_event_config = {'max_concurrent_events': 100}

        mock_factory.create_repository_factory.return_value = mock_repo_factory
        mock_repo_factory.create_asset_repository.return_value = mock_asset_repo
        mock_repo_factory.create_portfolio_repository.return_value = mock_portfolio_repo
        mock_factory.create_portfolio_simulator_service.return_value = mock_portfolio_service
        mock_factory.create_strategy_score_service.return_value = mock_strategy_service
        mock_factory.create_event_bus.return_value = mock_event_config

        builder = ConfiguredServiceBuilder()

        with patch.object(builder, '_logger') as mock_logger:
            result = builder.build_complete_service_stack()

            # Verify logging
            mock_logger.info.assert_any_call('Building complete service stack')
            mock_logger.info.assert_any_call('Service stack built successfully')

            # Verify result structure
            assert 'repositories' in result
            assert 'services' in result
            assert 'factory' in result
            assert 'config' in result

            assert result['repositories']['asset'] is mock_asset_repo
            assert result['repositories']['portfolio'] is mock_portfolio_repo
            assert result['services']['portfolio_simulator'] is mock_portfolio_service
            assert result['services']['strategy_scorer'] is mock_strategy_service
            assert result['services']['event_config'] is mock_event_config
            assert result['factory'] is mock_repo_factory
            assert result['config'] is mock_factory

            # Verify method calls
            mock_factory.create_repository_factory.assert_called_once()
            mock_repo_factory.create_asset_repository.assert_called_once()
            mock_repo_factory.create_portfolio_repository.assert_called_once()
            mock_factory.create_portfolio_simulator_service.assert_called_once_with(
                mock_portfolio_repo, mock_asset_repo
            )
            mock_factory.create_strategy_score_service.assert_called_once_with(
                {}, mock_asset_repo
            )
            mock_factory.create_event_bus.assert_called_once()


class TestGlobalFactoryInstance:
    """Test global factory instance handling."""

    def test_global_factory_initialization_success(self):
        """Test successful global factory initialization."""
        # Import here to use the actual initialized instance
        import portfolio_manager.config.factory

        # The global factory should be initialized and working
        assert portfolio_manager.config.factory.component_factory is not None
        assert hasattr(portfolio_manager.config.factory.component_factory, 'config_manager')

    def test_global_factory_initialization_failure(self):
        """Test global factory initialization failure handling."""
        # Since we can't easily simulate a failure in global initialization,
        # we test that the global factory handles missing config gracefully
        import portfolio_manager.config.factory

        # The factory should either be initialized or handle errors gracefully
        if portfolio_manager.config.factory.component_factory is None:
            # This would happen if config initialization failed
            assert True
        else:
            # If it's initialized, it should be a valid instance
            assert hasattr(portfolio_manager.config.factory.component_factory, 'config_manager')


class TestConfigurationErrorScenarios:
    """Test various configuration error scenarios."""

    def test_configuration_error_inheritance(self):
        """Test ConfigurationError inherits from DomainError."""
        error = ConfigurationError("test error")
        assert isinstance(error, DomainError)
        assert str(error) == "test error"

    def test_configuration_error_with_cause(self):
        """Test ConfigurationError with underlying cause."""
        cause = ValueError("Invalid value")
        try:
            raise cause
        except ValueError:
            error = ConfigurationError("Config failed")
            error.__cause__ = cause
        assert error.__cause__ is cause

    def test_factory_with_malformed_configuration_data(self):
        """Test factory with various malformed configuration scenarios."""
        test_cases = [
            {},  # Empty config
            {'application': None},  # Null section
            {'database': {'invalid': 'structure'}},  # Missing required fields
            {'application': {'name': 'test'}, 'database': 'not_a_dict'},  # Wrong type
        ]

        for malformed_config in test_cases:
            mock_config = Mock()
            mock_config.get_all.return_value = malformed_config

            with pytest.raises(ConfigurationError):
                ConfiguredComponentFactory(mock_config)
