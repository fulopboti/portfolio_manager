"""End-to-end integration tests for configuration system."""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import yaml
import os

from stockapp.config.factory import ConfiguredComponentFactory, ConfiguredServiceBuilder
from stockapp.config.settings import ConfigManager
from stockapp.infrastructure.duckdb.repository_factory import DuckDBRepositoryFactory
from stockapp.application.services import DataIngestionService, PortfolioSimulatorService


class TestEndToEndConfigurationIntegration:
    """Test complete configuration system integration from YAML to services."""
    
    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary config directory with test YAML files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create base.yaml
            base_config = {
                'application': {
                    'name': 'TestApp',
                    'version': '1.0.0',
                    'environment': 'testing',
                    'debug': True,
                    'log_level': 'DEBUG'
                },
                'database': {
                    'type': 'duckdb',
                    'connection': {
                        'database_path': ':memory:',
                        'memory': True,
                        'read_only': False,
                        'pragmas': {
                            'threads': 2,
                            'memory_limit': '1GB'
                        }
                    },
                    'pool': {
                        'max_connections': 5,
                        'connection_timeout': 30
                    }
                },
                'event_system': {
                    'bus': {
                        'max_concurrent_events': 50,
                        'error_isolation': True,
                        'enable_logging': True
                    },
                    'handlers': {
                        'timeout_seconds': 30,
                        'retry_attempts': 3,
                        'retry_delay': 1.0
                    }
                },
                'data_providers': {
                    'batch_size': 25,
                    'market_data': {
                        'primary': 'test_provider',
                        'fallback': [],
                        'cache_ttl': 300,
                        'rate_limits': {}
                    },
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {
                        'initial_cash': 10000.0,
                        'default_currency': 'USD',
                        'commission_rate': 0.001,
                        'min_commission': 1.0
                    },
                    'risk_management': {
                        'max_position_size': 0.1,
                        'max_sector_exposure': 0.25,
                        'stop_loss_threshold': -0.05
                    }
                },
                'strategies': {
                    'scoring': {
                        'enabled_strategies': ['test_strategy'],
                        'rebalance_frequency': 'weekly',
                        'min_score_threshold': 60
                    },
                    'backtesting': {
                        'default_period': '1Y',
                        'benchmark': 'SPY'
                    }
                },
                'analytics': {
                    'technical_indicators': {
                        'default_periods': {
                            'sma': [20, 50],
                            'rsi': 14,
                            'macd': [12, 26, 9]
                        }
                    },
                    'risk_metrics': {
                        'var_confidence': 0.95,
                        'var_period': 252,
                        'correlation_window': 60
                    }
                },
                'logging': {
                    'level': 'DEBUG',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'handlers': {
                        'console': {'enabled': True},
                        'file': {'enabled': False, 'path': 'app.log', 'max_size': '10MB', 'backup_count': 5}
                    }
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
            
            with open(config_dir / 'base.yaml', 'w') as f:
                yaml.dump(base_config, f)
            
            # Create testing.yaml with overrides
            testing_config = {
                'database': {
                    'connection': {
                        'pragmas': {
                            'threads': 1,  # Override for testing
                            'memory_limit': '512MB'
                        }
                    },
                    'pool': {
                        'max_connections': 3  # Override for testing
                    }
                },
                'data_providers': {
                    'batch_size': 10  # Override for testing
                },
                'event_system': {
                    'handlers': {
                        'retry_attempts': 1  # Override for testing
                    }
                }
            }
            
            with open(config_dir / 'testing.yaml', 'w') as f:
                yaml.dump(testing_config, f)
            
            yield config_dir
    
    def test_complete_configuration_loading_chain(self, temp_config_dir):
        """Test complete configuration loading from YAML through to services."""
        # Create config manager with temporary directory
        config_manager = ConfigManager(config_dir=temp_config_dir, env_prefix="TEST_APP")
        
        # Verify configuration loaded correctly
        assert config_manager.get("application.name") == "TestApp"
        assert config_manager.get("database.connection.pragmas.threads") == 1  # Testing override
        assert config_manager.get("database.pool.max_connections") == 3  # Testing override
        assert config_manager.get("data_providers.batch_size") == 10  # Testing override
        
        # Create factory with config manager
        factory = ConfiguredComponentFactory(config_manager)
        
        # Test configuration sections
        db_config = factory.get_database_config()
        assert db_config.type == "duckdb"
        assert db_config.connection.memory is True
        assert db_config.connection.pragmas["threads"] == 1
        assert db_config.pool.max_connections == 3
        
        data_providers_config = factory.get_data_providers_config()
        assert data_providers_config.batch_size == 10
        
        event_config = factory.get_event_system_config()
        assert event_config.handlers.retry_attempts == 1
    
    def test_repository_factory_creation_with_full_config_chain(self, temp_config_dir):
        """Test repository factory creation uses configuration from YAML."""
        config_manager = ConfigManager(config_dir=temp_config_dir)
        factory = ConfiguredComponentFactory(config_manager)
        
        repo_factory = factory.create_repository_factory()
        
        assert isinstance(repo_factory, DuckDBRepositoryFactory)
        assert repo_factory.database_path == ":memory:"
        assert repo_factory.auto_initialize is True
        assert repo_factory.config is not None
        assert repo_factory.config.connection.memory is True
        assert repo_factory.config.connection.pragmas["threads"] == 1  # From testing.yaml override
    
    def test_service_creation_with_full_config_chain(self, temp_config_dir):
        """Test service creation uses configuration from YAML."""
        config_manager = ConfigManager(config_dir=temp_config_dir)
        factory = ConfiguredComponentFactory(config_manager)
        
        mock_data_provider = Mock()
        mock_asset_repository = Mock()
        
        service = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)
        
        assert isinstance(service, DataIngestionService)
        assert service.batch_size == 10  # From testing.yaml override
        assert service.retry_attempts == 1  # From testing.yaml override
    
    def test_environment_variable_overrides_in_full_chain(self, temp_config_dir):
        """Test environment variable overrides work in full configuration chain."""
        # Set environment variables with correct structure 
        os.environ["TEST_APP_DATA_PROVIDERS_BATCH_SIZE"] = "99"
        os.environ["TEST_APP_EVENT_SYSTEM_HANDLERS_RETRY_ATTEMPTS"] = "7"
        
        try:
            config_manager = ConfigManager(config_dir=temp_config_dir, env_prefix="TEST_APP")
            factory = ConfiguredComponentFactory(config_manager)
            
            # Verify environment overrides applied
            data_providers_config = factory.get_data_providers_config()
            assert data_providers_config.batch_size == 99  # From environment variable
            
            event_config = factory.get_event_system_config()
            assert event_config.handlers.retry_attempts == 7  # From environment variable
            
            # Test service gets environment values
            mock_data_provider = Mock()
            mock_asset_repository = Mock()
            service = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)
            
            assert service.batch_size == 99
            assert service.retry_attempts == 7
            
        finally:
            # Clean up environment variables
            os.environ.pop("TEST_APP_DATA_PROVIDERS_BATCH_SIZE", None)
            os.environ.pop("TEST_APP_EVENT_SYSTEM_HANDLERS_RETRY_ATTEMPTS", None)
    
    def test_complete_service_stack_creation(self, temp_config_dir):
        """Test complete service stack creation with configuration."""
        config_manager = ConfigManager(config_dir=temp_config_dir)
        
        with patch('stockapp.config.factory.ConfiguredComponentFactory') as mock_factory_class:
            mock_factory = Mock()
            mock_factory_class.return_value = mock_factory
            
            # Setup factory to use our config manager
            real_factory = ConfiguredComponentFactory(config_manager)
            mock_factory.get_database_config.return_value = real_factory.get_database_config()
            mock_factory.get_data_providers_config.return_value = real_factory.get_data_providers_config()
            mock_factory.get_event_system_config.return_value = real_factory.get_event_system_config()
            mock_factory.get_portfolio_config.return_value = real_factory.get_portfolio_config()
            mock_factory.validated_config = real_factory.validated_config
            
            # Mock repository factory and services
            mock_repo_factory = Mock()
            mock_asset_repo = Mock()
            mock_portfolio_repo = Mock()
            mock_portfolio_service = Mock()
            mock_strategy_service = Mock()
            
            mock_factory.create_repository_factory.return_value = mock_repo_factory
            mock_repo_factory.create_asset_repository.return_value = mock_asset_repo
            mock_repo_factory.create_portfolio_repository.return_value = mock_portfolio_repo
            mock_factory.create_portfolio_simulator_service.return_value = mock_portfolio_service
            mock_factory.create_strategy_score_service.return_value = mock_strategy_service
            mock_factory.create_event_bus.return_value = {"max_concurrent_events": 50}
            
            builder = ConfiguredServiceBuilder()
            result = builder.build_complete_service_stack()
            
            # Verify complete service stack
            assert 'repositories' in result
            assert 'services' in result
            assert 'factory' in result
            assert 'config' in result
            
            # Verify configuration was used in service creation
            mock_factory.create_repository_factory.assert_called_once()
            mock_factory.create_portfolio_simulator_service.assert_called_once()
            mock_factory.create_strategy_score_service.assert_called_once()


class TestConfigurationErrorRecoveryIntegration:
    """Test configuration error recovery in integration scenarios."""
    
    def test_missing_config_files_fallback(self):
        """Test system handles missing configuration files gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            empty_config_dir = Path(temp_dir) / "empty"
            empty_config_dir.mkdir()
            
            # Should raise ConfigurationError due to missing base.yaml
            with pytest.raises(Exception):  # ConfigurationError from missing base.yaml
                ConfigManager(config_dir=empty_config_dir)
    
    def test_malformed_yaml_error_handling(self):
        """Test system handles malformed YAML gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create malformed YAML
            with open(config_dir / 'base.yaml', 'w') as f:
                f.write("invalid: yaml: content: [\n")  # Malformed YAML
            
            with pytest.raises(Exception):  # YAML parsing error
                ConfigManager(config_dir=config_dir)
    
    def test_partial_config_validation_errors(self):
        """Test system handles partial configuration validation errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create config missing required fields
            partial_config = {
                'application': {'name': 'TestApp'},
                'database': {'type': 'duckdb'}  # Missing required connection info
            }
            
            with open(config_dir / 'base.yaml', 'w') as f:
                yaml.dump(partial_config, f)
            
            config_manager = ConfigManager(config_dir=config_dir)
            
            # Factory should fail validation
            with pytest.raises(Exception):  # Configuration validation error
                ConfiguredComponentFactory(config_manager)


class TestConfigurationPerformanceIntegration:
    """Test configuration system performance in integration scenarios."""
    
    @pytest.fixture
    def large_config_dir(self):
        """Create config directory with large configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create large base config
            large_config = {
                'application': {'name': 'LargeApp', 'version': '1.0.0'},
                'database': {
                    'type': 'duckdb',
                    'connection': {
                        'database_path': ':memory:',
                        'memory': True,
                        'read_only': False,
                        'pragmas': {f'setting_{i}': f'value_{i}' for i in range(100)}
                    },
                    'pool': {'max_connections': 10, 'connection_timeout': 30}
                },
                'event_system': {
                    'bus': {'max_concurrent_events': 100, 'error_isolation': True, 'enable_logging': True},
                    'handlers': {'timeout_seconds': 30, 'retry_attempts': 3, 'retry_delay': 1.0}
                },
                'data_providers': {
                    'batch_size': 100,
                    'market_data': {'primary': 'test', 'fallback': [], 'cache_ttl': 300, 'rate_limits': {}},
                    'apis': {}
                },
                'portfolio': {
                    'simulation': {'initial_cash': 100000.0, 'default_currency': 'USD', 'commission_rate': 0.001, 'min_commission': 1.0},
                    'risk_management': {'max_position_size': 0.1, 'max_sector_exposure': 0.25, 'stop_loss_threshold': -0.05}
                },
                'strategies': {
                    'scoring': {'enabled_strategies': ['test'], 'rebalance_frequency': 'weekly', 'min_score_threshold': 60},
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
            
            with open(config_dir / 'base.yaml', 'w') as f:
                yaml.dump(large_config, f)
            
            yield config_dir
    
    def test_large_config_loading_performance(self, large_config_dir):
        """Test configuration loading performance with large config files."""
        import time
        
        start_time = time.time()
        
        config_manager = ConfigManager(config_dir=large_config_dir)
        factory = ConfiguredComponentFactory(config_manager)
        
        # Create multiple services to test repeated config access
        mock_data_provider = Mock()
        mock_asset_repository = Mock()
        mock_portfolio_repository = Mock()
        
        service1 = factory.create_data_ingestion_service(mock_data_provider, mock_asset_repository)
        service2 = factory.create_portfolio_simulator_service(mock_portfolio_repository, mock_asset_repository)
        repo_factory = factory.create_repository_factory()
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete reasonably quickly (less than 1 second)
        assert elapsed < 1.0
        
        # Verify services were created correctly
        assert isinstance(service1, DataIngestionService)
        assert isinstance(service2, PortfolioSimulatorService)
        assert isinstance(repo_factory, DuckDBRepositoryFactory)
    
    def test_repeated_config_access_caching(self, large_config_dir):
        """Test configuration access is efficiently cached."""
        config_manager = ConfigManager(config_dir=large_config_dir)
        factory = ConfiguredComponentFactory(config_manager)
        
        import time
        
        # First access
        start_time = time.time()
        config1 = factory.get_database_config()
        first_access_time = time.time() - start_time
        
        # Repeated accesses should be faster (cached)
        times = []
        for _ in range(10):
            start_time = time.time()
            config = factory.get_database_config()
            elapsed = time.time() - start_time
            times.append(elapsed)
            
            # Verify same object returned (cached)
            assert config is config1
        
        avg_cached_time = sum(times) / len(times)
        
        # Cached accesses should be faster than or equal to first access 
        # (on fast systems both might be near 0, so we just verify caching works by object identity)
        assert avg_cached_time <= first_access_time or (first_access_time < 0.01 and avg_cached_time < 0.01)