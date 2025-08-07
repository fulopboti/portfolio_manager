"""Tests for configuration schema validation with Pydantic."""

import pytest
from decimal import Decimal
from typing import Dict, Any

from stockapp.config.schema import (
    DatabaseConnectionConfig, DatabasePoolConfig, DatabaseConfig,
    EventBusConfig, EventHandlersConfig, EventSystemConfig,
    APIConfig, MarketDataConfig, DataProvidersConfig,
    PortfolioSimulationConfig, RiskManagementConfig, PortfolioConfig,
    ScoringConfig, BacktestingConfig, StrategiesConfig,
    TechnicalIndicatorsConfig, RiskMetricsConfig, AnalyticsConfig,
    LogHandlerConfig, LoggingConfig,
    MetricsConfig, HealthCheckConfig, MonitoringConfig,
    APISecurityConfig, EncryptionConfig, SecurityConfig,
    ApplicationConfig, StockAppConfig, validate_config
)
from pydantic import ValidationError


class TestDatabaseConfigValidation:
    """Test database configuration schema validation."""
    
    def test_database_connection_config_valid(self):
        """Test valid database connection configuration."""
        config = DatabaseConnectionConfig(
            database_path="./test.db",
            memory=False,
            read_only=True,
            pragmas={"threads": 4, "memory_limit": "2GB"}
        )
        
        assert config.database_path == "./test.db"
        assert config.memory is False
        assert config.read_only is True
        assert config.pragmas["threads"] == 4
        assert config.pragmas["memory_limit"] == "2GB"
    
    def test_database_connection_config_memory_path(self):
        """Test database connection with memory path."""
        config = DatabaseConnectionConfig(
            database_path=":memory:",
            memory=True,
            read_only=False,
            pragmas={}
        )
        
        assert config.database_path == ":memory:"
        assert config.memory is True
        assert config.pragmas == {}
    
    def test_database_connection_config_invalid_empty_path(self):
        """Test database connection with invalid empty path."""
        with pytest.raises(ValidationError) as exc_info:
            DatabaseConnectionConfig(
                database_path="",  # Empty path should be invalid
                memory=False,
                read_only=False,
                pragmas={}
            )
        
        assert "database_path cannot be empty" in str(exc_info.value)
    
    def test_database_pool_config_valid(self):
        """Test valid database pool configuration."""
        config = DatabasePoolConfig(
            max_connections=20,
            connection_timeout=60
        )
        
        assert config.max_connections == 20
        assert config.connection_timeout == 60
    
    def test_database_pool_config_constraints(self):
        """Test database pool configuration constraints."""
        # Test minimum constraint
        with pytest.raises(ValidationError):
            DatabasePoolConfig(max_connections=0)  # Should be >= 1
        
        # Test maximum constraint
        with pytest.raises(ValidationError):
            DatabasePoolConfig(max_connections=101)  # Should be <= 100
        
        # Test timeout constraint
        with pytest.raises(ValidationError):
            DatabasePoolConfig(connection_timeout=0)  # Should be >= 1
    
    def test_database_config_complete(self):
        """Test complete database configuration."""
        config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./complete.db",
                memory=False,
                read_only=False,
                pragmas={"threads": 8, "enable_progress_bar": True}
            ),
            pool=DatabasePoolConfig(
                max_connections=15,
                connection_timeout=45
            )
        )
        
        assert config.type == "duckdb"
        assert config.connection.database_path == "./complete.db"
        assert config.pool.max_connections == 15
        assert config.connection.pragmas["enable_progress_bar"] == True


class TestEventSystemConfigValidation:
    """Test event system configuration schema validation."""
    
    def test_event_bus_config_valid(self):
        """Test valid event bus configuration."""
        config = EventBusConfig(
            max_concurrent_events=200,
            error_isolation=True,
            enable_logging=False
        )
        
        assert config.max_concurrent_events == 200
        assert config.error_isolation is True
        assert config.enable_logging is False
    
    def test_event_bus_config_constraints(self):
        """Test event bus configuration constraints."""
        with pytest.raises(ValidationError):
            EventBusConfig(max_concurrent_events=0)  # Should be >= 1
    
    def test_event_handlers_config_valid(self):
        """Test valid event handlers configuration."""
        config = EventHandlersConfig(
            timeout_seconds=120,
            retry_attempts=5,
            retry_delay=2.5
        )
        
        assert config.timeout_seconds == 120
        assert config.retry_attempts == 5
        assert config.retry_delay == 2.5
    
    def test_event_handlers_config_constraints(self):
        """Test event handlers configuration constraints."""
        with pytest.raises(ValidationError):
            EventHandlersConfig(timeout_seconds=0)  # Should be >= 1
        
        with pytest.raises(ValidationError):
            EventHandlersConfig(retry_attempts=-1)  # Should be >= 0
        
        with pytest.raises(ValidationError):
            EventHandlersConfig(retry_delay=-0.5)  # Should be >= 0
    
    def test_event_system_config_complete(self):
        """Test complete event system configuration."""
        config = EventSystemConfig(
            bus=EventBusConfig(
                max_concurrent_events=150,
                error_isolation=False,
                enable_logging=True
            ),
            handlers=EventHandlersConfig(
                timeout_seconds=90,
                retry_attempts=3,
                retry_delay=1.0
            )
        )
        
        assert config.bus.max_concurrent_events == 150
        assert config.handlers.timeout_seconds == 90
        assert config.handlers.retry_delay == 1.0


class TestDataProvidersConfigValidation:
    """Test data providers configuration schema validation."""
    
    def test_api_config_valid(self):
        """Test valid API configuration."""
        config = APIConfig(
            base_url="https://api.example.com",
            api_key="secret_key",
            timeout=30
        )
        
        assert config.base_url == "https://api.example.com"
        assert config.api_key == "secret_key"
        assert config.timeout == 30
    
    def test_api_config_url_validation(self):
        """Test API configuration URL validation."""
        # Valid URLs should work
        APIConfig(base_url="https://api.example.com", timeout=10)
        APIConfig(base_url="http://localhost:8080", timeout=10)
        
        # Invalid URLs should fail
        with pytest.raises(ValidationError):
            APIConfig(base_url="not-a-url", timeout=10)
        
        with pytest.raises(ValidationError):
            APIConfig(base_url="", timeout=10)  # Empty URL
    
    def test_market_data_config_valid(self):
        """Test valid market data configuration."""
        config = MarketDataConfig(
            primary="yahoo_finance",
            fallback=["alpha_vantage", "iex_cloud"],
            cache_ttl=600,
            rate_limits={"yahoo_finance": 2000, "alpha_vantage": 500}
        )
        
        assert config.primary == "yahoo_finance"
        assert config.fallback == ["alpha_vantage", "iex_cloud"]
        assert config.cache_ttl == 600
        assert config.rate_limits["yahoo_finance"] == 2000
    
    def test_market_data_config_constraints(self):
        """Test market data configuration constraints."""
        with pytest.raises(ValidationError):
            MarketDataConfig(
                primary="test",
                cache_ttl=-1  # Should be >= 0
            )
    
    def test_data_providers_config_complete(self):
        """Test complete data providers configuration."""
        config = DataProvidersConfig(
            batch_size=50,
            market_data=MarketDataConfig(
                primary="test_provider",
                fallback=[],
                cache_ttl=300,
                rate_limits={}
            ),
            apis={
                "test_api": APIConfig(
                    base_url="https://test.api.com",
                    api_key=None,
                    timeout=15
                )
            }
        )
        
        assert config.batch_size == 50
        assert config.market_data.primary == "test_provider"
        assert "test_api" in config.apis
        assert config.apis["test_api"].timeout == 15
    
    def test_data_providers_config_batch_size_constraint(self):
        """Test data providers batch size constraint."""
        with pytest.raises(ValidationError):
            DataProvidersConfig(
                batch_size=0,  # Should be >= 1
                market_data=MarketDataConfig(primary="test"),
                apis={}
            )


class TestPortfolioConfigValidation:
    """Test portfolio configuration schema validation."""
    
    def test_portfolio_simulation_config_valid(self):
        """Test valid portfolio simulation configuration."""
        config = PortfolioSimulationConfig(
            initial_cash=50000.0,
            default_currency="EUR",
            commission_rate=0.002,
            min_commission=2.5
        )
        
        assert config.initial_cash == 50000.0
        assert config.default_currency == "EUR"
        assert config.commission_rate == 0.002
        assert config.min_commission == 2.5
    
    def test_portfolio_simulation_config_constraints(self):
        """Test portfolio simulation configuration constraints."""
        with pytest.raises(ValidationError):
            PortfolioSimulationConfig(initial_cash=-1000.0)  # Should be > 0
        
        with pytest.raises(ValidationError):
            PortfolioSimulationConfig(commission_rate=-0.001)  # Should be >= 0
        
        with pytest.raises(ValidationError):
            PortfolioSimulationConfig(commission_rate=1.1)  # Should be <= 1
    
    def test_portfolio_risk_management_config_valid(self):
        """Test valid portfolio risk management configuration."""
        config = RiskManagementConfig(
            max_position_size=0.15,
            max_sector_exposure=0.30,
            stop_loss_threshold=-0.08
        )
        
        assert config.max_position_size == 0.15
        assert config.max_sector_exposure == 0.30
        assert config.stop_loss_threshold == -0.08
    
    def test_portfolio_config_complete(self):
        """Test complete portfolio configuration."""
        config = PortfolioConfig(
            simulation=PortfolioSimulationConfig(
                initial_cash=75000.0,
                default_currency="USD",
                commission_rate=0.001,
                min_commission=1.0
            ),
            risk_management=RiskManagementConfig(
                max_position_size=0.12,
                max_sector_exposure=0.28,
                stop_loss_threshold=-0.06
            )
        )
        
        assert config.simulation.initial_cash == 75000.0
        assert config.risk_management.max_position_size == 0.12


class TestStrategiesConfigValidation:
    """Test strategies configuration schema validation."""
    
    def test_strategy_scoring_config_valid(self):
        """Test valid strategy scoring configuration."""
        config = ScoringConfig(
            enabled_strategies=["momentum", "value", "quality"],
            rebalance_frequency="weekly",
            min_score_threshold=70
        )
        
        assert config.enabled_strategies == ["momentum", "value", "quality"]
        assert config.rebalance_frequency == "weekly"
        assert config.min_score_threshold == 70
    
    def test_strategy_backtesting_config_valid(self):
        """Test valid strategy backtesting configuration."""
        config = BacktestingConfig(
            default_period="2Y",
            benchmark="VTI"
        )
        
        assert config.default_period == "2Y"
        assert config.benchmark == "VTI"
    
    def test_strategies_config_complete(self):
        """Test complete strategies configuration."""
        config = StrategiesConfig(
            scoring=ScoringConfig(
                enabled_strategies=["momentum"],
                rebalance_frequency="monthly",
                min_score_threshold=80
            ),
            backtesting=BacktestingConfig(
                default_period="3Y",
                benchmark="SPY"
            )
        )
        
        assert config.scoring.enabled_strategies == ["momentum"]
        assert config.backtesting.benchmark == "SPY"


class TestAnalyticsConfigValidation:
    """Test analytics configuration schema validation."""
    
    def test_technical_indicators_config_valid(self):
        """Test valid technical indicators configuration."""
        config = TechnicalIndicatorsConfig(
            default_periods={
                "sma": [10, 20, 50],
                "ema": [12, 26],
                "rsi": 14,
                "macd": [12, 26, 9]
            }
        )
        
        assert config.default_periods["sma"] == [10, 20, 50]
        assert config.default_periods["rsi"] == 14
        assert config.default_periods["macd"] == [12, 26, 9]
    
    def test_risk_metrics_config_valid(self):
        """Test valid risk metrics configuration."""
        config = RiskMetricsConfig(
            var_confidence=0.99,
            var_period=500,
            correlation_window=120
        )
        
        assert config.var_confidence == 0.99
        assert config.var_period == 500
        assert config.correlation_window == 120
    
    def test_analytics_config_complete(self):
        """Test complete analytics configuration."""
        config = AnalyticsConfig(
            technical_indicators=TechnicalIndicatorsConfig(
                default_periods={"sma": [20, 50], "rsi": 14}
            ),
            risk_metrics=RiskMetricsConfig(
                var_confidence=0.95,
                var_period=252,
                correlation_window=60
            )
        )
        
        assert config.technical_indicators.default_periods["sma"] == [20, 50]
        assert config.risk_metrics.var_confidence == 0.95


class TestLoggingConfigValidation:
    """Test logging configuration schema validation."""
    
    def test_logging_handler_config_valid(self):
        """Test valid logging handler configuration."""
        config = LogHandlerConfig(
            enabled=True,
            path="./logs/app.log",
            max_size="50MB",
            backup_count=10
        )
        
        assert config.enabled is True
        assert config.path == "./logs/app.log"
        assert config.max_size == "50MB"
        assert config.backup_count == 10
    
    def test_logging_config_valid(self):
        """Test valid logging configuration."""
        config = LoggingConfig(
            level="DEBUG",
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers={
                "console": LogHandlerConfig(enabled=True),
                "file": LogHandlerConfig(
                    enabled=True,
                    path="./app.log",
                    max_size="10MB",
                    backup_count=5
                )
            }
        )
        
        assert config.level == "DEBUG"
        assert config.handlers["console"].enabled is True
        assert config.handlers["file"].path == "./app.log"
    
    def test_logging_config_level_validation(self):
        """Test logging level validation."""
        # Valid levels should work
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = LoggingConfig(level=level, handlers={"console": LogHandlerConfig(enabled=True)})
            assert config.level == level
        
        # Invalid level should fail
        with pytest.raises(ValidationError):
            LoggingConfig(level="INVALID_LEVEL", handlers={"console": LogHandlerConfig(enabled=True)})


class TestMonitoringConfigValidation:
    """Test monitoring configuration schema validation."""
    
    def test_monitoring_metrics_config_valid(self):
        """Test valid monitoring metrics configuration."""
        config = MetricsConfig(
            enabled=True,
            port=9090,
            endpoint="/custom_metrics"
        )
        
        assert config.enabled is True
        assert config.port == 9090
        assert config.endpoint == "/custom_metrics"
    
    def test_monitoring_health_check_config_valid(self):
        """Test valid monitoring health check configuration."""
        config = HealthCheckConfig(
            enabled=True,
            endpoint="/status",
            timeout=10
        )
        
        assert config.enabled is True
        assert config.endpoint == "/status"
        assert config.timeout == 10
    
    def test_monitoring_config_complete(self):
        """Test complete monitoring configuration."""
        config = MonitoringConfig(
            metrics=MetricsConfig(
                enabled=False,
                port=8080,
                endpoint="/metrics"
            ),
            health_check=HealthCheckConfig(
                enabled=True,
                endpoint="/health",
                timeout=5
            )
        )
        
        assert config.metrics.enabled is False
        assert config.health_check.enabled is True


class TestSecurityConfigValidation:
    """Test security configuration schema validation."""
    
    def test_security_api_config_valid(self):
        """Test valid security API configuration."""
        config = APISecurityConfig(
            enable_auth=True,
            jwt_secret="super_secret_key",
            token_expiry=7200
        )
        
        assert config.enable_auth is True
        assert config.jwt_secret == "super_secret_key"
        assert config.token_expiry == 7200
    
    def test_security_encryption_config_valid(self):
        """Test valid security encryption configuration."""
        config = EncryptionConfig(
            algorithm="AES256",
            key="encryption_key_here"
        )
        
        assert config.algorithm == "AES256"
        assert config.key == "encryption_key_here"
    
    def test_security_config_complete(self):
        """Test complete security configuration."""
        config = SecurityConfig(
            api=APISecurityConfig(
                enable_auth=False,
                jwt_secret=None,
                token_expiry=3600
            ),
            encryption=EncryptionConfig(
                algorithm="AES256",
                key=None
            )
        )
        
        assert config.api.enable_auth is False
        assert config.encryption.algorithm == "AES256"


class TestStockAppConfigValidation:
    """Test complete StockApp configuration validation."""
    
    def test_complete_stock_app_config_valid(self):
        """Test valid complete StockApp configuration."""
        config_dict = {
            "application": {
                "name": "TestApp",
                "version": "1.0.0"
            },
            "database": {
                "type": "duckdb",
                "connection": {
                    "database_path": ":memory:",
                    "memory": True,
                    "read_only": False,
                    "pragmas": {"threads": 2}
                },
                "pool": {
                    "max_connections": 10,
                    "connection_timeout": 30
                }
            },
            "event_system": {
                "bus": {
                    "max_concurrent_events": 100,
                    "error_isolation": True,
                    "enable_logging": True
                },
                "handlers": {
                    "timeout_seconds": 30,
                    "retry_attempts": 3,
                    "retry_delay": 1.0
                }
            },
            "data_providers": {
                "batch_size": 100,
                "market_data": {
                    "primary": "yahoo_finance",
                    "fallback": [],
                    "cache_ttl": 300,
                    "rate_limits": {}
                },
                "apis": {}
            },
            "portfolio": {
                "simulation": {
                    "initial_cash": 100000.0,
                    "default_currency": "USD",
                    "commission_rate": 0.001,
                    "min_commission": 1.0
                },
                "risk_management": {
                    "max_position_size": 0.1,
                    "max_sector_exposure": 0.25,
                    "stop_loss_threshold": -0.05
                }
            },
            "strategies": {
                "scoring": {
                    "enabled_strategies": ["momentum"],
                    "rebalance_frequency": "weekly",
                    "min_score_threshold": 60
                },
                "backtesting": {
                    "default_period": "1Y",
                    "benchmark": "SPY"
                }
            },
            "analytics": {
                "technical_indicators": {
                    "default_periods": {
                        "sma": [20, 50],
                        "rsi": 14,
                        "macd": [12, 26, 9]
                    }
                },
                "risk_metrics": {
                    "var_confidence": 0.95,
                    "var_period": 252,
                    "correlation_window": 60
                }
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "handlers": {
                    "console": {"enabled": True},
                    "file": {
                        "enabled": False,
                        "path": "app.log",
                        "max_size": "10MB",
                        "backup_count": 5
                    }
                }
            },
            "monitoring": {
                "metrics": {
                    "enabled": False,
                    "port": 8080,
                    "endpoint": "/metrics"
                },
                "health_check": {
                    "enabled": True,
                    "endpoint": "/health",
                    "timeout": 5
                }
            },
            "security": {
                "api": {
                    "enable_auth": False,
                    "jwt_secret": None,
                    "token_expiry": 3600
                },
                "encryption": {
                    "algorithm": "AES256",
                    "key": None
                }
            }
        }
        
        config = validate_config(config_dict)
        
        assert isinstance(config, StockAppConfig)
        assert config.application.name == "TestApp"
        assert config.database.type == "duckdb"
        assert config.event_system.bus.max_concurrent_events == 100
        assert config.data_providers.batch_size == 100
        assert config.portfolio.simulation.initial_cash == 100000.0
        assert config.strategies.scoring.enabled_strategies == ["momentum"]
        assert config.analytics.technical_indicators.default_periods["rsi"] == 14
        assert config.logging.level == "INFO"
        assert config.monitoring.metrics.enabled is False
        assert config.security.api.enable_auth is False
    
    def test_stock_app_config_validation_errors(self):
        """Test StockApp configuration validation errors."""
        # Missing required sections
        incomplete_config = {
            "application": {"name": "TestApp"},
            # Missing other required sections
        }
        
        with pytest.raises(ValidationError):
            validate_config(incomplete_config)
    
    def test_stock_app_config_with_invalid_nested_values(self):
        """Test StockApp configuration with invalid nested values."""
        config_dict = {
            "application": {"name": "TestApp", "version": "1.0.0"},
            "database": {
                "type": "duckdb",
                "connection": {
                    "database_path": "",  # Invalid empty path
                    "memory": True,
                    "read_only": False,
                    "pragmas": {}
                },
                "pool": {"max_connections": 10, "connection_timeout": 30}
            },
            # ... other required sections with valid values
            "event_system": {
                "bus": {"max_concurrent_events": 100, "error_isolation": True, "enable_logging": True},
                "handlers": {"timeout_seconds": 30, "retry_attempts": 3, "retry_delay": 1.0}
            },
            "data_providers": {
                "batch_size": 100,
                "market_data": {"primary": "test", "fallback": [], "cache_ttl": 300, "rate_limits": {}},
                "apis": {}
            },
            "portfolio": {
                "simulation": {"initial_cash": 100000.0, "default_currency": "USD", "commission_rate": 0.001, "min_commission": 1.0},
                "risk_management": {"max_position_size": 0.1, "max_sector_exposure": 0.25, "stop_loss_threshold": -0.05}
            },
            "strategies": {
                "scoring": {"enabled_strategies": ["test"], "rebalance_frequency": "weekly", "min_score_threshold": 60},
                "backtesting": {"default_period": "1Y", "benchmark": "SPY"}
            },
            "analytics": {
                "technical_indicators": {"default_periods": {"sma": [20], "rsi": 14}},
                "risk_metrics": {"var_confidence": 0.95, "var_period": 252, "correlation_window": 60}
            },
            "logging": {
                "level": "INFO",
                "format": "%(message)s",
                "handlers": {"console": {"enabled": True}, "file": {"enabled": False, "path": "app.log", "max_size": "10MB", "backup_count": 5}}
            },
            "monitoring": {
                "metrics": {"enabled": False, "port": 8080, "endpoint": "/metrics"},
                "health_check": {"enabled": True, "endpoint": "/health", "timeout": 5}
            },
            "security": {
                "api": {"enable_auth": False, "jwt_secret": None, "token_expiry": 3600},
                "encryption": {"algorithm": "AES256", "key": None}
            }
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_config(config_dict)
        
        assert "database_path cannot be empty" in str(exc_info.value)