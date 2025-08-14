"""Configuration validation schemas using Pydantic."""

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class DatabaseConnectionConfig(BaseModel):
    """Database connection configuration."""

    database_path: str = Field(..., description="Path to DuckDB database file")
    memory: bool = Field(False, description="Use in-memory database")
    read_only: bool = Field(False, description="Open database in read-only mode")
    pragmas: dict[str, str | int] = Field(
        default_factory=dict, description="DuckDB pragma settings"
    )

    @field_validator("database_path")
    @classmethod
    def validate_database_path(cls, v):
        if v != ":memory:" and not v:
            raise ValueError("database_path cannot be empty")
        return v


class DatabasePoolConfig(BaseModel):
    """Database connection pool configuration."""

    max_connections: int = Field(
        10, ge=1, le=100, description="Maximum number of connections"
    )
    connection_timeout: int = Field(
        30, ge=1, description="Connection timeout in seconds"
    )


class DatabaseConfig(BaseModel):
    """Complete database configuration."""

    type: str = Field("duckdb", description="Database type")
    connection: DatabaseConnectionConfig
    pool: DatabasePoolConfig


class EventBusConfig(BaseModel):
    """Event bus configuration."""

    max_concurrent_events: int = Field(
        100, ge=1, description="Maximum concurrent events"
    )
    error_isolation: bool = Field(True, description="Isolate handler errors")
    enable_logging: bool = Field(True, description="Enable event logging")


class EventHandlersConfig(BaseModel):
    """Event handlers configuration."""

    timeout_seconds: int = Field(30, ge=1, description="Handler timeout")
    retry_attempts: int = Field(3, ge=0, description="Number of retry attempts")
    retry_delay: float = Field(1.0, ge=0, description="Delay between retries")


class EventSystemConfig(BaseModel):
    """Event system configuration."""

    bus: EventBusConfig
    handlers: EventHandlersConfig


class APIConfig(BaseModel):
    """External API configuration."""

    base_url: str = Field(..., description="Base URL for API")
    api_key: str | None = Field(None, description="API key")
    timeout: int = Field(10, ge=1, description="Request timeout in seconds")

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v


class YFinanceConfig(BaseModel):
    """Yahoo Finance provider configuration."""

    enabled: bool = Field(True, description="Enable Yahoo Finance provider")
    request_delay: float = Field(
        0.1, ge=0, description="Delay between requests in seconds"
    )
    max_retries: int = Field(3, ge=0, description="Maximum retry attempts")
    timeout: int = Field(30, ge=1, description="Request timeout in seconds")


class MarketDataConfig(BaseModel):
    """Market data provider configuration."""

    primary: str = Field(..., description="Primary data provider")
    fallback: list[str] = Field(default_factory=list, description="Fallback providers")
    cache_ttl: int = Field(300, ge=0, description="Cache TTL in seconds")
    rate_limits: dict[str, int] = Field(
        default_factory=dict, description="Rate limits per provider"
    )
    yfinance: YFinanceConfig = Field(default_factory=YFinanceConfig)


class DataProvidersConfig(BaseModel):
    """Data providers configuration."""

    batch_size: int = Field(
        100, ge=1, description="Default batch size for data ingestion"
    )
    market_data: MarketDataConfig
    apis: dict[str, APIConfig]


class PortfolioSimulationConfig(BaseModel):
    """Portfolio simulation configuration."""

    initial_cash: Decimal = Field(
        Decimal("100000.0"), gt=0, description="Initial cash amount"
    )
    default_currency: str = Field("USD", description="Default currency")
    commission_rate: float = Field(0.001, ge=0, le=1, description="Commission rate")
    min_commission: Decimal = Field(
        Decimal("1.0"), ge=0, description="Minimum commission"
    )


class RiskManagementConfig(BaseModel):
    """Risk management configuration."""

    max_position_size: float = Field(
        0.10, gt=0, le=1, description="Maximum position size as fraction"
    )
    max_sector_exposure: float = Field(
        0.25, gt=0, le=1, description="Maximum sector exposure"
    )
    stop_loss_threshold: float = Field(
        -0.05, ge=-1, le=0, description="Stop loss threshold"
    )


class PortfolioConfig(BaseModel):
    """Portfolio configuration."""

    simulation: PortfolioSimulationConfig
    risk_management: RiskManagementConfig


class ScoringConfig(BaseModel):
    """Strategy scoring configuration."""

    enabled_strategies: list[str] = Field(
        default_factory=list, description="Enabled strategies"
    )
    rebalance_frequency: str = Field("weekly", description="Rebalancing frequency")
    min_score_threshold: int = Field(
        60, ge=0, le=100, description="Minimum score threshold"
    )


class BacktestingConfig(BaseModel):
    """Backtesting configuration."""

    default_period: str = Field("1Y", description="Default backtesting period")
    benchmark: str = Field("SPY", description="Benchmark symbol")


class StrategiesConfig(BaseModel):
    """Strategies configuration."""

    scoring: ScoringConfig
    backtesting: BacktestingConfig


class TechnicalIndicatorsConfig(BaseModel):
    """Technical indicators configuration."""

    default_periods: dict[str, int | list[int]] = Field(default_factory=dict)


class RiskMetricsConfig(BaseModel):
    """Risk metrics configuration."""

    var_confidence: float = Field(0.95, gt=0, lt=1, description="VaR confidence level")
    var_period: int = Field(252, gt=0, description="VaR period in trading days")
    correlation_window: int = Field(60, gt=0, description="Correlation window")


class AnalyticsConfig(BaseModel):
    """Analytics configuration."""

    technical_indicators: TechnicalIndicatorsConfig
    risk_metrics: RiskMetricsConfig


class LogHandlerConfig(BaseModel):
    """Log handler configuration."""

    enabled: bool = Field(True, description="Enable handler")
    path: str | None = Field(None, description="Log file path")
    max_size: str | None = Field(None, description="Maximum log file size")
    backup_count: int | None = Field(None, description="Number of backup files")


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field("INFO", description="Log level")
    format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", description="Log format"
    )
    handlers: dict[str, LogHandlerConfig]

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log level must be one of {valid_levels}")
        return v.upper()


class MetricsConfig(BaseModel):
    """Metrics configuration."""

    enabled: bool = Field(False, description="Enable metrics")
    port: int = Field(8080, ge=1, le=65535, description="Metrics port")
    endpoint: str = Field("/metrics", description="Metrics endpoint")


class HealthCheckConfig(BaseModel):
    """Health check configuration."""

    enabled: bool = Field(True, description="Enable health checks")
    endpoint: str = Field("/health", description="Health check endpoint")
    timeout: int = Field(5, ge=1, description="Health check timeout")


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""

    metrics: MetricsConfig
    health_check: HealthCheckConfig


class APISecurityConfig(BaseModel):
    """API security configuration."""

    enable_auth: bool = Field(False, description="Enable authentication")
    jwt_secret: str | None = Field(None, description="JWT secret key")
    token_expiry: int = Field(3600, gt=0, description="Token expiry in seconds")


class EncryptionConfig(BaseModel):
    """Encryption configuration."""

    algorithm: str = Field("AES256", description="Encryption algorithm")
    key: str | None = Field(None, description="Encryption key")


class SecurityConfig(BaseModel):
    """Security configuration."""

    api: APISecurityConfig
    encryption: EncryptionConfig


class ApplicationConfig(BaseModel):
    """Application-level configuration."""

    name: str = Field("Portfolio Manager", description="Application name")
    version: str = Field("0.1.0", description="Application version")
    environment: str = Field("development", description="Environment name")
    debug: bool = Field(False, description="Debug mode")
    log_level: str = Field("INFO", description="Application log level")


class PortfolioManagerConfig(BaseModel):
    """Complete Portfolio Manager configuration schema."""

    application: ApplicationConfig
    database: DatabaseConfig
    event_system: EventSystemConfig
    data_providers: DataProvidersConfig
    portfolio: PortfolioConfig
    strategies: StrategiesConfig
    analytics: AnalyticsConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig
    security: SecurityConfig

    class Config:
        extra = "allow"  # Allow extra fields for environment variable overrides


def validate_config(config_dict: dict) -> PortfolioManagerConfig:
    """
    Validate configuration dictionary against schema.

    Args:
        config_dict: Configuration dictionary to validate

    Returns:
        Validated configuration object

    Raises:
        ValidationError: If configuration is invalid
    """
    return PortfolioManagerConfig(**config_dict)
