"""
Configuration management for StockApp.

Provides YAML-based configuration with environment variable overrides,
validation schemas, and easy access patterns.

Example usage:
    from stockapp.config import config
    
    # Get configuration values
    debug_mode = config.get("application.debug", False)
    db_path = config.get("database.connection.database_path")
    
    # Get configuration sections
    db_config = config.get_section("database")
    event_config = config.get_section("event_system")
    
    # Check environment
    if config.is_production():
        # Production-specific logic
        pass
"""

from .settings import config, ConfigManager, ConfigurationError
from .schema import (
    StockAppConfig, 
    ApplicationConfig,
    DatabaseConfig, 
    EventSystemConfig,
    DataProvidersConfig,
    PortfolioConfig,
    StrategiesConfig,
    AnalyticsConfig,
    LoggingConfig,
    MonitoringConfig,
    SecurityConfig,
    validate_config
)

__all__ = [
    # Main configuration instance and manager
    "config",
    "ConfigManager", 
    "ConfigurationError",
    
    # Configuration schemas
    "StockAppConfig",
    "ApplicationConfig", 
    "DatabaseConfig",
    "EventSystemConfig",
    "DataProvidersConfig", 
    "PortfolioConfig",
    "StrategiesConfig",
    "AnalyticsConfig",
    "LoggingConfig",
    "MonitoringConfig",
    "SecurityConfig",
    
    # Utilities
    "validate_config"
]


def get_config() -> ConfigManager:
    """
    Get the global configuration manager instance.
    
    Returns:
        ConfigManager: Global configuration instance
    """
    return config


def reload_config() -> None:
    """
    Reload configuration from files and environment variables.
    
    Useful for development or when configuration files change.
    """
    global config
    config = ConfigManager()


def get_database_url() -> str:
    """
    Get database connection URL for the current environment.
    
    Returns:
        Database connection URL
    """
    db_path = config.get("database.connection.database_path", "./data/stockapp.db")
    if db_path == ":memory:":
        return "duckdb:///:memory:"
    return f"duckdb:///{db_path}"


def get_log_config() -> dict:
    """
    Get logging configuration suitable for Python's logging.dictConfig().
    
    Returns:
        Logging configuration dictionary
    """
    log_config = config.get_section("logging")
    
    # Convert our config format to Python logging dictConfig format
    handlers = {}
    loggers = {
        "root": {
            "level": log_config.get("level", "INFO"),
            "handlers": []
        }
    }
    
    # Console handler
    if log_config.get("handlers", {}).get("console", {}).get("enabled", True):
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "level": log_config.get("level", "INFO"),
            "formatter": "default"
        }
        loggers["root"]["handlers"].append("console")
    
    # File handler
    file_config = log_config.get("handlers", {}).get("file", {})
    if file_config.get("enabled", False):
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": log_config.get("level", "INFO"),
            "formatter": "default",
            "filename": file_config.get("path", "./logs/stockapp.log"),
            "maxBytes": _parse_size(file_config.get("max_size", "10MB")),
            "backupCount": file_config.get("backup_count", 5)
        }
        loggers["root"]["handlers"].append("file")
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            }
        },
        "handlers": handlers,
        "loggers": loggers
    }


def _parse_size(size_str: str) -> int:
    """
    Parse size string to bytes.
    
    Args:
        size_str: Size string like "10MB", "1GB"
        
    Returns:
        Size in bytes
    """
    size_str = size_str.upper()
    multipliers = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024
    }
    
    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            return int(float(size_str[:-len(suffix)]) * multiplier)
    
    # Default to bytes if no suffix
    return int(size_str)