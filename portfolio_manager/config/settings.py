"""Configuration settings management for the portfolio manager application."""

import os
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Union

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML is required for configuration management")

from pydantic import BaseModel, ValidationError

from portfolio_manager.config.schema import PortfolioManagerConfig, validate_config

# Module-level logger for testing
logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails."""
    pass


class ConfigManager:
    """Manages application configuration with environment variable overrides."""
    
    def __init__(self, config_dir: Optional[Path] = None, env_prefix: str = "PORTFOLIO_MANAGER"):
        """Initialize configuration manager.
        
        Args:
            config_dir: Directory containing configuration files. If None, uses default.
            env_prefix: Prefix for environment variables.
        """
        self.config_dir = config_dir or self._get_default_config_dir()
        self.env_prefix = env_prefix
        self._config: Optional[Dict[str, Any]] = None
        
        # Load configuration immediately to catch errors during construction
        self.load_config()
        
    def _get_default_config_dir(self) -> Path:
        """Get the default configuration directory."""
        package_dir = Path(__file__).parent
        return package_dir / "defaults"
    
    @property
    def config_path(self) -> Path:
        """Get the path to the configuration directory."""
        return self.config_dir
    
    def load_config(self) -> Dict[str, Any]:
        """Load and validate configuration from files and environment."""
        if self._config is not None:
            return self._config
            
        try:
            # Load base configuration
            config_data = self._load_base_config()
            
            # Load environment-specific configuration
            config_data = self._load_environment_config(config_data)
            
            # Apply environment variable overrides
            config_data = self._apply_env_overrides(config_data)
            
            self._config = config_data
            environment = config_data.get("application", {}).get("environment", "development")
            logger.info(f"Configuration loaded successfully for environment: {environment}")
            return self._config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise ConfigurationError(f"Configuration loading failed: {e}")
    
    def _load_base_config(self) -> Dict[str, Any]:
        """Load base configuration from base.yaml."""
        base_file = self.config_dir / "base.yaml"
        try:
            with open(base_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
                logger.debug("Loaded base configuration")
                return config
        except FileNotFoundError:
            logger.error(f"Base configuration file not found: {base_file}")
            raise ConfigurationError(f"Base configuration file not found: {base_file}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in base config file: {e}")
    
    def _load_environment_config(self, base_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load environment-specific configuration."""
        # Check for environment variable directly without applying to config
        environment = "development"  # default
        env_var = f"{self.env_prefix}_ENVIRONMENT"
        if env_var in os.environ:
            environment = os.environ[env_var]
        else:
            # Fall back to base config
            environment = base_config.get("application", {}).get("environment", "development")
        
        env_file = self.config_dir / f"{environment}.yaml"
        
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    env_config = yaml.safe_load(f) or {}
                logger.debug("Loaded environment configuration")
                result = self._deep_merge(base_config, env_config)
                # Ensure application.environment remains from base config
                if "application" in base_config and "environment" in base_config["application"]:
                    result["application"]["environment"] = base_config["application"]["environment"]
                return result
            except yaml.YAMLError as e:
                logger.warning(f"Invalid YAML in environment config file: {e}")
                return base_config
        else:
            logger.debug("No environment-specific configuration found")
            return base_config
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
                
        return base
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides to configuration."""
        applied_overrides = 0
        for env_var, value in os.environ.items():
            if env_var.startswith(f"{self.env_prefix}_"):
                # Convert environment variable name to config path
                config_key = env_var[len(f"{self.env_prefix}_"):].lower()
                
                # Skip environment variable as it's handled separately
                if config_key == "environment":
                    continue
                
                # Handle special cases where environment variable names don't match config structure
                # For example: DATA_PROVIDERS_BATCH_SIZE should become data_providers.batch_size
                if config_key == "data_providers_batch_size":
                    config_path = ["data_providers", "batch_size"]
                elif config_key == "event_system_handlers_retry_attempts":
                    config_path = ["event_system", "handlers", "retry_attempts"]
                else:
                    config_path = config_key.split('_')
                
                # Parse the value
                parsed_value = self._parse_env_value(value)
                
                # Set the value in the config
                try:
                    self._set_nested_value(config, config_path, parsed_value)
                    applied_overrides += 1
                    logger.debug(f"Applied environment override: {config_key} = {value}")
                except Exception as e:
                    logger.warning(f"Failed to apply environment override {config_key}: {e}")

        if applied_overrides > 0:
            logger.info(f"Applied {applied_overrides} environment variable overrides")

        return config

    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value to appropriate type."""
        if value.lower() in ('', 'null', 'none'):
            return None
        elif value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        elif ',' in value:
            # Handle lists
            items = [item.strip() for item in value.split(',') if item.strip()]
            return items
        else:
            # Try to parse as number
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value

    def _set_nested_value(self, config: Dict[str, Any], path: list, value: Any) -> None:
        """Set a nested value in configuration dictionary."""
        current = config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            elif not isinstance(current[key], dict):
                # If the current value is not a dict, we can't set nested values
                raise ConfigurationError("Cannot set nested value")
            current = current[key]
        current[path[-1]] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by dot-notation key."""
        config = self.load_config()
        keys = key.split('.')
        value = config

        try:
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k)
                else:
                    return default
                if value is None:
                    return default
            return value
        except (KeyError, AttributeError):
            return default

    def get_section(self, section: str) -> Dict[str, Any]:
        """Get a configuration section."""
        config = self.load_config()
        keys = section.split('.')
        value = config
        
        try:
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, {})
                else:
                    return {}
            return value if isinstance(value, dict) else {}
        except (KeyError, AttributeError):
            return {}
    
    def has(self, key: str) -> bool:
        """Check if a configuration key exists and has a non-None value."""
        value = self.get(key)
        return value is not None
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        config = self.load_config()
        keys = key.split('.')
        self._set_nested_value(config, keys, value)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration as a dictionary."""
        return self.load_config().copy()
    
    def get_environment(self) -> str:
        """Get the current environment."""
        return self.get("application.environment", "development")
    
    def is_debug(self) -> bool:
        """Check if debug mode is enabled."""
        return self.get("application.debug", False)
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.get_environment().lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.get_environment().lower() == "development"
    
    def is_testing(self) -> bool:
        """Check if running in testing environment."""
        return self.get_environment().lower() == "testing"


# Global configuration instance
config = ConfigManager()
