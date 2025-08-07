"""Configuration management with YAML files and environment variable overrides."""

import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML is required for configuration management. Install with: pip install PyYAML")

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when configuration loading or parsing fails."""
    pass


class ConfigManager:
    """
    Manages YAML configuration with environment variable overrides.
    
    Supports:
    - Loading base configuration from YAML files
    - Environment-specific overrides (dev, prod, test)
    - Environment variable overrides using dot notation
    - Type parsing from environment variables
    - Nested configuration access
    
    Example:
        config = ConfigManager()
        db_path = config.get("database.connection.database_path")
        debug_mode = config.get("application.debug", False)
    """
    
    def __init__(self, config_dir: Optional[Path] = None, env_prefix: str = "STOCKAPP"):
        """
        Initialize configuration manager.
        
        Args:
            config_dir: Directory containing YAML config files
            env_prefix: Prefix for environment variables (e.g., STOCKAPP_DATABASE_HOST)
        """
        self.config_dir = config_dir or Path(__file__).parent / "defaults"
        self.env_prefix = env_prefix
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML files and apply environment overrides."""
        try:
            # Load base configuration
            self._load_base_config()
            
            # Load environment-specific overrides
            self._load_environment_config()
            
            # Apply environment variable overrides
            self._apply_env_overrides()
            
            logger.info(f"Configuration loaded successfully for environment: {self.get('application.environment', 'unknown')}")
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}") from e
    
    def _load_base_config(self) -> None:
        """Load base configuration from base.yaml."""
        base_path = self.config_dir / "base.yaml"
        if not base_path.exists():
            raise ConfigurationError(f"Base configuration file not found: {base_path}")
        
        try:
            with open(base_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}
            logger.debug(f"Loaded base configuration from {base_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in base configuration: {e}")
    
    def _load_environment_config(self) -> None:
        """Load environment-specific configuration overrides."""
        env = os.getenv(f"{self.env_prefix}_ENVIRONMENT", 
                       self._config.get("application", {}).get("environment", "development"))
        
        env_path = self.config_dir / f"{env}.yaml"
        if env_path.exists():
            try:
                with open(env_path, 'r', encoding='utf-8') as f:
                    env_config = yaml.safe_load(f) or {}
                self._deep_merge(self._config, env_config)
                logger.debug(f"Loaded environment configuration from {env_path}")
            except yaml.YAMLError as e:
                logger.warning(f"Invalid YAML in environment configuration {env_path}: {e}")
        else:
            logger.debug(f"No environment-specific configuration found for: {env}")
    
    def _deep_merge(self, base: dict, override: dict) -> None:
        """
        Deep merge override dict into base dict.
        
        Args:
            base: Base dictionary to merge into
            override: Override dictionary to merge from
        """
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides using dot notation."""
        env_overrides_applied = 0
        
        # Define mapping for compound keys to handle schema structure
        key_mappings = {
            'data_providers': 'data_providers',
            'event_system': 'event_system',
            'technical_indicators': 'technical_indicators', 
            'risk_management': 'risk_management',
            'health_check': 'health_check',
            'risk_metrics': 'risk_metrics',
            'api_security': 'api_security'
        }
        
        # Define fields that should NOT be split on underscores (field names in config)
        no_split_fields = {
            'batch_size', 'retry_attempts', 'timeout_seconds', 'retry_delay',
            'max_connections', 'connection_timeout', 'database_path', 'read_only',
            'max_concurrent_events', 'error_isolation', 'enable_logging',
            'cache_ttl', 'rate_limits', 'initial_cash', 'default_currency',
            'commission_rate', 'min_commission', 'max_position_size', 
            'max_sector_exposure', 'stop_loss_threshold', 'enabled_strategies',
            'rebalance_frequency', 'min_score_threshold', 'default_period',
            'default_periods', 'var_confidence', 'var_period', 'correlation_window',
            'max_size', 'backup_count', 'enable_auth', 'jwt_secret', 'token_expiry'
        }
        
        for key, value in os.environ.items():
            if key.startswith(f"{self.env_prefix}_"):
                # Convert STOCKAPP_DATABASE_CONNECTION_DATABASE_PATH to database.connection.database_path
                remaining_key = key[len(f"{self.env_prefix}_"):].lower()
                
                # Handle compound keys first
                config_path = []
                found_compound = False
                
                for compound_key, schema_key in key_mappings.items():
                    compound_env_key = compound_key.upper()  # e.g., 'DATA_PROVIDERS'
                    if remaining_key.startswith(compound_env_key.lower()):
                        # Found compound key, use schema mapping
                        config_path = [schema_key]
                        remaining_parts = remaining_key[len(compound_env_key):].lstrip('_')
                        if remaining_parts:
                            # Check if remaining parts contain a no-split field
                            if remaining_parts in no_split_fields:
                                # Don't split if it's a known field name
                                config_path.append(remaining_parts)
                            else:
                                # Split normally but preserve no-split fields at the end
                                parts = remaining_parts.split('_')
                                if len(parts) > 1:
                                    # Check if the last part(s) form a no-split field
                                    for i in range(1, len(parts) + 1):
                                        potential_field = '_'.join(parts[-i:])
                                        if potential_field in no_split_fields:
                                            # Found a no-split field at the end
                                            config_path.extend(parts[:-i])
                                            config_path.append(potential_field)
                                            break
                                    else:
                                        # No no-split field found, split normally
                                        config_path.extend(parts)
                                else:
                                    config_path.append(remaining_parts)
                        found_compound = True
                        break
                
                if not found_compound:
                    # No compound key found, split normally
                    config_path = remaining_key.split("_")
                
                parsed_value = self._parse_env_value(value)
                
                try:
                    self._set_nested_value(self._config, config_path, parsed_value)
                    env_overrides_applied += 1
                    logger.debug(f"Applied environment override: {'.'.join(config_path)} = {parsed_value}")
                except Exception as e:
                    logger.warning(f"Failed to apply environment override {key}: {e}")
        
        if env_overrides_applied > 0:
            logger.info(f"Applied {env_overrides_applied} environment variable overrides")
    
    def _set_nested_value(self, config: dict, path: List[str], value: Any) -> None:
        """
        Set nested dictionary value using path list.
        
        Args:
            config: Configuration dictionary
            path: List of keys representing nested path
            value: Value to set
        """
        for key in path[:-1]:
            config = config.setdefault(key, {})
            if not isinstance(config, dict):
                raise ConfigurationError(f"Cannot set nested value: path {'.'.join(path)} conflicts with existing non-dict value")
        
        config[path[-1]] = value
    
    def _parse_env_value(self, value: str) -> Any:
        """
        Parse environment variable value to appropriate Python type.
        
        Args:
            value: Raw environment variable value
            
        Returns:
            Parsed value with appropriate type
        """
        # Handle empty/null values
        if not value or value.lower() in ('null', 'none', ''):
            return None
        
        # Boolean parsing
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        
        # Numeric parsing
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        # List parsing (comma-separated)
        if "," in value:
            return [item.strip() for item in value.split(",") if item.strip()]
        
        return value
    
    def get(self, path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            path: Dot-separated path to configuration value (e.g., "database.connection.host")
            default: Default value if path not found
            
        Returns:
            Configuration value or default
        """
        keys = path.split(".")
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get entire configuration section.
        
        Args:
            section: Section name (e.g., "database")
            
        Returns:
            Dictionary containing section configuration
        """
        return self.get(section, {})
    
    def has(self, path: str) -> bool:
        """
        Check if configuration path exists.
        
        Args:
            path: Dot-separated path to check
            
        Returns:
            True if path exists, False otherwise
        """
        return self.get(path, None) is not None
    
    def set(self, path: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            path: Dot-separated path to set
            value: Value to set
        """
        keys = path.split(".")
        self._set_nested_value(self._config, keys, value)
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get complete configuration dictionary.
        
        Returns:
            Full configuration dictionary
        """
        return self._config.copy()
    
    def get_environment(self) -> str:
        """
        Get current environment name.
        
        Returns:
            Environment name (development, production, testing, etc.)
        """
        return self.get("application.environment", "development")
    
    def is_debug(self) -> bool:
        """
        Check if application is in debug mode.
        
        Returns:
            True if debug mode is enabled
        """
        return self.get("application.debug", False)
    
    def is_production(self) -> bool:
        """
        Check if application is running in production.
        
        Returns:
            True if environment is production
        """
        return self.get_environment().lower() == "production"
    
    def is_testing(self) -> bool:
        """
        Check if application is running in test mode.
        
        Returns:
            True if environment is testing
        """
        return self.get_environment().lower() == "testing"


# Global configuration instance
# This will be initialized when the module is imported
try:
    config = ConfigManager()
except Exception as e:
    # Fallback configuration for cases where YAML files aren't available
    logger.error(f"Failed to initialize configuration manager: {e}")
    
    class FallbackConfig:
        """Minimal fallback configuration when main config fails to load."""
        
        def get(self, path: str, default: Any = None) -> Any:
            return default
        
        def get_section(self, section: str) -> Dict[str, Any]:
            return {}
        
        def has(self, path: str) -> bool:
            return False
        
        def get_environment(self) -> str:
            return "development"
        
        def is_debug(self) -> bool:
            return True
        
        def is_production(self) -> bool:
            return False
        
        def is_testing(self) -> bool:
            return False
    
    config = FallbackConfig()