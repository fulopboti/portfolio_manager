"""Unit tests for configuration management."""

import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch

from stockapp.config.settings import ConfigManager, ConfigurationError


class TestConfigManager:
    """Test configuration manager functionality."""
    
    def test_basic_configuration_loading(self):
        """Test basic configuration loading from default files."""
        config = ConfigManager()
        
        # Test basic values from base.yaml (with development overrides)
        assert config.get("application.name") == "StockApp"
        assert config.get("application.version") == "1.0.0"
        assert config.get("database.type") == "duckdb"
        assert config.get("event_system.bus.max_concurrent_events") == 50  # Overridden in development.yaml
    
    def test_get_with_default_values(self):
        """Test getting values with defaults."""
        config = ConfigManager()
        
        # Existing value
        assert config.get("application.name", "DefaultApp") == "StockApp"
        
        # Non-existing value with default
        assert config.get("nonexistent.key", "default_value") == "default_value"
        
        # Non-existing value without default
        assert config.get("nonexistent.key") is None
    
    def test_get_section(self):
        """Test getting configuration sections."""
        config = ConfigManager()
        
        # Get database section
        db_config = config.get_section("database")
        assert isinstance(db_config, dict)
        assert db_config["type"] == "duckdb"
        assert "connection" in db_config
        assert "pool" in db_config
        
        # Get non-existing section
        empty_section = config.get_section("nonexistent")
        assert empty_section == {}
    
    def test_has_method(self):
        """Test checking if configuration paths exist."""
        config = ConfigManager()
        
        # Existing paths
        assert config.has("application.name") is True
        assert config.has("database.connection.database_path") is True
        
        # Non-existing paths
        assert config.has("nonexistent.key") is False
        assert config.has("application.nonexistent") is False
    
    def test_environment_helpers(self):
        """Test environment helper methods."""
        config = ConfigManager()
        
        # Default environment from base.yaml
        assert config.get_environment() == "development"
        assert config.is_debug() is True
        assert config.is_production() is False
        assert config.is_testing() is False
    
    @patch.dict(os.environ, {"STOCKAPP_APPLICATION_DEBUG": "false"})
    def test_environment_variable_override_boolean(self):
        """Test environment variable override for boolean values."""
        config = ConfigManager()
        
        # Should be overridden by environment variable
        assert config.get("application.debug") is False
    
    def test_environment_variable_override_integer(self):
        """Test environment variable override for integer values.""" 
        import tempfile
        from pathlib import Path
        
        # Set environment variable directly (note: using different key structure)
        os.environ["STOCKAPP_DATABASE_POOL_SIZE"] = "25"
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                config_dir = Path(temp_dir)
                base_yaml = config_dir / "base.yaml"
                base_yaml.write_text("""
database:
  pool:
    size: 10
    connection_timeout: 30
                """)
                
                config = ConfigManager(config_dir=config_dir)
                
                # Should be overridden by environment variable
                assert config.get("database.pool.size") == 25
        finally:
            # Clean up
            if "STOCKAPP_DATABASE_POOL_SIZE" in os.environ:
                del os.environ["STOCKAPP_DATABASE_POOL_SIZE"]
    
    def test_environment_variable_override_float(self):
        """Test environment variable override for float values."""
        import tempfile
        from pathlib import Path
        
        # Set environment variable directly
        os.environ["STOCKAPP_PORTFOLIO_SIMULATION_RATE"] = "0.005"
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                config_dir = Path(temp_dir)
                base_yaml = config_dir / "base.yaml"
                base_yaml.write_text("""
portfolio:
  simulation:
    rate: 0.001
                """)
                
                config = ConfigManager(config_dir=config_dir)
                
                # Should be overridden by environment variable
                assert config.get("portfolio.simulation.rate") == 0.005
        finally:
            # Clean up
            if "STOCKAPP_PORTFOLIO_SIMULATION_RATE" in os.environ:
                del os.environ["STOCKAPP_PORTFOLIO_SIMULATION_RATE"]
    
    def test_environment_variable_override_list(self):
        """Test environment variable override for list values."""
        import tempfile
        from pathlib import Path
        
        # Set environment variable directly
        os.environ["STOCKAPP_STRATEGIES_ENABLED"] = "momentum,value"
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                config_dir = Path(temp_dir)
                base_yaml = config_dir / "base.yaml"
                base_yaml.write_text("""
strategies:
  enabled: ["momentum", "value", "quality"]
                """)
                
                config = ConfigManager(config_dir=config_dir)
                
                # Should be overridden by environment variable
                strategies = config.get("strategies.enabled")
                assert strategies == ["momentum", "value"]
        finally:
            # Clean up
            if "STOCKAPP_STRATEGIES_ENABLED" in os.environ:
                del os.environ["STOCKAPP_STRATEGIES_ENABLED"]
    
    @patch.dict(os.environ, {"STOCKAPP_NEW_CONFIG_KEY": "new_value"})
    def test_environment_variable_new_key(self):
        """Test environment variable creating new configuration key."""
        config = ConfigManager()
        
        # Should create new configuration key
        assert config.get("new.config.key") == "new_value"
    
    @patch.dict(os.environ, {"STOCKAPP_ENVIRONMENT": "production"})
    def test_environment_specific_config_loading(self):
        """Test loading environment-specific configuration."""
        import tempfile
        from pathlib import Path
        
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Base config
            base_yaml = config_dir / "base.yaml"
            base_yaml.write_text("""
application:
  environment: development
  debug: true
  log_level: INFO
            """)
            
            # Production config
            prod_yaml = config_dir / "production.yaml"
            prod_yaml.write_text("""
application:
  debug: false
  log_level: WARNING
            """)
            
            config = ConfigManager(config_dir=config_dir)
            
            # Should load production.yaml overrides but application.environment remains from base
            assert config.get("application.environment") == "development"  # From base config
            assert config.get("application.debug") is False  # From production.yaml
            assert config.get("application.log_level") == "WARNING"  # From production.yaml
    
    def test_set_method(self):
        """Test setting configuration values."""
        config = ConfigManager()
        
        # Set new value
        config.set("test.new.value", "test_data")
        assert config.get("test.new.value") == "test_data"
        
        # Override existing value
        config.set("application.name", "TestApp")
        assert config.get("application.name") == "TestApp"
    
    def test_get_all_method(self):
        """Test getting complete configuration."""
        config = ConfigManager()
        
        all_config = config.get_all()
        assert isinstance(all_config, dict)
        assert "application" in all_config
        assert "database" in all_config
        assert "event_system" in all_config
    
    def test_parse_env_value_types(self):
        """Test parsing different environment variable types."""
        config = ConfigManager()
        
        # Test boolean parsing
        assert config._parse_env_value("true") is True
        assert config._parse_env_value("false") is False
        assert config._parse_env_value("True") is True
        assert config._parse_env_value("FALSE") is False
        
        # Test numeric parsing
        assert config._parse_env_value("42") == 42
        assert config._parse_env_value("3.14") == 3.14
        
        # Test list parsing
        assert config._parse_env_value("a,b,c") == ["a", "b", "c"]
        assert config._parse_env_value("1,2,3") == ["1", "2", "3"]
        
        # Test null values
        assert config._parse_env_value("null") is None
        assert config._parse_env_value("") is None
        
        # Test string values
        assert config._parse_env_value("hello") == "hello"
    
    def test_custom_config_directory(self):
        """Test using custom configuration directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create minimal config file
            base_config = config_dir / "base.yaml"
            base_config.write_text("""
application:
  name: "CustomApp"
  version: "2.0.0"
            """)
            
            config = ConfigManager(config_dir=config_dir)
            assert config.get("application.name") == "CustomApp"
            assert config.get("application.version") == "2.0.0"
    
    def test_configuration_error_handling(self):
        """Test error handling for configuration issues."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Test missing base.yaml
            with pytest.raises(ConfigurationError):
                ConfigManager(config_dir=config_dir)
            
            # Test invalid YAML
            base_config = config_dir / "base.yaml"
            base_config.write_text("invalid: yaml: content: [")
            
            with pytest.raises(ConfigurationError):
                ConfigManager(config_dir=config_dir)
    
    def test_deep_merge_functionality(self):
        """Test deep merging of configuration dictionaries."""
        config = ConfigManager()
        
        base = {
            "level1": {
                "level2": {
                    "existing_key": "original_value",
                    "keep_key": "keep_value"
                }
            }
        }
        
        override = {
            "level1": {
                "level2": {
                    "existing_key": "new_value",
                    "new_key": "new_value"
                }
            }
        }
        
        config._deep_merge(base, override)
        
        # Check merged result
        assert base["level1"]["level2"]["existing_key"] == "new_value"
        assert base["level1"]["level2"]["keep_key"] == "keep_value"  
        assert base["level1"]["level2"]["new_key"] == "new_value"


class TestConfigManagerIntegration:
    """Integration tests for configuration manager."""
    
    @patch.dict(os.environ, {
        "STOCKAPP_ENVIRONMENT": "testing",
        "STOCKAPP_APPLICATION_DEBUG": "false",
        "STOCKAPP_DATABASE_CONNECTION_DATABASE_PATH": ":memory:",
        "STOCKAPP_EVENT_SYSTEM_BUS_MAX_CONCURRENT_EVENTS": "10"
    })
    def test_complete_configuration_flow(self):
        """Test complete configuration loading with all features."""
        config = ConfigManager()
        
        # Test environment-specific loading
        assert config.get_environment() == "testing"
        assert config.is_testing() is True
        
        # Test environment variable overrides
        assert config.get("application.debug") is False
        assert config.get("database.connection.database_path") == ":memory:"
        assert config.get("event_system.bus.max_concurrent_events") == 10
        
        # Test values from testing.yaml
        assert config.get("database.connection.memory") is True
        assert config.get("logging.handlers.console.enabled") is False