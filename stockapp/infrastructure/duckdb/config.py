"""DuckDB configuration management."""

import json
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DuckDBConfig:
    """Configuration settings for DuckDB connections.
    
    This class centralizes all DuckDB configuration options and supports
    environment-based configuration overrides for different deployment environments.
    """
    
    # Memory settings
    memory_limit: str = "4GB"
    
    # Threading settings
    threads: int = 4
    
    # Timezone settings
    timezone: str = "UTC"
    
    # Performance settings
    enable_optimizer: bool = True
    enable_profiling: bool = False
    
    # Connection settings
    read_only: bool = False

    # Pragmas
    pragmas: dict[str, str | int] = None
    
    @classmethod
    def from_environment(cls, **overrides) -> "DuckDBConfig":
        """Create configuration from environment variables with optional overrides.
        
        Environment variables:
        - DUCKDB_MEMORY_LIMIT: Memory limit (default: 4GB)
        - DUCKDB_THREADS: Number of threads (default: 4)
        - DUCKDB_TIMEZONE: Timezone (default: UTC)
        - DUCKDB_ENABLE_OPTIMIZER: Enable optimizer (default: true)
        - DUCKDB_ENABLE_PROFILING: Enable profiling (default: false)
        - DUCKDB_READ_ONLY: Read-only mode (default: false)
        
        Args:
            **overrides: Configuration overrides
            
        Returns:
            DuckDBConfig instance with environment-based settings
        """
        config = cls(
            memory_limit=os.getenv("DUCKDB_MEMORY_LIMIT", cls.memory_limit),
            threads=int(os.getenv("DUCKDB_THREADS", str(cls.threads))),
            timezone=os.getenv("DUCKDB_TIMEZONE", cls.timezone),
            enable_optimizer=os.getenv("DUCKDB_ENABLE_OPTIMIZER", "true").lower() == "true",
            enable_profiling=os.getenv("DUCKDB_ENABLE_PROFILING", "false").lower() == "true",
            read_only=os.getenv("DUCKDB_READ_ONLY", "false").lower() == "true",
            pragmas=json.loads(os.getenv("DUCKDB_PRAGMAS", "{}")) or None,
        )
        
        # Apply any overrides
        for key, value in overrides.items():
            if hasattr(config, key):
                setattr(config, key, value)
        
        return config
    
    def get_connection_settings(self) -> list[str]:
        """Get list of SQL commands to configure a DuckDB connection.
        
        Returns:
            List of SQL SET commands for DuckDB configuration
        """
        settings = [
            f"SET memory_limit='{self.memory_limit}'",
            f"SET threads TO {self.threads}",
            f"SET TimeZone='{self.timezone}'",
        ]
        
        # Only add optimizer/profiling settings if they're supported
        # These may fail on some DuckDB versions, so they're optional
        if self.enable_optimizer:
            settings.append("SET enable_optimizer = true")
        
        if self.enable_profiling:
            settings.append("SET enable_profiling = true")
        
        return settings
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return (
            f"DuckDBConfig(memory_limit={self.memory_limit}, "
            f"threads={self.threads}, timezone={self.timezone}, "
            f"optimizer={self.enable_optimizer}, profiling={self.enable_profiling}, "
            f"pragmas={self.pragmas or {}})"
        )