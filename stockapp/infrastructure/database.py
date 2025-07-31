"""Database connection and management interfaces."""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, Dict, List, Optional


class DatabaseConnection(ABC):
    """Abstract interface for database connections."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish database connection."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close database connection."""
        pass

    @abstractmethod
    async def execute(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        """Execute a query without returning results."""
        pass

    @abstractmethod
    async def fetch_one(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Execute a query and return one result."""
        pass

    @abstractmethod
    async def fetch_all(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return all results."""
        pass

    @abstractmethod
    async def execute_many(self, query: str, parameters_list: List[Dict[str, Any]]) -> None:
        """Execute a query multiple times with different parameters."""
        pass

    @abstractmethod
    @asynccontextmanager
    async def transaction(self) -> AsyncContextManager[None]:
        """Start a database transaction."""
        pass

    @abstractmethod
    async def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        pass

    @abstractmethod
    async def drop_tables(self) -> None:
        """Drop all database tables (for testing)."""
        pass


class SchemaManager(ABC):
    """Abstract interface for database schema management."""

    @abstractmethod
    async def create_schema(self) -> None:
        """Create the complete database schema."""
        pass

    @abstractmethod
    async def drop_schema(self) -> None:
        """Drop the complete database schema."""
        pass

    @abstractmethod
    async def get_schema_version(self) -> Optional[str]:
        """Get current schema version."""
        pass

    @abstractmethod
    async def migrate_schema(self, target_version: Optional[str] = None) -> None:
        """Migrate schema to target version."""
        pass

    @abstractmethod
    def get_create_table_sql(self) -> Dict[str, str]:
        """Get SQL statements for creating all tables."""
        pass