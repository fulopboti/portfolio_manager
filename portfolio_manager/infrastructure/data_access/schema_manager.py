"""Database schema management and migration abstractions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class MigrationType(Enum):
    """Types of database migrations."""
    CREATE_TABLE = "create_table"
    ALTER_TABLE = "alter_table"
    DROP_TABLE = "drop_table"
    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"
    DATA_MIGRATION = "data_migration"


@dataclass(frozen=True)
class Migration:
    """Represents a database migration.

    Encapsulates the information needed to apply or rollback
    a database schema change.
    """

    version: str
    name: str
    migration_type: MigrationType
    up_sql: str
    down_sql: str
    description: str
    created_at: datetime
    checksum: str

    def get_migration_id(self) -> str:
        """Get unique identifier for this migration.

        Returns:
            Unique string identifying this migration
        """
        return f"{self.version}_{self.name}"


@dataclass(frozen=True)
class TableDefinition:
    """Represents a database table definition."""

    name: str
    columns: dict[str, str]  # column_name -> column_definition
    primary_key: list[str]
    foreign_keys: dict[str, str]  # column -> referenced_table.column
    indexes: list[str]
    constraints: list[str]


class SchemaManager(ABC):
    """Abstract interface for database schema management.

    Provides methods for creating, modifying, and querying
    database schema structure.
    """

    @abstractmethod
    async def create_schema(self) -> None:
        """Create the complete database schema from scratch.

        Creates all tables, indexes, constraints, and other schema
        objects required by the application.

        Raises:
            SchemaError: If schema creation fails
        """
        pass

    @abstractmethod
    async def drop_schema(self) -> None:
        """Drop the complete database schema.

        Removes all application tables and objects. Should be used
        carefully, typically only in testing scenarios.

        Raises:
            SchemaError: If schema deletion fails
        """
        pass

    @abstractmethod
    async def schema_exists(self) -> bool:
        """Check if the database schema exists.

        Returns:
            bool: True if schema is present, False otherwise
        """
        pass

    @abstractmethod
    async def get_schema_version(self) -> str | None:
        """Get the current schema version.

        Returns:
            Current schema version string, or None if no version info
        """
        pass

    @abstractmethod
    async def set_schema_version(self, version: str) -> None:
        """Set the current schema version.

        Args:
            version: Version string to set

        Raises:
            SchemaError: If version cannot be set
        """
        pass

    @abstractmethod
    async def get_table_names(self) -> set[str]:
        """Get names of all tables in the schema.

        Returns:
            Set of table name strings
        """
        pass

    @abstractmethod
    async def table_exists(self, table_name: str) -> bool:
        """Check if a specific table exists.

        Args:
            table_name: Name of table to check

        Returns:
            bool: True if table exists, False otherwise
        """
        pass

    @abstractmethod
    async def get_table_definition(self, table_name: str) -> TableDefinition | None:
        """Get the definition of a specific table.

        Args:
            table_name: Name of table to describe

        Returns:
            TableDefinition object, or None if table doesn't exist
        """
        pass

    @abstractmethod
    async def create_table(self, definition: TableDefinition) -> None:
        """Create a new table with the given definition.

        Args:
            definition: Complete table definition

        Raises:
            SchemaError: If table creation fails
        """
        pass

    @abstractmethod
    async def drop_table(self, table_name: str) -> None:
        """Drop a table from the database.

        Args:
            table_name: Name of table to drop

        Raises:
            SchemaError: If table cannot be dropped
        """
        pass

    @abstractmethod
    async def get_create_table_sql(self) -> dict[str, str]:
        """Get SQL statements for creating all application tables.

        Returns:
            Dictionary mapping table names to CREATE TABLE statements
        """
        pass


class MigrationManager(ABC):
    """Abstract interface for database migration management.

    Handles versioned schema changes, allowing for controlled
    database evolution and rollback capabilities.
    """

    @abstractmethod
    async def initialize_migration_tracking(self) -> None:
        """Initialize migration tracking in the database.

        Creates the necessary tables/structures to track applied
        migrations and schema versions.

        Raises:
            MigrationError: If initialization fails
        """
        pass

    @abstractmethod
    async def get_applied_migrations(self) -> list[str]:
        """Get list of migration versions that have been applied.

        Returns:
            List of migration version strings in application order
        """
        pass

    @abstractmethod
    async def get_pending_migrations(self) -> list[Migration]:
        """Get list of migrations that need to be applied.

        Returns:
            List of Migration objects in dependency order
        """
        pass

    @abstractmethod
    async def apply_migration(self, migration: Migration) -> None:
        """Apply a single migration to the database.

        Args:
            migration: Migration to apply

        Raises:
            MigrationError: If migration application fails
        """
        pass

    @abstractmethod
    async def rollback_migration(self, migration: Migration) -> None:
        """Rollback a single migration from the database.

        Args:
            migration: Migration to rollback

        Raises:
            MigrationError: If migration rollback fails
        """
        pass

    @abstractmethod
    async def migrate_to_version(self, target_version: str | None = None) -> None:
        """Migrate database to a specific version.

        Args:
            target_version: Version to migrate to, or None for latest

        Raises:
            MigrationError: If migration fails
        """
        pass

    @abstractmethod
    async def validate_migration_integrity(self) -> bool:
        """Validate that applied migrations haven't been tampered with.

        Returns:
            bool: True if all migrations are valid, False otherwise
        """
        pass

    @abstractmethod
    def load_migrations_from_directory(self, directory: str) -> list[Migration]:
        """Load migration definitions from a filesystem directory.

        Args:
            directory: Path to directory containing migration files

        Returns:
            List of Migration objects sorted by version

        Raises:
            MigrationError: If migrations cannot be loaded
        """
        pass
