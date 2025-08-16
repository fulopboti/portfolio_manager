"""DuckDB schema manager implementation."""

import logging
from typing import Any

from portfolio_manager.infrastructure.data_access.exceptions import SchemaError
from portfolio_manager.infrastructure.data_access.schema_manager import (
    SchemaManager,
    TableDefinition,
)

from ..query_executor import DuckDBQueryExecutor
from .schema_definitions import PortfolioManagerSchema
from .schema_inspector import DuckDBSchemaInspector
from .table_builder import DuckDBTableBuilder

logger = logging.getLogger(__name__)


class DuckDBSchemaManager(SchemaManager):
    """DuckDB implementation of schema management.

    Orchestrates schema creation, deletion, and validation using
    specialized components for DuckDB-specific operations.
    """

    def __init__(self, query_executor: DuckDBQueryExecutor):
        """Initialize schema manager.

        Args:
            query_executor: DuckDB query executor for database operations
        """
        self.query_executor = query_executor
        self.table_builder = DuckDBTableBuilder()
        self.inspector = DuckDBSchemaInspector(query_executor)
        self._current_version: str | None = None

    async def create_schema(self) -> None:
        """Create the complete database schema from scratch."""
        try:
            logger.info("Creating Portfolio Manager database schema...")

            # Get all schema definitions
            tables = PortfolioManagerSchema.get_all_tables()
            indexes = PortfolioManagerSchema.get_all_indexes()
            views = PortfolioManagerSchema.get_all_views()

            # Apply DuckDB optimizations
            optimization_sql = self.table_builder.optimize_for_analytics()
            for line in optimization_sql.split("\n"):
                if line.strip() and not line.strip().startswith("--"):
                    try:
                        await self.query_executor.execute_command(line.strip())
                    except Exception as e:
                        logger.warning(
                            f"Optimization setting failed: {line.strip()} - {str(e)}"
                        )

            # Create tables in dependency order
            table_creation_order = self.table_builder.get_table_creation_order(tables)

            for table_name in table_creation_order:
                if table_name in tables:
                    table_def = tables[table_name]
                    create_sql = self.table_builder.build_create_table_sql(table_def)

                    logger.debug(f"Creating table: {table_name}")
                    await self.query_executor.execute_command(create_sql)

            # Skip foreign key constraints - DuckDB has limited support
            # Foreign keys will be handled in future refactoring iterations
            logger.debug(
                "Skipping foreign key constraint creation - limited DuckDB support"
            )

            # Create indexes
            for index_def in indexes:
                try:
                    index_sql = self.table_builder.build_create_index_sql(index_def)
                    logger.debug(f"Creating index: {index_def.name}")
                    await self.query_executor.execute_command(index_sql)
                except Exception as e:
                    logger.warning(f"Failed to create index {index_def.name}: {str(e)}")

            # Create views
            for view_def in views:
                try:
                    view_sql = self.table_builder.build_create_view_sql(
                        view_def.name, view_def.sql
                    )
                    logger.debug(f"Creating view: {view_def.name}")
                    await self.query_executor.execute_command(view_sql)
                except Exception as e:
                    logger.warning(f"Failed to create view {view_def.name}: {str(e)}")

            # Set schema version
            await self.set_schema_version(PortfolioManagerSchema.SCHEMA_VERSION)

            logger.info("Database schema created successfully")

        except Exception as e:
            logger.error(f"Schema creation failed: {str(e)}")
            raise SchemaError(f"Failed to create schema: {str(e)}") from e

    async def drop_schema(self) -> None:
        """Drop the complete database schema."""
        try:
            logger.info("Dropping Portfolio Manager database schema...")

            # Get schema definitions
            tables = PortfolioManagerSchema.get_all_tables()
            indexes = PortfolioManagerSchema.get_all_indexes()
            views = PortfolioManagerSchema.get_all_views()

            # Drop views first
            for view_def in views:
                try:
                    drop_sql = self.table_builder.build_drop_view_sql(view_def.name)
                    await self.query_executor.execute_command(drop_sql)
                except Exception as e:
                    logger.debug(f"Failed to drop view {view_def.name}: {str(e)}")

            # Drop indexes
            for index_def in indexes:
                try:
                    drop_sql = self.table_builder.build_drop_index_sql(index_def.name)
                    await self.query_executor.execute_command(drop_sql)
                except Exception as e:
                    logger.debug(f"Failed to drop index {index_def.name}: {str(e)}")

            # Drop tables in reverse dependency order
            drop_order = self.table_builder.get_table_drop_order(tables)

            for table_name in drop_order:
                try:
                    drop_sql = self.table_builder.build_drop_table_sql(
                        table_name, cascade=True
                    )
                    await self.query_executor.execute_command(drop_sql)
                    logger.debug(f"Dropped table: {table_name}")
                except Exception as e:
                    logger.debug(f"Failed to drop table {table_name}: {str(e)}")

            self._current_version = None
            logger.info("Database schema dropped successfully")

        except Exception as e:
            logger.error(f"Schema drop failed: {str(e)}")
            raise SchemaError(f"Failed to drop schema: {str(e)}") from e

    async def schema_exists(self) -> bool:
        """Check if the database schema exists."""
        try:
            existing_tables = await self.inspector.get_existing_tables()

            # Check if core tables exist
            core_tables = {"assets", "portfolios", "trades", "positions"}
            return core_tables.issubset(existing_tables)

        except Exception as e:
            logger.error(f"Failed to check schema existence: {str(e)}")
            return False

    async def get_schema_info(self) -> dict[str, Any] | None:
        """Get general schema information including version and table count."""
        try:
            tables = await self.get_table_names()
            version = await self.get_schema_version()

            return {
                "version": version,
                "table_count": len(tables),
                "tables": sorted(tables),
                "schema_exists": len(tables) > 0,
            }
        except Exception as e:
            logger.error(f"Failed to get schema info: {str(e)}")
            return None

    async def get_schema_version(self) -> str | None:
        """Get the current schema version."""
        if self._current_version is not None:
            return self._current_version

        try:
            # Check if schema_migrations table exists
            if not await self.inspector.table_exists("schema_migrations"):
                return None

            # Get latest migration version
            result = await self.query_executor.execute_query(
                """
                SELECT version
                FROM schema_migrations
                WHERE success = true
                ORDER BY applied_at DESC
                LIMIT 1
            """
            )

            if result.rows:
                self._current_version = result.rows[0]["version"]
                return self._current_version

            return None

        except Exception as e:
            logger.error(f"Failed to get schema version: {str(e)}")
            return None

    async def set_schema_version(self, version: str) -> None:
        """Set the current schema version."""
        try:
            # Ensure schema_migrations table exists
            if not await self.inspector.table_exists("schema_migrations"):
                migrations_table = PortfolioManagerSchema.get_schema_migrations_table()
                create_sql = self.table_builder.build_create_table_sql(migrations_table)
                await self.query_executor.execute_command(create_sql)

            # Check if version already exists using parameterized query
            existing = await self.query_executor.execute_query(
                "SELECT COUNT(*) as count FROM schema_migrations WHERE version = $version",
                {"version": version},
            )

            if existing.rows[0]["count"] == 0:
                # Insert new version record
                await self.query_executor.execute_command(
                    """
                    INSERT INTO schema_migrations (version, name, migration_type, applied_at, checksum, success)
                    VALUES ($version, 'schema_creation', 'SCHEMA_INIT', CURRENT_TIMESTAMP, 'initial', true)
                """,
                    {"version": version},
                )
            else:
                # Update existing record
                await self.query_executor.execute_command(
                    """
                    UPDATE schema_migrations
                    SET applied_at = CURRENT_TIMESTAMP, success = true
                    WHERE version = $version
                """,
                    {"version": version},
                )

            self._current_version = version
            logger.debug(f"Schema version set to: {version}")

        except Exception as e:
            logger.error(f"Failed to set schema version: {str(e)}")
            raise SchemaError(f"Cannot set schema version: {str(e)}") from e

    async def get_table_names(self) -> set[str]:
        """Get names of all tables in the schema."""
        return await self.inspector.get_existing_tables()

    async def table_exists(self, table_name: str) -> bool:
        """Check if a specific table exists."""
        return await self.inspector.table_exists(table_name)

    async def get_table_definition(self, table_name: str) -> TableDefinition | None:
        """Get the definition of a specific table."""
        try:
            # Check if table exists
            if not await self.inspector.table_exists(table_name):
                return None

            # Get column structure
            columns = await self.inspector.get_table_structure(table_name)
            if columns is None:
                return None

            # Get indexes (simplified - just names)
            indexes = await self.inspector.get_table_indexes(table_name)
            index_names = [idx["name"] for idx in indexes]

            # Create table definition (simplified)
            return TableDefinition(
                name=table_name,
                columns=columns,
                primary_key=[],  # Would need more complex logic to determine
                foreign_keys={},  # Would need more complex logic to determine
                indexes=index_names,
                constraints=[],  # Would need more complex logic to determine
            )

        except Exception as e:
            logger.error(f"Failed to get table definition for {table_name}: {str(e)}")
            return None

    async def create_table(self, definition: TableDefinition) -> None:
        """Create a new table with the given definition."""
        try:
            create_sql = self.table_builder.build_create_table_sql(definition)
            await self.query_executor.execute_command(create_sql)

            # Skip foreign keys for now as DuckDB has limited support
            # Foreign key constraints will be handled in future iterations
            if definition.foreign_keys:
                logger.debug(
                    f"Skipping foreign key creation for {definition.name} - not fully supported in DuckDB"
                )

            logger.info(f"Created table: {definition.name}")

        except Exception as e:
            logger.error(f"Failed to create table {definition.name}: {str(e)}")
            raise SchemaError(f"Cannot create table {definition.name}: {str(e)}") from e

    async def drop_table(self, table_name: str) -> None:
        """Drop a table from the database."""
        try:
            drop_sql = self.table_builder.build_drop_table_sql(table_name, cascade=True)
            await self.query_executor.execute_command(drop_sql)
            logger.info(f"Dropped table: {table_name}")

        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {str(e)}")
            raise SchemaError(f"Cannot drop table {table_name}: {str(e)}") from e

    async def get_create_table_sql(self) -> dict[str, str]:
        """Get SQL statements for creating all application tables."""
        try:
            tables = PortfolioManagerSchema.get_all_tables()
            sql_statements = {}

            for table_name, table_def in tables.items():
                sql_statements[table_name] = self.table_builder.build_create_table_sql(
                    table_def
                )

            return sql_statements

        except Exception as e:
            logger.error(f"Failed to generate create table SQL: {str(e)}")
            raise SchemaError(f"Cannot generate create table SQL: {str(e)}") from e

    async def validate_schema(self) -> dict[str, Any]:
        """Validate current schema against expected definitions.

        Returns:
            Dictionary with validation results and recommendations
        """
        try:
            expected_tables = PortfolioManagerSchema.get_all_tables()
            validation_results = await self.inspector.validate_schema_integrity(
                expected_tables
            )

            # Add statistics
            stats = await self.inspector.get_database_statistics()
            validation_results["statistics"] = stats

            # Check referential integrity
            integrity_violations = await self.inspector.check_referential_integrity()
            validation_results["integrity_violations"] = integrity_violations

            # Overall status
            has_issues = (
                validation_results["missing_tables"]
                or validation_results["extra_tables"]
                or validation_results["column_mismatches"]
                or integrity_violations
            )

            validation_results["status"] = "INVALID" if has_issues else "VALID"
            validation_results["schema_version"] = await self.get_schema_version()

            return validation_results

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            return {
                "status": "ERROR",
                "error": str(e),
                "missing_tables": [],
                "extra_tables": [],
                "column_mismatches": [],
                "integrity_violations": [],
                "statistics": {},
            }
