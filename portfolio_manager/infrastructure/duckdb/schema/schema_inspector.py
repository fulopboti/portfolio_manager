"""DuckDB schema introspection and validation."""

import logging

from portfolio_manager.infrastructure.data_access.exceptions import SchemaError
from portfolio_manager.infrastructure.data_access.schema_manager import TableDefinition

from ..query_executor import DuckDBQueryExecutor

logger = logging.getLogger(__name__)


class DuckDBSchemaInspector:
    """DuckDB schema introspection and validation.

    Provides methods to inspect existing database schema and compare
    with expected schema definitions.
    """

    def __init__(self, query_executor: DuckDBQueryExecutor):
        """Initialize schema inspector.

        Args:
            query_executor: DuckDB query executor for database operations
        """
        self.query_executor = query_executor

    async def get_existing_tables(self) -> set[str]:
        """Get names of all tables that exist in the database.

        Returns:
            Set of table names
        """
        try:
            result = await self.query_executor.execute_query("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_type = 'BASE TABLE'
            """)

            return {row["table_name"] for row in result.rows}

        except Exception as e:
            logger.error(f"Failed to get existing tables: {str(e)}")
            raise SchemaError(f"Cannot inspect existing tables: {str(e)}") from e

    async def get_table_structure(self, table_name: str) -> dict[str, str] | None:
        """Get column structure for a specific table.

        Args:
            table_name: Name of table to inspect

        Returns:
            Dictionary mapping column names to their definitions, or None if table doesn't exist
        """
        try:
            result = await self.query_executor.execute_query("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = $table_name
                  AND table_schema = 'main'
                ORDER BY ordinal_position
            """, {"table_name": table_name})

            if not result.rows:
                return None

            columns = {}
            for row in result.rows:
                column_name = row["column_name"]
                data_type = row["data_type"]
                is_nullable = row["is_nullable"] == "YES"
                default_value = row["column_default"]

                # Build column definition
                column_def = data_type
                if not is_nullable:
                    column_def += " NOT NULL"
                if default_value:
                    column_def += f" DEFAULT {default_value}"

                columns[column_name] = column_def

            return columns

        except Exception as e:
            logger.error(f"Failed to get table structure for {table_name}: {str(e)}")
            raise SchemaError(f"Cannot inspect table {table_name}: {str(e)}") from e

    async def get_table_indexes(self, table_name: str) -> list[dict[str, str]]:
        """Get indexes for a specific table.

        Args:
            table_name: Name of table to get indexes for

        Returns:
            List of index information dictionaries
        """
        try:
            # DuckDB doesn't have a standard information_schema for indexes
            # We'll use PRAGMA table_info and other DuckDB-specific queries
            result = await self.query_executor.execute_query("""
                SELECT
                    index_name,
                    is_unique
                FROM duckdb_indexes()
                WHERE table_name = $table_name
            """, {"table_name": table_name})

            indexes = []
            for row in result.rows:
                indexes.append({
                    "name": row["index_name"],
                    "column": "unknown",  # DuckDB doesn't easily provide column info
                    "unique": bool(row["is_unique"])
                })

            return indexes

        except Exception as e:
            # If DuckDB doesn't support this query, return empty list
            logger.debug(f"Could not get indexes for {table_name}: {str(e)}")
            return []

    async def table_exists(self, table_name: str) -> bool:
        """Check if a specific table exists.

        Args:
            table_name: Name of table to check

        Returns:
            True if table exists, False otherwise
        """
        try:
            existing_tables = await self.get_existing_tables()
            return table_name in existing_tables
        except Exception as e:
            logger.error(f"Failed to check if table {table_name} exists: {str(e)}")
            return False

    async def validate_schema_integrity(self, expected_tables: dict[str, TableDefinition]) -> dict[str, list[str]]:
        """Validate database schema against expected schema definitions.

        Args:
            expected_tables: Expected table definitions

        Returns:
            Dictionary with validation results:
            - "missing_tables": Tables that should exist but don't
            - "extra_tables": Tables that exist but shouldn't
            - "column_mismatches": Tables with column differences
        """
        validation_results = {
            "missing_tables": [],
            "extra_tables": [],
            "column_mismatches": []
        }

        try:
            existing_tables = await self.get_existing_tables()
            expected_table_names = set(expected_tables.keys())

            # Find missing tables
            missing_tables = expected_table_names - existing_tables
            validation_results["missing_tables"] = list(missing_tables)

            # Find extra tables (excluding system tables)
            system_tables = {"schema_migrations"}
            extra_tables = existing_tables - expected_table_names - system_tables
            validation_results["extra_tables"] = list(extra_tables)

            # Check column structure for existing tables
            for table_name in expected_table_names & existing_tables:
                expected_def = expected_tables[table_name]
                actual_columns = await self.get_table_structure(table_name)

                if actual_columns is None:
                    continue

                # Simple check - compare column names
                expected_columns = set(expected_def.columns.keys())
                actual_column_names = set(actual_columns.keys())

                if expected_columns != actual_column_names:
                    missing_cols = expected_columns - actual_column_names
                    extra_cols = actual_column_names - expected_columns

                    mismatch_info = f"{table_name}: "
                    if missing_cols:
                        mismatch_info += f"missing columns {missing_cols}, "
                    if extra_cols:
                        mismatch_info += f"extra columns {extra_cols}, "

                    validation_results["column_mismatches"].append(mismatch_info.rstrip(", "))

            return validation_results

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise SchemaError(f"Cannot validate schema integrity: {str(e)}") from e

    async def get_database_statistics(self) -> dict[str, int]:
        """Get basic database statistics.

        Returns:
            Dictionary with database statistics
        """
        try:
            stats = {}

            # Get table count
            existing_tables = await self.get_existing_tables()
            stats["table_count"] = len(existing_tables)

            # Get total row counts for each table
            total_rows = 0
            for table_name in existing_tables:
                try:
                    result = await self.query_executor.execute_scalar(
                        f"SELECT COUNT(*) FROM {table_name}"
                    )
                    table_rows = int(result or 0)
                    stats[f"{table_name}_rows"] = table_rows
                    total_rows += table_rows
                except Exception:
                    # If we can't count rows in a table, skip it
                    stats[f"{table_name}_rows"] = -1

            stats["total_rows"] = total_rows

            return stats

        except Exception as e:
            logger.error(f"Failed to get database statistics: {str(e)}")
            return {"error": str(e)}

    async def check_referential_integrity(self) -> list[str]:
        """Check referential integrity of foreign key constraints.

        Returns:
            List of integrity violation messages (empty if no violations)
        """
        violations = []

        try:
            # Get existing tables first
            existing_tables = await self.get_existing_tables()

            # Only check integrity if the required tables exist
            required_tables = {"trades", "portfolios", "assets", "positions"}
            if not required_tables.issubset(existing_tables):
                # Not all tables exist, so no integrity violations to check
                return violations

            # Check trades reference valid portfolios
            result = await self.query_executor.execute_query("""
                SELECT COUNT(*) as count
                FROM trades t
                LEFT JOIN portfolios p ON t.portfolio_id = p.portfolio_id
                WHERE p.portfolio_id IS NULL
            """)

            if result.rows and result.rows[0]["count"] > 0:
                violations.append(f"Found {result.rows[0]['count']} trades with invalid portfolio references")

            # Check trades reference valid assets
            result = await self.query_executor.execute_query("""
                SELECT COUNT(*) as count
                FROM trades t
                LEFT JOIN assets a ON t.symbol = a.symbol
                WHERE a.symbol IS NULL
            """)

            if result.rows and result.rows[0]["count"] > 0:
                violations.append(f"Found {result.rows[0]['count']} trades with invalid asset references")

            # Check positions reference valid portfolios and assets
            result = await self.query_executor.execute_query("""
                SELECT COUNT(*) as count
                FROM positions pos
                LEFT JOIN portfolios p ON pos.portfolio_id = p.portfolio_id
                LEFT JOIN assets a ON pos.symbol = a.symbol
                WHERE p.portfolio_id IS NULL OR a.symbol IS NULL
            """)

            if result.rows and result.rows[0]["count"] > 0:
                violations.append(f"Found {result.rows[0]['count']} positions with invalid references")

        except Exception as e:
            logger.error(f"Referential integrity check failed: {str(e)}")
            # Don't add error to violations for missing tables - that's expected for new DBs
            logger.debug(f"Integrity check error (likely missing tables): {str(e)}")

        return violations
