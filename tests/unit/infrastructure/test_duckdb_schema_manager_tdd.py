"""TDD tests for DuckDB Schema Manager - driving implementation of missing features."""

import os
import tempfile
import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Set, Optional

from stockapp.infrastructure.duckdb.connection import DuckDBConnection
from stockapp.infrastructure.duckdb.query_executor import DuckDBQueryExecutor
from stockapp.infrastructure.duckdb.schema.schema_manager import DuckDBSchemaManager
from stockapp.infrastructure.duckdb.schema.schema_definitions import StockAppSchema
from stockapp.infrastructure.data_access.schema_manager import TableDefinition
from stockapp.infrastructure.data_access.exceptions import SchemaError


class TestDuckDBSchemaManagerTDD:
    """TDD tests for DuckDB Schema Manager implementation."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "schema_test.db")
        yield db_path
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest_asyncio.fixture
    async def connection(self, temp_db_path):
        """Create a connected DuckDB connection."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        yield conn
        await conn.disconnect()

    @pytest.fixture
    def query_executor(self, connection):
        """Create a DuckDB query executor."""
        return DuckDBQueryExecutor(connection)

    @pytest.fixture
    def schema_manager(self, query_executor):
        """Create a DuckDB schema manager."""
        return DuckDBSchemaManager(query_executor)

    def test_initialization_tdd(self, query_executor):
        """Test schema manager initialization with all required components."""
        schema_manager = DuckDBSchemaManager(query_executor)
        
        # Should initialize with all required components
        assert schema_manager.query_executor is query_executor
        assert hasattr(schema_manager, 'table_builder')
        assert hasattr(schema_manager, 'inspector')
        assert schema_manager._current_version is None

    @pytest.mark.asyncio
    async def test_create_schema_full_workflow_tdd(self, schema_manager):
        """Test complete schema creation workflow."""
        # Initially no schema should exist
        exists = await schema_manager.schema_exists()
        assert exists is False
        
        # Create the schema
        await schema_manager.create_schema()
        
        # Schema should now exist
        exists = await schema_manager.schema_exists()
        assert exists is True
        
        # All expected tables should be created
        table_names = await schema_manager.get_table_names()
        expected_tables = {
            "assets", "asset_snapshots", "asset_metrics", "portfolios", 
            "trades", "positions", "strategy_scores", "portfolio_metrics",
            "risk_metrics", "audit_events", "schema_migrations"
        }
        assert expected_tables.issubset(table_names)

    @pytest.mark.asyncio
    async def test_create_schema_with_optimization_tdd(self, schema_manager):
        """Test schema creation applies DuckDB optimizations."""
        # Mock the table builder to verify optimization is called
        with patch.object(schema_manager.table_builder, 'optimize_for_analytics') as mock_optimize:
            mock_optimize.return_value = "PRAGMA enable_profiling='json';\nPRAGMA enable_progress_bar=true;"
            
            await schema_manager.create_schema()
            
            # Optimization should have been called
            mock_optimize.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_handles_table_creation_order_tdd(self, schema_manager):
        """Test schema creation respects table dependency order."""
        with patch.object(schema_manager.table_builder, 'get_table_creation_order') as mock_order:
            expected_order = ["assets", "portfolios", "positions", "trades"]
            mock_order.return_value = expected_order
            
            await schema_manager.create_schema()
            
            # Table creation order should have been determined
            mock_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_handles_indexes_tdd(self, schema_manager):
        """Test schema creation creates all indexes."""
        await schema_manager.create_schema()
        
        # Verify indexes were created by checking if they exist
        # This tests that the schema manager processes StockAppSchema.get_all_indexes()
        result = await schema_manager.query_executor.execute_query("""
            SELECT name FROM sqlite_master 
            WHERE type = 'index' AND name NOT LIKE 'sqlite_%'
        """)
        
        # Should have created some indexes
        assert len(result.rows) > 0

    @pytest.mark.asyncio
    async def test_create_schema_handles_views_tdd(self, schema_manager):
        """Test schema creation creates all views."""
        await schema_manager.create_schema()
        
        # Verify views were created
        result = await schema_manager.query_executor.execute_query("""
            SELECT name FROM sqlite_master 
            WHERE type = 'view'
        """)
        
        view_names = {row["name"] for row in result.rows}
        expected_views = {"portfolio_summary", "latest_asset_prices", "daily_portfolio_performance"}
        assert expected_views.issubset(view_names)

    @pytest.mark.asyncio
    async def test_create_schema_sets_version_tdd(self, schema_manager):
        """Test schema creation sets the schema version."""
        await schema_manager.create_schema()
        
        # Schema version should be set
        version = await schema_manager.get_schema_version()
        assert version == StockAppSchema.SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_create_schema_idempotent_tdd(self, schema_manager):
        """Test schema creation is idempotent (can be run multiple times safely)."""
        # Create schema first time
        await schema_manager.create_schema()
        table_count_1 = len(await schema_manager.get_table_names())
        
        # Create schema second time - should not fail or duplicate
        await schema_manager.create_schema()
        table_count_2 = len(await schema_manager.get_table_names())
        
        # Should have same number of tables
        assert table_count_1 == table_count_2

    @pytest.mark.asyncio
    async def test_create_schema_error_handling_tdd(self, schema_manager):
        """Test schema creation handles errors gracefully."""
        # Mock query executor to simulate failure
        with patch.object(schema_manager.query_executor, 'execute_command') as mock_exec:
            mock_exec.side_effect = Exception("Database error")
            
            with pytest.raises(SchemaError) as exc_info:
                await schema_manager.create_schema()
            
            assert "Failed to create schema" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_drop_schema_removes_all_tables_tdd(self, schema_manager):
        """Test drop_schema removes all application tables."""
        # Create schema first
        await schema_manager.create_schema()
        assert await schema_manager.schema_exists() is True
        
        # Drop schema
        await schema_manager.drop_schema()
        
        # Schema should no longer exist
        assert await schema_manager.schema_exists() is False
        
        # No application tables should remain
        table_names = await schema_manager.get_table_names()
        expected_tables = {
            "assets", "asset_snapshots", "portfolios", "trades", 
            "positions", "strategy_scores"
        }
        assert not expected_tables.intersection(table_names)

    @pytest.mark.asyncio
    async def test_drop_schema_handles_dependencies_tdd(self, schema_manager):
        """Test drop_schema handles table dependencies correctly."""
        await schema_manager.create_schema()
        
        with patch.object(schema_manager.table_builder, 'get_table_drop_order') as mock_order:
            expected_order = ["trades", "positions", "portfolios", "assets"]
            mock_order.return_value = expected_order
            
            await schema_manager.drop_schema()
            
            # Table drop order should have been determined
            mock_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_drop_schema_removes_views_and_indexes_tdd(self, schema_manager):
        """Test drop_schema removes views and indexes before tables."""
        await schema_manager.create_schema()
        
        # Verify views exist before drop
        result = await schema_manager.query_executor.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'view'"
        )
        assert len(result.rows) > 0
        
        await schema_manager.drop_schema()
        
        # Views should be gone
        result = await schema_manager.query_executor.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'view'"
        )
        assert len(result.rows) == 0

    @pytest.mark.asyncio
    async def test_drop_schema_error_handling_tdd(self, schema_manager):
        """Test drop_schema handles errors gracefully and continues trying to drop."""
        # Test that drop_schema continues even when individual operations fail
        with patch.object(schema_manager.query_executor, 'execute_command') as mock_exec:
            mock_exec.side_effect = Exception("Cannot drop table")
            
            # Should not raise error - it logs errors but continues
            # This is the robust behavior - try to clean up as much as possible
            await schema_manager.drop_schema()
            
            # Should have attempted to call execute_command multiple times
            assert mock_exec.call_count > 0

    @pytest.mark.asyncio
    async def test_schema_exists_when_no_tables_tdd(self, schema_manager):
        """Test schema_exists returns False when no tables exist."""
        exists = await schema_manager.schema_exists()
        assert exists is False

    @pytest.mark.asyncio
    async def test_schema_exists_when_partial_schema_tdd(self, schema_manager):
        """Test schema_exists returns False when only some tables exist."""
        # Create just one table manually
        await schema_manager.query_executor.execute_command(
            "CREATE TABLE assets (symbol VARCHAR PRIMARY KEY, name VARCHAR)"
        )
        
        # Should still return False as schema is incomplete
        exists = await schema_manager.schema_exists()
        assert exists is False

    @pytest.mark.asyncio
    async def test_schema_exists_when_complete_schema_tdd(self, schema_manager):
        """Test schema_exists returns True when complete schema exists."""
        await schema_manager.create_schema()
        
        exists = await schema_manager.schema_exists()
        assert exists is True

    @pytest.mark.asyncio
    async def test_get_schema_version_no_version_tdd(self, schema_manager):
        """Test get_schema_version returns None when no version is set."""
        version = await schema_manager.get_schema_version()
        assert version is None

    @pytest.mark.asyncio
    async def test_get_schema_version_with_version_tdd(self, schema_manager):
        """Test get_schema_version returns correct version when set."""
        await schema_manager.create_schema()
        
        version = await schema_manager.get_schema_version()
        assert version == StockAppSchema.SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_set_schema_version_tdd(self, schema_manager):
        """Test set_schema_version stores version correctly."""
        test_version = "2.0.0"
        
        await schema_manager.set_schema_version(test_version)
        
        # Should be able to retrieve the version
        retrieved_version = await schema_manager.get_schema_version()
        assert retrieved_version == test_version

    @pytest.mark.asyncio
    async def test_set_schema_version_creates_metadata_table_tdd(self, schema_manager):
        """Test set_schema_version creates metadata table if needed."""
        await schema_manager.set_schema_version("1.0.0")
        
        # Schema migrations table should exist (used for version tracking)
        result = await schema_manager.query_executor.execute_query("""
            SELECT name FROM sqlite_master 
            WHERE type = 'table' AND name = 'schema_migrations'
        """)
        assert len(result.rows) == 1

    @pytest.mark.asyncio
    async def test_get_table_names_empty_database_tdd(self, schema_manager):
        """Test get_table_names returns empty set for empty database."""
        table_names = await schema_manager.get_table_names()
        assert isinstance(table_names, set)
        assert len(table_names) == 0

    @pytest.mark.asyncio
    async def test_get_table_names_with_tables_tdd(self, schema_manager):
        """Test get_table_names returns correct table names."""
        await schema_manager.create_schema()
        
        table_names = await schema_manager.get_table_names()
        
        assert isinstance(table_names, set)
        expected_tables = {"assets", "portfolios", "trades", "positions"}
        assert expected_tables.issubset(table_names)

    @pytest.mark.asyncio
    async def test_table_exists_false_tdd(self, schema_manager):
        """Test table_exists returns False for non-existent table."""
        exists = await schema_manager.table_exists("non_existent_table")
        assert exists is False

    @pytest.mark.asyncio
    async def test_table_exists_true_tdd(self, schema_manager):
        """Test table_exists returns True for existing table."""
        await schema_manager.create_schema()
        
        exists = await schema_manager.table_exists("assets")
        assert exists is True

    @pytest.mark.asyncio
    async def test_get_table_definition_non_existent_tdd(self, schema_manager):
        """Test get_table_definition returns None for non-existent table."""
        table_def = await schema_manager.get_table_definition("non_existent")
        assert table_def is None

    @pytest.mark.asyncio
    async def test_get_table_definition_existing_table_tdd(self, schema_manager):
        """Test get_table_definition returns correct definition for existing table."""
        await schema_manager.create_schema()
        
        table_def = await schema_manager.get_table_definition("assets")
        
        assert table_def is not None
        assert isinstance(table_def, TableDefinition)
        assert table_def.name == "assets"
        assert "symbol" in table_def.columns
        assert "exchange" in table_def.columns

    @pytest.mark.asyncio
    async def test_create_table_success_tdd(self, schema_manager):
        """Test create_table creates individual table successfully."""
        table_def = TableDefinition(
            name="test_table",
            columns={"id": "INTEGER PRIMARY KEY", "name": "VARCHAR NOT NULL"},
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=[]
        )
        
        await schema_manager.create_table(table_def)
        
        # Table should exist
        exists = await schema_manager.table_exists("test_table")
        assert exists is True

    @pytest.mark.asyncio
    async def test_create_table_duplicate_error_tdd(self, schema_manager):
        """Test create_table handles duplicate table creation."""
        table_def = TableDefinition(
            name="duplicate_table",
            columns={"id": "INTEGER PRIMARY KEY"},
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=[]
        )
        
        # Create table first time
        await schema_manager.create_table(table_def)
        
        # Creating again should handle gracefully (not fail)
        await schema_manager.create_table(table_def)

    @pytest.mark.asyncio
    async def test_drop_table_success_tdd(self, schema_manager):
        """Test drop_table removes table successfully."""
        # Create table first
        table_def = TableDefinition(
            name="drop_test",
            columns={"id": "INTEGER PRIMARY KEY"},
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=[]
        )
        await schema_manager.create_table(table_def)
        
        # Drop the table
        await schema_manager.drop_table("drop_test")
        
        # Table should no longer exist
        exists = await schema_manager.table_exists("drop_test")
        assert exists is False

    @pytest.mark.asyncio
    async def test_drop_table_non_existent_tdd(self, schema_manager):
        """Test drop_table handles non-existent table gracefully."""
        # Should not raise error
        await schema_manager.drop_table("non_existent_table")

    @pytest.mark.asyncio
    async def test_get_create_table_sql_tdd(self, schema_manager):
        """Test get_create_table_sql returns all table creation statements."""
        sql_statements = await schema_manager.get_create_table_sql()
        
        assert isinstance(sql_statements, dict)
        
        # Should include all schema tables
        expected_tables = {"assets", "portfolios", "trades", "positions"}
        for table_name in expected_tables:
            assert table_name in sql_statements
            assert "CREATE TABLE" in sql_statements[table_name]

    @pytest.mark.asyncio
    async def test_schema_validation_workflow_tdd(self, schema_manager):
        """Test complete schema validation workflow."""
        # Initially schema is invalid
        assert await schema_manager.schema_exists() is False
        
        # Create schema
        await schema_manager.create_schema()
        
        # Schema should be valid
        assert await schema_manager.schema_exists() is True
        
        # Version should be set
        version = await schema_manager.get_schema_version()
        assert version is not None
        
        # All required tables should exist
        required_tables = {"assets", "portfolios", "trades", "positions"}
        table_names = await schema_manager.get_table_names()
        assert required_tables.issubset(table_names)

    @pytest.mark.asyncio
    async def test_concurrent_schema_operations_tdd(self, schema_manager):
        """Test schema operations are safe under concurrent access."""
        import asyncio
        
        # Multiple concurrent schema creations should be safe
        tasks = [schema_manager.create_schema() for _ in range(3)]
        
        # Should complete without error
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Schema should exist and be valid
        assert await schema_manager.schema_exists() is True

    @pytest.mark.asyncio
    async def test_schema_manager_integration_with_components_tdd(self, schema_manager):
        """Test schema manager integrates correctly with table builder and inspector."""
        # Verify components are properly integrated
        assert schema_manager.table_builder is not None
        assert schema_manager.inspector is not None
        
        # Components should be usable
        tables = StockAppSchema.get_all_tables()
        table_order = schema_manager.table_builder.get_table_creation_order(tables)
        assert isinstance(table_order, list)
        assert len(table_order) > 0
        
        # Inspector should work after schema creation
        await schema_manager.create_schema()
        existing_tables = await schema_manager.inspector.get_existing_tables()
        assert len(existing_tables) > 0