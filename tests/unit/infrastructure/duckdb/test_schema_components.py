"""Comprehensive tests for DuckDB schema components."""

import asyncio
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock
import pytest
import pytest_asyncio

from portfolio_manager.infrastructure.duckdb.connection import DuckDBConnection, DuckDBTransactionManager
from portfolio_manager.infrastructure.duckdb.query_executor import DuckDBQueryExecutor
from portfolio_manager.infrastructure.duckdb.schema.schema_definitions import IndexDefinition, PortfolioManagerSchema, ViewDefinition
from portfolio_manager.infrastructure.duckdb.schema.table_builder import DuckDBTableBuilder
from portfolio_manager.infrastructure.duckdb.schema.schema_inspector import DuckDBSchemaInspector
from portfolio_manager.infrastructure.duckdb.schema.schema_manager import DuckDBSchemaManager

from portfolio_manager.infrastructure.data_access.schema_manager import TableDefinition
from portfolio_manager.infrastructure.data_access.exceptions import SchemaError, QueryError


class TestPortfolioManagerSchema:
    """Test cases for PortfolioManagerSchema definitions."""

    def test_schema_version(self):
        """Test schema version is defined."""
        assert hasattr(PortfolioManagerSchema, 'SCHEMA_VERSION')
        assert isinstance(PortfolioManagerSchema.SCHEMA_VERSION, str)
        assert len(PortfolioManagerSchema.SCHEMA_VERSION) > 0

    def test_get_assets_table(self):
        """Test assets table definition."""
        table_def = PortfolioManagerSchema.get_assets_table()
        
        assert isinstance(table_def, TableDefinition)
        assert table_def.name == "assets"
        assert "symbol" in table_def.columns
        assert "asset_type" in table_def.columns
        assert "exchange" in table_def.columns
        assert table_def.primary_key == ["symbol"]

    def test_get_portfolios_table(self):
        """Test portfolios table definition."""
        table_def = PortfolioManagerSchema.get_portfolios_table()
        
        assert table_def.name == "portfolios"
        assert "portfolio_id" in table_def.columns
        assert "name" in table_def.columns
        assert "created_at" in table_def.columns
        assert table_def.primary_key == ["portfolio_id"]

    def test_get_trades_table(self):
        """Test trades table definition."""
        table_def = PortfolioManagerSchema.get_trades_table()
        
        assert table_def.name == "trades"
        assert "trade_id" in table_def.columns
        assert "portfolio_id" in table_def.columns
        assert "symbol" in table_def.columns
        assert "qty" in table_def.columns
        assert "price" in table_def.columns
        assert table_def.primary_key == ["trade_id"]

    def test_get_positions_table(self):
        """Test positions table definition."""
        table_def = PortfolioManagerSchema.get_positions_table()
        
        assert table_def.name == "positions"
        assert "portfolio_id" in table_def.columns
        assert "symbol" in table_def.columns
        assert "qty" in table_def.columns
        assert "avg_cost" in table_def.columns
        assert table_def.primary_key == ["portfolio_id", "symbol"]

    def test_get_asset_snapshots_table(self):
        """Test asset snapshots table definition."""
        table_def = PortfolioManagerSchema.get_asset_snapshots_table()
        
        assert table_def.name == "asset_snapshots"
        assert "symbol" in table_def.columns
        assert "timestamp" in table_def.columns
        assert "close" in table_def.columns  # Asset snapshots use OHLC pricing
        assert "volume" in table_def.columns

    def test_get_schema_migrations_table(self):
        """Test schema migrations table definition."""
        table_def = PortfolioManagerSchema.get_schema_migrations_table()
        
        assert table_def.name == "schema_migrations"
        assert "version" in table_def.columns
        assert "applied_at" in table_def.columns
        assert "success" in table_def.columns
        assert table_def.primary_key == ["version"]

    def test_get_all_tables(self):
        """Test getting all table definitions."""
        tables = PortfolioManagerSchema.get_all_tables()
        
        assert isinstance(tables, dict)
        expected_tables = {
            "assets", "portfolios", "trades", "positions", 
            "asset_snapshots", "asset_metrics", "risk_metrics",
            "strategy_scores", "portfolio_metrics", "audit_events"
        }
        
        assert set(tables.keys()) >= expected_tables
        
        # Verify all are TableDefinition instances
        for table_name, table_def in tables.items():
            assert isinstance(table_def, TableDefinition)
            assert table_def.name == table_name

    def test_get_all_indexes(self):
        """Test getting all index definitions."""
        indexes = PortfolioManagerSchema.get_all_indexes()
        
        assert isinstance(indexes, list)
        assert len(indexes) > 0
        
        # Verify all are IndexDefinition instances
        for index_def in indexes:
            assert isinstance(index_def, IndexDefinition)
            assert hasattr(index_def, 'name')
            assert hasattr(index_def, 'table')
            assert hasattr(index_def, 'columns')

    def test_get_all_views(self):
        """Test getting all view definitions."""
        views = PortfolioManagerSchema.get_all_views()
        
        assert isinstance(views, list)
        # Views are optional, so just check structure if any exist
        for view_def in views:
            assert isinstance(view_def, ViewDefinition)
            assert hasattr(view_def, 'name')
            assert hasattr(view_def, 'sql')


class TestDuckDBTableBuilder:
    """Test cases for DuckDBTableBuilder."""

    @pytest.fixture
    def table_builder(self):
        """Create a table builder instance."""
        return DuckDBTableBuilder()

    def test_initialization(self, table_builder):
        """Test table builder initialization."""
        assert isinstance(table_builder, DuckDBTableBuilder)

    def test_build_create_table_sql_simple(self, table_builder):
        """Test simple table creation SQL generation."""
        table_def = TableDefinition(
            name="simple_table",
            columns={
                "id": "INTEGER PRIMARY KEY",
                "name": "VARCHAR NOT NULL"
            },
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=[]
        )
        
        sql = table_builder.build_create_table_sql(table_def)
        
        assert "CREATE TABLE IF NOT EXISTS simple_table" in sql
        assert "id INTEGER PRIMARY KEY" in sql
        assert "name VARCHAR NOT NULL" in sql

    def test_build_create_table_sql_with_constraints(self, table_builder):
        """Test table creation SQL with constraints."""
        table_def = TableDefinition(
            name="constrained_table",
            columns={
                "id": "INTEGER",
                "email": "VARCHAR",
                "age": "INTEGER"
            },
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=["UNIQUE(email)", "CHECK(age >= 0)"]
        )
        
        sql = table_builder.build_create_table_sql(table_def)
        
        # Check for table name and constraints
        assert "CREATE TABLE IF NOT EXISTS constrained_table" in sql
        assert "UNIQUE(email)" in sql
        assert "CHECK(age >= 0)" in sql

    def test_build_drop_table_sql(self, table_builder):
        """Test table drop SQL generation."""
        sql = table_builder.build_drop_table_sql("test_table")
        assert sql == "DROP TABLE IF EXISTS test_table;"
        
        sql_cascade = table_builder.build_drop_table_sql("test_table", cascade=True)
        assert sql_cascade == "DROP TABLE IF EXISTS test_table CASCADE;"

    def test_build_create_index_sql(self, table_builder):
        """Test index creation SQL generation."""
        index_def = IndexDefinition(
            name="idx_test",
            table="test_table",
            columns=["column1", "column2"],
            unique=False
        )
        
        sql = table_builder.build_create_index_sql(index_def)
        
        assert "CREATE INDEX idx_test" in sql
        assert "ON test_table" in sql
        assert "(column1, column2)" in sql

    def test_build_create_index_sql_unique(self, table_builder):
        """Test unique index creation SQL generation."""
        index_def = IndexDefinition(
            name="idx_unique",
            table="test_table",
            columns=["email"],
            unique=True
        )
        
        sql = table_builder.build_create_index_sql(index_def)
        assert "CREATE UNIQUE INDEX" in sql

    def test_build_drop_index_sql(self, table_builder):
        """Test index drop SQL generation."""
        sql = table_builder.build_drop_index_sql("idx_test")
        assert sql == "DROP INDEX IF EXISTS idx_test;"

    def test_build_add_foreign_key_sql(self, table_builder):
        """Test foreign key addition SQL generation."""
        sql = table_builder.build_add_foreign_key_sql(
            "child_table", "parent_id", "parent_table.id"
        )
        
        assert "ALTER TABLE child_table" in sql
        assert "ADD CONSTRAINT" in sql
        assert "FOREIGN KEY (parent_id)" in sql
        assert "REFERENCES parent_table.id" in sql

    def test_build_create_view_sql(self, table_builder):
        """Test view creation SQL generation."""
        view_sql = "SELECT id, name FROM users WHERE active = true"
        sql = table_builder.build_create_view_sql("active_users", view_sql)
        
        assert "CREATE VIEW active_users AS" in sql
        assert view_sql in sql

    def test_build_drop_view_sql(self, table_builder):
        """Test view drop SQL generation."""
        sql = table_builder.build_drop_view_sql("test_view")
        assert sql == "DROP VIEW IF EXISTS test_view;"

    def test_get_table_creation_order(self, table_builder):
        """Test table creation order determination."""
        tables = {
            "parent": TableDefinition(
                name="parent",
                columns={"id": "INTEGER PRIMARY KEY"},
                primary_key=["id"],
                foreign_keys={},
                indexes=[],
                constraints=[]
            ),
            "child": TableDefinition(
                name="child",
                columns={"id": "INTEGER", "parent_id": "INTEGER"},
                primary_key=["id"],
                foreign_keys={"parent_id": "parent.id"},
                indexes=[],
                constraints=[]
            ),
            "independent": TableDefinition(
                name="independent",
                columns={"id": "INTEGER PRIMARY KEY"},
                primary_key=["id"],
                foreign_keys={},
                indexes=[],
                constraints=[]
            )
        }
        
        order = table_builder.get_table_creation_order(tables)
        
        # Parent should come before child
        parent_idx = order.index("parent")
        child_idx = order.index("child")
        assert parent_idx < child_idx
        
        # Independent can be anywhere
        assert "independent" in order

    def test_get_table_drop_order(self, table_builder):
        """Test table drop order determination."""
        tables = {
            "parent": TableDefinition(
                name="parent",
                columns={"id": "INTEGER PRIMARY KEY"},
                primary_key=["id"],
                foreign_keys={},
                indexes=[],
                constraints=[]
            ),
            "child": TableDefinition(
                name="child", 
                columns={"id": "INTEGER", "parent_id": "INTEGER"},
                primary_key=["id"],
                foreign_keys={"parent_id": "parent.id"},
                indexes=[],
                constraints=[]
            )
        }
        
        order = table_builder.get_table_drop_order(tables)
        
        # Child should be dropped before parent (reverse of creation order)
        child_idx = order.index("child")
        parent_idx = order.index("parent")
        assert child_idx < parent_idx

    def test_build_complete_schema_sql(self, table_builder):
        """Test complete schema SQL generation."""
        tables = PortfolioManagerSchema.get_all_tables()
        indexes = PortfolioManagerSchema.get_all_indexes()
        
        sql = table_builder.build_complete_schema_sql(tables, indexes)
        
        assert "-- Portfolio Manager Database Schema" in sql
        assert "-- Create Tables" in sql
        assert "CREATE TABLE" in sql
        
        if indexes:
            assert "-- Create Indexes" in sql


class TestDuckDBSchemaInspector:
    """Test cases for DuckDBSchemaInspector."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.db")
        yield db_path
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    @pytest_asyncio.fixture
    async def query_executor(self, temp_db_path):
        """Create a connected query executor."""
        connection = DuckDBConnection(temp_db_path)
        await connection.connect()
        executor = DuckDBQueryExecutor(connection)
        yield executor
        await connection.disconnect()

    @pytest.fixture
    def schema_inspector(self, query_executor):
        """Create a schema inspector."""
        return DuckDBSchemaInspector(query_executor)

    def test_initialization(self, query_executor):
        """Test schema inspector initialization."""
        inspector = DuckDBSchemaInspector(query_executor)
        assert inspector.query_executor is query_executor

    @pytest.mark.asyncio
    async def test_get_existing_tables(self, schema_inspector, query_executor):
        """Test getting existing tables."""
        # Create test tables
        await query_executor.execute_command("CREATE TABLE test1 (id INTEGER)")
        await query_executor.execute_command("CREATE TABLE test2 (id INTEGER)")
        
        tables = await schema_inspector.get_existing_tables()
        
        assert isinstance(tables, set)
        assert "test1" in tables
        assert "test2" in tables

    @pytest.mark.asyncio
    async def test_table_exists(self, schema_inspector, query_executor):
        """Test table existence checking."""
        # Initially doesn't exist
        exists = await schema_inspector.table_exists("test_table")
        assert exists is False
        
        # Create table
        await query_executor.execute_command("CREATE TABLE test_table (id INTEGER)")
        
        # Now should exist
        exists = await schema_inspector.table_exists("test_table")
        assert exists is True

    @pytest.mark.asyncio
    async def test_get_table_structure(self, schema_inspector, query_executor):
        """Test getting table structure."""
        # Create test table
        await query_executor.execute_command("""
            CREATE TABLE struct_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                age INTEGER,
                active BOOLEAN DEFAULT true
            )
        """)
        
        structure = await schema_inspector.get_table_structure("struct_test")
        
        assert isinstance(structure, dict)
        assert "id" in structure
        assert "name" in structure
        assert "age" in structure
        assert "active" in structure

    @pytest.mark.asyncio
    async def test_get_table_structure_nonexistent(self, schema_inspector):
        """Test getting structure of nonexistent table."""
        structure = await schema_inspector.get_table_structure("nonexistent")
        assert structure is None

    @pytest.mark.asyncio
    async def test_get_table_indexes(self, schema_inspector, query_executor):
        """Test getting table indexes."""
        # Create test table and index
        await query_executor.execute_command("CREATE TABLE idx_test (id INTEGER, name VARCHAR)")
        await query_executor.execute_command("CREATE INDEX idx_name ON idx_test (name)")
        
        indexes = await schema_inspector.get_table_indexes("idx_test")
        
        assert isinstance(indexes, list)
        # Check if our index is found (may include system indexes)
        index_names = [idx.get("name", "") for idx in indexes]
        assert any("idx_name" in name for name in index_names)

    @pytest.mark.asyncio
    async def test_get_database_statistics(self, schema_inspector, query_executor):
        """Test getting database statistics."""
        # Create some test data
        await query_executor.execute_command("CREATE TABLE stats_test (id INTEGER)")
        await query_executor.execute_command("INSERT INTO stats_test VALUES (1), (2), (3)")
        
        stats = await schema_inspector.get_database_statistics()
        
        assert isinstance(stats, dict)
        assert "table_count" in stats
        assert stats["table_count"] >= 1

    @pytest.mark.asyncio
    async def test_check_referential_integrity(self, schema_inspector):
        """Test referential integrity checking."""
        violations = await schema_inspector.check_referential_integrity()
        
        assert isinstance(violations, list)
        # Should be empty for new database
        assert len(violations) == 0

    @pytest.mark.asyncio
    async def test_validate_schema_integrity(self, schema_inspector, query_executor):
        """Test schema integrity validation."""
        # Create expected tables
        expected_tables = {
            "users": TableDefinition(
                name="users",
                columns={"id": "INTEGER PRIMARY KEY", "name": "VARCHAR"},
                primary_key=["id"],
                foreign_keys={},
                indexes=[],
                constraints=[]
            )
        }
        
        # Create the actual table
        await query_executor.execute_command("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)")
        
        result = await schema_inspector.validate_schema_integrity(expected_tables)
        
        assert isinstance(result, dict)
        assert "missing_tables" in result
        assert "extra_tables" in result
        assert "column_mismatches" in result
        
        # Should have no missing tables since we created it
        assert "users" not in result["missing_tables"]


class TestDuckDBSchemaManager:
    """Test cases for DuckDBSchemaManager."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.db")
        yield db_path
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    @pytest_asyncio.fixture
    async def query_executor(self, temp_db_path):
        """Create a connected query executor."""
        connection = DuckDBConnection(temp_db_path)
        await connection.connect()
        executor = DuckDBQueryExecutor(connection)
        yield executor
        await connection.disconnect()

    @pytest.fixture
    def schema_manager(self, query_executor):
        """Create a schema manager."""
        return DuckDBSchemaManager(query_executor)

    def test_initialization(self, query_executor):
        """Test schema manager initialization."""
        manager = DuckDBSchemaManager(query_executor)
        assert manager.query_executor is query_executor
        assert hasattr(manager, 'table_builder')
        assert hasattr(manager, 'inspector')

    @pytest.mark.asyncio
    async def test_create_schema(self, schema_manager):
        """Test complete schema creation."""
        await schema_manager.create_schema()
        
        # Verify core tables were created
        tables = await schema_manager.get_table_names()
        expected_core_tables = {"assets", "portfolios", "trades", "positions"}
        assert expected_core_tables.issubset(tables)

    @pytest.mark.asyncio
    async def test_schema_exists(self, schema_manager):
        """Test schema existence checking."""
        # Initially should not exist
        exists = await schema_manager.schema_exists()
        assert exists is False
        
        # Create schema
        await schema_manager.create_schema()
        
        # Now should exist
        exists = await schema_manager.schema_exists()
        assert exists is True

    @pytest.mark.asyncio
    async def test_drop_schema(self, schema_manager):
        """Test schema dropping."""
        # Create schema first
        await schema_manager.create_schema()
        assert await schema_manager.schema_exists() is True
        
        # Drop schema
        await schema_manager.drop_schema()
        
        # Should no longer exist
        exists = await schema_manager.schema_exists()
        assert exists is False

    @pytest.mark.asyncio
    async def test_get_schema_version(self, schema_manager):
        """Test schema version retrieval."""
        # Initially no version
        version = await schema_manager.get_schema_version()
        assert version is None
        
        # Create schema
        await schema_manager.create_schema()
        
        # Should have version
        version = await schema_manager.get_schema_version()
        assert version == PortfolioManagerSchema.SCHEMA_VERSION

    @pytest.mark.asyncio
    async def test_set_schema_version(self, schema_manager):
        """Test schema version setting."""
        test_version = "0.1.0"
        await schema_manager.set_schema_version(test_version)
        
        version = await schema_manager.get_schema_version()
        assert version == test_version

    @pytest.mark.asyncio
    async def test_table_exists(self, schema_manager, query_executor):
        """Test table existence checking."""
        # Initially doesn't exist
        exists = await schema_manager.table_exists("test_table")
        assert exists is False
        
        # Create table
        await query_executor.execute_command("CREATE TABLE test_table (id INTEGER)")
        
        # Now should exist
        exists = await schema_manager.table_exists("test_table")
        assert exists is True

    @pytest.mark.asyncio
    async def test_create_table(self, schema_manager):
        """Test individual table creation."""
        table_def = TableDefinition(
            name="custom_table",
            columns={
                "id": "INTEGER PRIMARY KEY",
                "data": "VARCHAR NOT NULL"
            },
            primary_key=["id"],
            foreign_keys={},
            indexes=[],
            constraints=[]
        )
        
        await schema_manager.create_table(table_def)
        
        # Verify table was created
        exists = await schema_manager.table_exists("custom_table")
        assert exists is True

    @pytest.mark.asyncio
    async def test_drop_table(self, schema_manager, query_executor):
        """Test individual table dropping."""
        # Create table first
        await query_executor.execute_command("CREATE TABLE drop_test (id INTEGER)")
        assert await schema_manager.table_exists("drop_test") is True
        
        # Drop table
        await schema_manager.drop_table("drop_test")
        
        # Should no longer exist
        exists = await schema_manager.table_exists("drop_test")
        assert exists is False

    @pytest.mark.asyncio
    async def test_get_create_table_sql(self, schema_manager):
        """Test SQL generation for all tables."""
        sql_statements = await schema_manager.get_create_table_sql()
        
        assert isinstance(sql_statements, dict)
        assert len(sql_statements) > 0
        
        # Check some expected tables
        expected_tables = ["assets", "portfolios", "trades"]
        for table_name in expected_tables:
            assert table_name in sql_statements
            assert "CREATE TABLE" in sql_statements[table_name]

    @pytest.mark.asyncio
    async def test_validate_schema(self, schema_manager):
        """Test schema validation."""
        # Create schema first
        await schema_manager.create_schema()
        
        validation = await schema_manager.validate_schema()
        
        assert isinstance(validation, dict)
        assert "status" in validation
        assert "schema_version" in validation
        assert "missing_tables" in validation
        assert "extra_tables" in validation


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
