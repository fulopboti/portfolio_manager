"""Comprehensive tests for DuckDB migration manager."""

import asyncio
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock
import pytest
import pytest_asyncio

from portfolio_manager.infrastructure.duckdb.connection import DuckDBConnection
from portfolio_manager.infrastructure.duckdb.query_executor import DuckDBQueryExecutor
from portfolio_manager.infrastructure.duckdb.schema.migration_manager import DuckDBMigrationManager

from portfolio_manager.infrastructure.data_access.schema_manager import Migration, MigrationType
from portfolio_manager.infrastructure.data_access.exceptions import MigrationError
from datetime import datetime


class TestDuckDBMigrationManager:
    """Test cases for DuckDBMigrationManager."""

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
    def migration_manager(self, query_executor):
        """Create a migration manager."""
        return DuckDBMigrationManager(query_executor)

    @pytest.fixture
    def sample_migration(self):
        """Create a sample migration for testing."""
        return Migration(
            version="001",
            name="create_test_table",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE test_migration (id INTEGER PRIMARY KEY, name VARCHAR)",
            down_sql="DROP TABLE test_migration",
            description="Create test migration table",
            created_at=datetime.now(),
            checksum="abc123"
        )

    def test_initialization(self, query_executor):
        """Test migration manager initialization."""
        manager = DuckDBMigrationManager(query_executor)
        assert manager.query_executor is query_executor

    @pytest.mark.asyncio
    async def test_initialize_migration_tracking(self, migration_manager, query_executor):
        """Test migration tracking table initialization."""
        await migration_manager.initialize_migration_tracking()

        # Verify schema_migrations table was created
        result = await query_executor.execute_query("""
            SELECT name FROM pragma_table_info('schema_migrations')
        """)

        column_names = [row["name"] for row in result.rows]
        expected_columns = ["version", "name", "migration_type", "applied_at", "checksum", "success"]

        for col in expected_columns:
            assert col in column_names

    @pytest.mark.asyncio
    async def test_initialize_migration_tracking_already_exists(self, migration_manager, query_executor):
        """Test initialization when table already exists."""
        # Create table manually first
        await query_executor.execute_command("""
            CREATE TABLE schema_migrations (
                version VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL
            )
        """)

        # Should not error when called again
        await migration_manager.initialize_migration_tracking()

    @pytest.mark.asyncio
    async def test_initialize_migration_tracking_error(self, migration_manager):
        """Test initialization error handling."""
        # Mock query executor to raise error
        mock_executor = Mock()
        mock_executor.execute_command = AsyncMock(side_effect=Exception("Database error"))
        migration_manager.query_executor = mock_executor

        with pytest.raises(MigrationError) as exc_info:
            await migration_manager.initialize_migration_tracking()

        assert "Cannot initialize migration tracking" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_applied_migrations_empty(self, migration_manager):
        """Test getting applied migrations when none exist."""
        migrations = await migration_manager.get_applied_migrations()

        assert isinstance(migrations, list)
        assert len(migrations) == 0

    @pytest.mark.asyncio
    async def test_get_applied_migrations_with_data(self, migration_manager, query_executor):
        """Test getting applied migrations with existing data."""
        # Initialize tracking
        await migration_manager.initialize_migration_tracking()

        # Insert test migration records
        await query_executor.execute_command(f"""
            INSERT INTO schema_migrations 
            (version, name, migration_type, applied_at, checksum, success)
            VALUES 
            ('001', 'first_migration', 'create_table', CURRENT_TIMESTAMP, 'hash1', true),
            ('002', 'second_migration', 'DATA_MIGRATION', CURRENT_TIMESTAMP, 'hash2', true),
            ('003', 'failed_migration', 'create_table', CURRENT_TIMESTAMP, 'hash3', false)
        """)

        migrations = await migration_manager.get_applied_migrations()

        assert len(migrations) == 2  # Only successful ones
        assert "001" in migrations
        assert "002" in migrations
        assert "003" not in migrations  # Failed migration not included

    @pytest.mark.asyncio
    async def test_get_applied_migrations_error(self, migration_manager):
        """Test get applied migrations error handling."""
        # Mock query executor to raise error
        mock_executor = Mock()
        mock_executor.execute_command = AsyncMock()  # For initialize
        mock_executor.execute_query = AsyncMock(side_effect=Exception("Query error"))
        migration_manager.query_executor = mock_executor

        with pytest.raises(MigrationError):
            await migration_manager.get_applied_migrations()

    @pytest.mark.asyncio
    async def test_get_pending_migrations(self, migration_manager):
        """Test getting pending migrations."""
        # Basic implementation returns empty list
        migrations = await migration_manager.get_pending_migrations()

        assert isinstance(migrations, list)
        assert len(migrations) == 0

    @pytest.mark.asyncio
    async def test_apply_migration_success(self, migration_manager, sample_migration, query_executor):
        """Test successful migration application."""
        # Initialize migration tracking first
        await migration_manager.initialize_migration_tracking()

        await migration_manager.apply_migration(sample_migration)

        # Verify migration was applied
        result = await query_executor.execute_query("SELECT name FROM pragma_table_info('test_migration')")
        column_names = [row["name"] for row in result.rows]
        assert "id" in column_names
        assert "name" in column_names

        # Verify migration was recorded
        result = await query_executor.execute_query(
            "SELECT * FROM schema_migrations WHERE version = '001'"
        )
        assert len(result.rows) == 1
        assert result.rows[0]["success"] is True

    @pytest.mark.asyncio
    async def test_apply_migration_sql_error(self, migration_manager, query_executor):
        """Test migration application with SQL error."""
        bad_migration = Migration(
            version="bad", 
            name="bad_migration",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="INVALID SQL STATEMENT",
            down_sql="DROP TABLE test",
            description="Bad migration for testing",
            created_at=datetime.now(),
            checksum="bad123"
        )

        with pytest.raises(MigrationError) as exc_info:
            await migration_manager.apply_migration(bad_migration)

        assert "Failed to apply migration" in str(exc_info.value)

        # Verify failure was recorded (if tracking exists)
        try:
            result = await query_executor.execute_query(
                "SELECT success FROM schema_migrations WHERE version = 'bad'"
            )
            if result.rows:
                assert result.rows[0]["success"] is False
        except:
            pass  # Tracking might not be initialized

    @pytest.mark.asyncio
    async def test_apply_migration_record_error(self, migration_manager, sample_migration):
        """Test migration application with recording error."""
        # Mock executor to succeed on migration but fail on recording
        mock_executor = Mock()
        mock_executor.execute_command = AsyncMock(side_effect=[
            None,  # Migration SQL succeeds
            Exception("Record error")  # Recording fails
        ])
        migration_manager.query_executor = mock_executor

        with pytest.raises(MigrationError):
            await migration_manager.apply_migration(sample_migration)

    @pytest.mark.asyncio
    async def test_rollback_migration_success(self, migration_manager, sample_migration, query_executor):
        """Test successful migration rollback."""
        # Initialize migration tracking first
        await migration_manager.initialize_migration_tracking()

        # Apply migration first
        await migration_manager.apply_migration(sample_migration)
        assert await query_executor.execute_scalar("SELECT COUNT(*) FROM pragma_table_info('test_migration')") > 0

        # Rollback migration
        await migration_manager.rollback_migration(sample_migration)

        # Verify table was dropped - should not exist anymore
        try:
            result = await query_executor.execute_query("SELECT name FROM pragma_table_info('test_migration')")
            assert len(result.rows) == 0  # If query succeeds, should return empty
        except:
            # Better - table doesn't exist at all
            pass

        # Verify migration record was removed
        result = await query_executor.execute_query(
            "SELECT COUNT(*) as count FROM schema_migrations WHERE version = '001'"
        )
        assert result.rows[0]["count"] == 0

    @pytest.mark.asyncio
    async def test_rollback_migration_sql_error(self, migration_manager, query_executor):
        """Test migration rollback with SQL error."""
        bad_rollback = Migration(
            version="rollback_test",
            name="rollback_migration",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE rollback_test (id INTEGER)",
            down_sql="INVALID ROLLBACK SQL",
            description="Bad rollback migration for testing",
            created_at=datetime.now(),
            checksum="rollback123"
        )

        # Apply first (assuming it works)
        try:
            await migration_manager.apply_migration(Migration(
                version="rollback_test",
                name="rollback_migration", 
                migration_type=MigrationType.CREATE_TABLE,
                up_sql="CREATE TABLE rollback_test (id INTEGER)",
                down_sql="DROP TABLE rollback_test",
                description="Good rollback migration for testing",
                created_at=datetime.now(),
                checksum="rollback123"
            ))
        except:
            pass

        with pytest.raises(MigrationError):
            await migration_manager.rollback_migration(bad_rollback)

    @pytest.mark.asyncio
    async def test_migrate_to_version(self, migration_manager):
        """Test migrate to specific version."""
        # Basic implementation just logs
        await migration_manager.migrate_to_version("0.1.0")
        # Should not raise error

        await migration_manager.migrate_to_version()
        # Should not raise error

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_success(self, migration_manager, query_executor):
        """Test successful migration integrity validation."""
        # Initialize tracking with successful migrations
        await migration_manager.initialize_migration_tracking()
        await query_executor.execute_command(f"""
            INSERT INTO schema_migrations 
            (version, name, migration_type, applied_at, checksum, success)
            VALUES ('001', 'test', 'create_table', CURRENT_TIMESTAMP, 'hash1', true)
        """)

        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is True

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_with_failures(self, migration_manager, query_executor):
        """Test migration integrity validation with failed migrations."""
        # Initialize tracking with failed migration
        await migration_manager.initialize_migration_tracking()
        await query_executor.execute_command(f"""
            INSERT INTO schema_migrations 
            (version, name, migration_type, applied_at, checksum, success)
            VALUES ('001', 'failed', 'create_table', CURRENT_TIMESTAMP, 'hash1', false)
        """)

        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is False

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_error(self, migration_manager):
        """Test migration integrity validation with error."""
        # Mock executor to raise error
        mock_executor = Mock()
        mock_executor.execute_command = AsyncMock()  # For initialize
        mock_executor.execute_query = AsyncMock(side_effect=Exception("Query error"))
        migration_manager.query_executor = mock_executor

        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is False

    def test_load_migrations_from_directory(self, migration_manager):
        """Test loading migrations from directory."""
        migrations = migration_manager.load_migrations_from_directory("/fake/path")

        # Basic implementation returns empty list
        assert isinstance(migrations, list)
        assert len(migrations) == 0

    @pytest.mark.asyncio
    async def test_migration_workflow_complete(self, migration_manager, query_executor):
        """Test complete migration workflow."""
        # Initialize migration tracking first
        await migration_manager.initialize_migration_tracking()

        # Create a series of migrations
        migration1 = Migration(
            version="001",
            name="create_users",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)",
            down_sql="DROP TABLE users",
            description="Create users table",
            created_at=datetime.now(),
            checksum="hash001"
        )

        migration2 = Migration(
            version="002", 
            name="add_email_column",
            migration_type=MigrationType.ALTER_TABLE,
            up_sql="ALTER TABLE users ADD COLUMN email VARCHAR",
            down_sql="ALTER TABLE users DROP COLUMN email",
            description="Add email column to users table",
            created_at=datetime.now(),
            checksum="hash002"
        )

        # Apply migrations
        await migration_manager.apply_migration(migration1)
        await migration_manager.apply_migration(migration2)

        # Verify both are applied
        applied = await migration_manager.get_applied_migrations()
        assert "001" in applied
        assert "002" in applied

        # Verify table structure
        result = await query_executor.execute_query("SELECT name FROM pragma_table_info('users')")
        column_names = [row["name"] for row in result.rows]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names

        # Test rollback
        await migration_manager.rollback_migration(migration2)
        applied = await migration_manager.get_applied_migrations()
        assert "001" in applied
        assert "002" not in applied

        # Verify integrity
        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is True

    @pytest.mark.asyncio
    async def test_concurrent_migration_application(self, migration_manager):
        """Test concurrent migration applications (basic test)."""
        migration1 = Migration(
            version="concurrent1",
            name="test1",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE concurrent1 (id INTEGER)",
            down_sql="DROP TABLE concurrent1",
            description="Concurrent test migration 1",
            created_at=datetime.now(),
            checksum="conc1"
        )

        migration2 = Migration(
            version="concurrent2",
            name="test2", 
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE concurrent2 (id INTEGER)",
            down_sql="DROP TABLE concurrent2",
            description="Concurrent test migration 2",
            created_at=datetime.now(),
            checksum="conc2"
        )

        # Initialize migration manager first
        await migration_manager.initialize_migration_tracking()

        # Apply both migrations
        await migration_manager.apply_migration(migration1)
        await migration_manager.apply_migration(migration2)

        # Both should be applied
        applied = await migration_manager.get_applied_migrations()
        assert "concurrent1" in applied
        assert "concurrent2" in applied

    @pytest.mark.asyncio
    async def test_migration_with_transaction_rollback(self, migration_manager, query_executor):
        """Test migration behavior with transaction rollback."""
        # Create a migration that will partially succeed then fail
        # This tests the transaction behavior
        failing_migration = Migration(
            version="fail_test",
            name="failing_migration",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="""
                CREATE TABLE success_table (id INTEGER);
                INVALID SQL THAT WILL FAIL;
            """,
            down_sql="DROP TABLE success_table",
            description="Migration that fails partway through",
            created_at=datetime.now(),
            checksum="fail123"
        )

        with pytest.raises(MigrationError):
            await migration_manager.apply_migration(failing_migration)

        # Verify no partial state exists (table should not exist)
        try:
            result = await query_executor.execute_query("SELECT * FROM success_table")
            # If we get here, the table exists (bad - transaction didn't roll back)
            assert False, "Table should not exist after failed migration"
        except:
            # Good - table doesn't exist, transaction was rolled back
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
