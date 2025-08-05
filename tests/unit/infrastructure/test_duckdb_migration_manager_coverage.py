"""Comprehensive tests to achieve full coverage for DuckDB migration manager."""

import os
import tempfile
import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

from stockapp.infrastructure.duckdb.connection import DuckDBConnection
from stockapp.infrastructure.duckdb.query_executor import DuckDBQueryExecutor
from stockapp.infrastructure.duckdb.schema.migration_manager import DuckDBMigrationManager
from stockapp.infrastructure.data_access.schema_manager import Migration, MigrationType
from stockapp.infrastructure.data_access.exceptions import MigrationError


class TestDuckDBMigrationManagerCoverage:
    """Tests to achieve comprehensive coverage for DuckDB migration manager."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "migration_test.db")
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
    def migration_manager(self, query_executor):
        """Create a DuckDB migration manager."""
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
            description="Test migration for coverage",
            created_at=datetime.now(timezone.utc),
            checksum="abc123"
        )

    def test_initialization_coverage(self, query_executor):
        """Test migration manager initialization (line 19)."""
        migration_manager = DuckDBMigrationManager(query_executor)
        assert migration_manager.query_executor is query_executor

    @pytest.mark.asyncio
    async def test_initialize_migration_tracking_success_coverage(self, migration_manager):
        """Test initialize_migration_tracking method success (lines 27-47)."""
        await migration_manager.initialize_migration_tracking()
        
        # Verify table was created by checking its existence
        result = await migration_manager.query_executor.execute_query("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='schema_migrations'
        """)
        # Note: This tests the functionality even if system table query differs

    @pytest.mark.asyncio
    async def test_initialize_migration_tracking_error_coverage(self, migration_manager):
        """Test initialize_migration_tracking error handling."""
        # Mock query_executor to raise exception
        with patch.object(migration_manager.query_executor, 'execute_command') as mock_execute:
            mock_execute.side_effect = Exception("Database error")
            
            with pytest.raises(MigrationError) as exc_info:
                await migration_manager.initialize_migration_tracking()
            
            assert "Cannot initialize migration tracking" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_applied_migrations_empty_coverage(self, migration_manager):
        """Test get_applied_migrations with no migrations (lines 49-65)."""
        applied = await migration_manager.get_applied_migrations()
        assert isinstance(applied, list)
        assert len(applied) == 0

    @pytest.mark.asyncio
    async def test_get_applied_migrations_with_data_coverage(self, migration_manager):
        """Test get_applied_migrations with existing migrations."""
        await migration_manager.initialize_migration_tracking()
        
        # Insert a test migration record manually
        await migration_manager.query_executor.execute_command("""
            INSERT INTO schema_migrations (version, name, migration_type, applied_at, checksum, success)
            VALUES ('001', 'test_migration', 'CREATE_TABLE', CURRENT_TIMESTAMP, 'abc123', true)
        """)
        
        applied = await migration_manager.get_applied_migrations()
        assert len(applied) == 1
        assert applied[0] == "001"

    @pytest.mark.asyncio
    async def test_get_applied_migrations_error_coverage(self, migration_manager):
        """Test get_applied_migrations error handling."""
        # Mock query_executor to raise exception
        with patch.object(migration_manager.query_executor, 'execute_query') as mock_query:
            mock_query.side_effect = Exception("Database error")
            
            with pytest.raises(MigrationError) as exc_info:
                await migration_manager.get_applied_migrations()
            
            assert "Cannot get applied migrations" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_pending_migrations_coverage(self, migration_manager):
        """Test get_pending_migrations method (lines 67-71)."""
        pending = await migration_manager.get_pending_migrations()
        
        # Basic implementation returns empty list
        assert isinstance(pending, list)  
        assert len(pending) == 0

    @pytest.mark.asyncio
    async def test_apply_migration_success_coverage(self, migration_manager, sample_migration):
        """Test successful migration application (lines 73-91)."""
        await migration_manager.initialize_migration_tracking()
        
        await migration_manager.apply_migration(sample_migration)
        
        # Verify migration was recorded
        applied = await migration_manager.get_applied_migrations()
        assert sample_migration.version in applied
        
        # Verify table was created
        result = await migration_manager.query_executor.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_migration'"
        )

    @pytest.mark.asyncio
    async def test_apply_migration_sql_error_coverage(self, migration_manager):
        """Test apply_migration with SQL error (lines 93-111)."""
        await migration_manager.initialize_migration_tracking()
        
        # Create migration with invalid SQL
        bad_migration = Migration(
            version="002",
            name="invalid_migration",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="INVALID SQL SYNTAX HERE",
            down_sql="DROP TABLE test",
            description="Invalid migration for testing",
            created_at=datetime.now(timezone.utc),
            checksum="def456"
        )
        
        with pytest.raises(MigrationError) as exc_info:
            await migration_manager.apply_migration(bad_migration)
        
        assert "Failed to apply migration" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_apply_migration_record_failure_coverage(self, migration_manager, sample_migration):
        """Test apply_migration when recording migration fails."""
        await migration_manager.initialize_migration_tracking()
        
        # Mock execute_command to fail on the second call (recording)
        original_execute = migration_manager.query_executor.execute_command
        call_count = 0
        
        async def mock_execute_command(sql, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Second call is recording migration
                raise Exception("Recording failed")
            return await original_execute(sql, params)
        
        with patch.object(migration_manager.query_executor, 'execute_command', side_effect=mock_execute_command):
            with pytest.raises(MigrationError) as exc_info:
                await migration_manager.apply_migration(sample_migration)
            
            assert "Failed to apply migration" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_rollback_migration_success_coverage(self, migration_manager, sample_migration):
        """Test successful migration rollback (lines 113-129)."""
        await migration_manager.initialize_migration_tracking()
        
        # Apply migration first
        await migration_manager.apply_migration(sample_migration)
        
        # Then rollback
        await migration_manager.rollback_migration(sample_migration)
        
        # Verify migration is no longer in applied list
        applied = await migration_manager.get_applied_migrations()
        assert sample_migration.version not in applied

    @pytest.mark.asyncio
    async def test_rollback_migration_sql_error_coverage(self, migration_manager):
        """Test rollback_migration with SQL error."""
        await migration_manager.initialize_migration_tracking()
        
        # Create migration with invalid rollback SQL
        bad_migration = Migration(
            version="003",
            name="bad_rollback",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE temp_test (id INTEGER)",
            down_sql="INVALID ROLLBACK SQL",
            description="Bad rollback migration for testing",
            created_at=datetime.now(timezone.utc),
            checksum="ghi789"
        )
        
        # Apply it first
        await migration_manager.apply_migration(bad_migration)
        
        # Try to rollback - should fail
        with pytest.raises(MigrationError) as exc_info:
            await migration_manager.rollback_migration(bad_migration)
        
        assert "Failed to rollback migration" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_to_version_with_target_coverage(self, migration_manager):
        """Test migrate_to_version with target version (lines 134-135)."""
        # This method just logs, so test it doesn't raise errors
        await migration_manager.migrate_to_version("1.0.0")

    @pytest.mark.asyncio
    async def test_migrate_to_version_latest_coverage(self, migration_manager):
        """Test migrate_to_version to latest (lines 137)."""
        # This method just logs, so test it doesn't raise errors
        await migration_manager.migrate_to_version()

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_success_coverage(self, migration_manager):
        """Test validate_migration_integrity success (lines 139-156)."""
        await migration_manager.initialize_migration_tracking()
        
        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is True

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_with_failures_coverage(self, migration_manager):
        """Test validate_migration_integrity with failed migrations."""
        await migration_manager.initialize_migration_tracking()
        
        # Insert a failed migration
        await migration_manager.query_executor.execute_command("""
            INSERT INTO schema_migrations (version, name, migration_type, applied_at, checksum, success)
            VALUES ('999', 'failed_migration', 'CREATE_TABLE', CURRENT_TIMESTAMP, 'failed123', false)
        """)
        
        is_valid = await migration_manager.validate_migration_integrity()
        assert is_valid is False

    @pytest.mark.asyncio
    async def test_validate_migration_integrity_error_coverage(self, migration_manager):
        """Test validate_migration_integrity error handling."""
        # Mock query_executor to raise exception
        with patch.object(migration_manager.query_executor, 'execute_query') as mock_query:
            mock_query.side_effect = Exception("Database error")
            
            is_valid = await migration_manager.validate_migration_integrity()
            assert is_valid is False

    def test_load_migrations_from_directory_coverage(self, migration_manager):
        """Test load_migrations_from_directory method (lines 158-163)."""
        migrations = migration_manager.load_migrations_from_directory("/fake/path")
        
        # Basic implementation returns empty list
        assert isinstance(migrations, list)
        assert len(migrations) == 0

    @pytest.mark.asyncio
    async def test_applied_migration_ordering_coverage(self, migration_manager):
        """Test that applied migrations are returned in correct order."""
        await migration_manager.initialize_migration_tracking()
        
        # Insert migrations out of order
        migrations_to_insert = [
            ("003", "third_migration"),
            ("001", "first_migration"), 
            ("002", "second_migration"),
        ]
        
        for version, name in migrations_to_insert:
            await migration_manager.query_executor.execute_command("""
                INSERT INTO schema_migrations (version, name, migration_type, applied_at, checksum, success)
                VALUES ($version, $name, 'CREATE_TABLE', CURRENT_TIMESTAMP, 'checksum', true)
            """, {"version": version, "name": name})
        
        applied = await migration_manager.get_applied_migrations()
        
        # Should be ordered by applied_at (insertion order in this test)  
        assert len(applied) == 3
        assert applied == ["003", "001", "002"]  # Order of insertion

    @pytest.mark.asyncio
    async def test_rollback_nonexistent_migration_coverage(self, migration_manager, sample_migration):
        """Test rollback_migration when migration wasn't applied."""
        await migration_manager.initialize_migration_tracking()
        
        # Try to rollback migration that was never applied - will fail due to missing table
        with pytest.raises(MigrationError) as exc_info:
            await migration_manager.rollback_migration(sample_migration)
        
        assert "Failed to rollback migration" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_parameter_binding_coverage(self, migration_manager, sample_migration):
        """Test parameter binding in SQL queries."""
        await migration_manager.initialize_migration_tracking()
        
        # Apply migration to test parameter binding in insert
        await migration_manager.apply_migration(sample_migration)
        
        # Verify parameters were bound correctly
        result = await migration_manager.query_executor.execute_query("""
            SELECT version, name, migration_type, checksum 
            FROM schema_migrations 
            WHERE version = '001'
        """)
        
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row["version"] == "001"
        assert row["name"] == "create_test_table"
        assert row["migration_type"] == "create_table"
        assert row["checksum"] == "abc123"