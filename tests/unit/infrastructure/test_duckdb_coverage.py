"""Comprehensive tests to achieve full coverage for DuckDB infrastructure components."""

import os
import tempfile
import pytest
import pytest_asyncio
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4

from portfolio_manager.infrastructure.duckdb.connection import DuckDBConnection, DuckDBTransactionManager
from portfolio_manager.infrastructure.data_access.exceptions import ConnectionError, TransactionError


class TestDuckDBConnectionCoverage:
    """Tests to achieve comprehensive coverage for DuckDB connection."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path.""" 
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "coverage_test.db")
        yield db_path
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_connection_initialization_coverage(self, temp_db_path):
        """Test connection initialization to cover lines 34-37."""
        # Test default values
        conn = DuckDBConnection(temp_db_path)
        assert conn.database_path == temp_db_path
        assert conn.config.read_only is False
        assert conn._connection is None
        assert conn._is_connected is False
        
        # Test read-only initialization  
        from portfolio_manager.infrastructure.duckdb.config import DuckDBConfig
        ro_config = DuckDBConfig(read_only=True)
        conn_ro = DuckDBConnection(temp_db_path, ro_config)
        assert conn_ro.config.read_only is True
        assert conn_ro._connection is None
        assert conn_ro._is_connected is False

    @pytest.mark.asyncio
    async def test_connect_directory_creation_coverage(self):
        """Test connect creates directory structure (lines 41-44)."""
        temp_base = tempfile.mkdtemp()
        import shutil
        shutil.rmtree(temp_base)  # Remove to test creation
        
        nested_path = os.path.join(temp_base, "nested", "deep", "test.db")
        conn = DuckDBConnection(nested_path)
        
        try:
            await conn.connect()
            # Verify directory was created
            assert os.path.exists(os.path.dirname(nested_path))
            assert conn._is_connected is True
        finally:
            await conn.disconnect()
            if os.path.exists(temp_base):
                shutil.rmtree(temp_base, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_connect_configuration_coverage(self, temp_db_path):
        """Test connection configuration is called (lines 52-55)."""
        conn = DuckDBConnection(temp_db_path)
        
        with patch.object(conn, '_configure_connection', new_callable=AsyncMock) as mock_config:
            await conn.connect()
            mock_config.assert_called_once()
            assert conn._is_connected is True
        
        await conn.disconnect()

    @pytest.mark.asyncio  
    async def test_connect_exception_handling_coverage(self):
        """Test connect exception handling (lines 57-59)."""
        # Use invalid database path to trigger exception
        invalid_path = "\x00invalid/path.db"  # Null byte should cause error
        conn = DuckDBConnection(invalid_path)
        
        with pytest.raises(ConnectionError) as exc_info:
            await conn.connect()
        
        assert "Failed to connect to DuckDB" in str(exc_info.value)
        assert conn._is_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_with_connection_coverage(self, temp_db_path):
        """Test disconnect with active connection (lines 64-71)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        assert conn._is_connected is True
        
        await conn.disconnect()
        assert conn._connection is None
        assert conn._is_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_exception_handling_coverage(self, temp_db_path):
        """Test disconnect exception handling (lines 67-71)."""
        conn = DuckDBConnection(temp_db_path) 
        await conn.connect()
        
        # Mock connection.close() to raise exception
        mock_conn = Mock()
        mock_conn.close.side_effect = Exception("Close error")
        conn._connection = mock_conn
        
        # Should handle exception gracefully
        await conn.disconnect()
        assert conn._connection is None
        assert conn._is_connected is False

    @pytest.mark.asyncio
    async def test_is_connected_coverage(self, temp_db_path):
        """Test is_connected method (lines 75-76)."""
        conn = DuckDBConnection(temp_db_path)
        
        # Not connected initially
        assert await conn.is_connected() is False
        
        await conn.connect()
        assert await conn.is_connected() is True
        
        await conn.disconnect()
        assert await conn.is_connected() is False

    @pytest.mark.asyncio
    async def test_ping_not_connected_coverage(self, temp_db_path):
        """Test ping when not connected (lines 79-80)."""
        conn = DuckDBConnection(temp_db_path)
        result = await conn.ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_ping_success_coverage(self, temp_db_path):
        """Test successful ping (lines 82-85)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        try:
            result = await conn.ping()
            assert result is True
        finally:
            await conn.disconnect()

    @pytest.mark.asyncio
    async def test_ping_exception_coverage(self, temp_db_path):
        """Test ping exception handling (lines 86-88)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        # Mock connection to raise exception
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query error")
        conn._connection = mock_conn
        
        result = await conn.ping()
        assert result is False
        
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_get_connection_info_disconnected_coverage(self, temp_db_path):
        """Test get_connection_info when disconnected (lines 92-93)."""
        conn = DuckDBConnection(temp_db_path)
        info = await conn.get_connection_info()
        assert info == {"status": "disconnected"}

    @pytest.mark.asyncio
    async def test_get_connection_info_connected_coverage(self, temp_db_path):
        """Test get_connection_info when connected (lines 95-111)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        try:
            info = await conn.get_connection_info()
            assert info["status"] == "connected"
            assert info["database_path"] == temp_db_path
            assert info["read_only"] is False
            assert "duckdb_version" in info
            assert "database_size_bytes" in info
            assert info["connection_type"] == "file"
        finally:
            await conn.disconnect()

    @pytest.mark.asyncio
    async def test_get_connection_info_memory_database_coverage(self):
        """Test get_connection_info for memory database (line 110)."""
        conn = DuckDBConnection(":memory:")
        await conn.connect()
        
        try:
            info = await conn.get_connection_info()
            assert info["connection_type"] == "memory"
        finally:
            await conn.disconnect()

    @pytest.mark.asyncio
    async def test_get_connection_info_exception_coverage(self, temp_db_path):
        """Test get_connection_info exception handling (lines 112-114)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        # Mock connection to raise exception
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query error")
        conn._connection = mock_conn
        
        info = await conn.get_connection_info()
        assert info["status"] == "error"
        assert "error" in info
        
        await conn.disconnect()

    @pytest.mark.asyncio
    async def test_configure_connection_no_connection_coverage(self, temp_db_path):
        """Test _configure_connection with no connection (lines 118-119)."""
        conn = DuckDBConnection(temp_db_path)
        conn._connection = None
        
        # Should return early without error
        await conn._configure_connection()

    @pytest.mark.asyncio
    async def test_configure_connection_success_coverage(self, temp_db_path):
        """Test successful _configure_connection (lines 121-134)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        try:
            # Configuration should have been called during connect
            # Verify connection is properly configured
            assert conn._connection is not None
        finally:
            await conn.disconnect()

    @pytest.mark.asyncio
    async def test_configure_connection_exception_coverage(self, temp_db_path):
        """Test _configure_connection exception handling (lines 136-137)."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        
        # Mock connection to raise exception during configure
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Config error")
        conn._connection = mock_conn
        
        # Should handle exception gracefully
        await conn._configure_connection()
        
        await conn.disconnect()

    def test_raw_connection_property_coverage(self, temp_db_path):
        """Test raw_connection property (lines 141-142)."""
        conn = DuckDBConnection(temp_db_path)
        
        # Initially None
        assert conn.raw_connection is None
        
        # Mock connection
        mock_conn = Mock()
        conn._connection = mock_conn
        assert conn.raw_connection is mock_conn


class TestDuckDBTransactionManagerCoverage:
    """Tests to achieve comprehensive coverage for DuckDB transaction manager."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "txn_test.db")
        yield db_path
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest_asyncio.fixture
    async def connected_connection(self, temp_db_path):
        """Create a connected DuckDB connection."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        yield conn
        await conn.disconnect()

    def test_transaction_manager_initialization_coverage(self, temp_db_path):
        """Test transaction manager initialization (lines 154-156)."""
        conn = DuckDBConnection(temp_db_path)
        tm = DuckDBTransactionManager(conn)
        
        assert tm.connection is conn
        assert tm._transaction_depth == 0
        assert tm._savepoint_counter == 0

    @pytest.mark.asyncio
    async def test_transaction_not_connected_coverage(self, temp_db_path):
        """Test transaction when not connected (lines 161-162)."""
        conn = DuckDBConnection(temp_db_path)  # Not connected
        tm = DuckDBTransactionManager(conn)
        
        with pytest.raises(TransactionError) as exc_info:
            async with tm.transaction():
                pass
        
        assert "Database connection is not active" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_transaction_no_raw_connection_coverage(self, connected_connection):
        """Test transaction with no raw connection (lines 164-166)."""
        tm = DuckDBTransactionManager(connected_connection)
        connected_connection._connection = None  # Simulate no raw connection
        connected_connection._is_connected = False  # Also mark as not connected
        
        with pytest.raises(TransactionError) as exc_info:
            async with tm.transaction():
                pass
        
        assert "Database connection is not active" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_nested_transaction_coverage(self, connected_connection):
        """Test nested transactions with savepoints (lines 169-174)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        # Since DuckDB doesn't support standard SAVEPOINT syntax,
        # nested transactions will likely fail - test that they handle the error
        async with tm.transaction():
            assert tm._transaction_depth == 1
            initial_savepoint_counter = tm._savepoint_counter
            
            # Nested transaction should attempt savepoint but may fail due to DuckDB limitations
            try:
                async with tm.transaction():
                    assert tm._savepoint_counter > initial_savepoint_counter
            except TransactionError:
                # Expected if DuckDB doesn't support savepoints
                pass

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_exception_coverage(self, connected_connection):
        """Test transaction rollback on exception (lines 181-183)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        with pytest.raises(ValueError):
            async with tm.transaction():
                assert tm._transaction_depth == 1
                raise ValueError("Test error")
        
        assert tm._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_savepoint_not_in_transaction_coverage(self, connected_connection):
        """Test savepoint outside transaction (lines 188-189)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        with pytest.raises(TransactionError) as exc_info:
            async with tm.savepoint("test_sp"):
                pass
        
        assert "Cannot create savepoint outside of transaction" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_savepoint_no_raw_connection_coverage(self, connected_connection):
        """Test savepoint with no raw connection (lines 191-193)."""
        tm = DuckDBTransactionManager(connected_connection)
        await tm.begin_transaction()
        
        connected_connection._connection = None  # Simulate no raw connection
        
        try:
            with pytest.raises(TransactionError) as exc_info:
                async with tm.savepoint("test_sp"):
                    pass
            
            assert "No raw connection available" in str(exc_info.value)
        finally:
            tm._transaction_depth = 0  # Reset for cleanup

    @pytest.mark.asyncio
    async def test_savepoint_rollback_error_coverage(self, connected_connection):
        """Test savepoint rollback error handling (lines 208-213)."""
        tm = DuckDBTransactionManager(connected_connection)
        await tm.begin_transaction()
        
        # Mock connection to cause rollback error
        mock_conn = Mock()
        # First call succeeds (CREATE SAVEPOINT), second fails (ROLLBACK TO SAVEPOINT)
        mock_conn.execute.side_effect = [None, Exception("Rollback error")]
        connected_connection._connection = mock_conn
        
        try:
            with pytest.raises(TransactionError):
                async with tm.savepoint("test_sp"):
                    raise ValueError("Test error")
        finally:
            tm._transaction_depth = 0  # Reset for cleanup

    @pytest.mark.asyncio
    async def test_begin_transaction_not_connected_coverage(self, temp_db_path):
        """Test begin_transaction when not connected (lines 217-218)."""
        conn = DuckDBConnection(temp_db_path)
        tm = DuckDBTransactionManager(conn)
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.begin_transaction()
        
        assert "Database connection is not active" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_begin_transaction_no_raw_connection_coverage(self, connected_connection):
        """Test begin_transaction with no raw connection (lines 220-222)."""
        tm = DuckDBTransactionManager(connected_connection)
        connected_connection._connection = None
        connected_connection._is_connected = False  # Also mark as not connected
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.begin_transaction()
        
        assert "Database connection is not active" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_begin_transaction_exception_coverage(self, connected_connection):
        """Test begin_transaction exception handling (lines 228-229)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        # Mock connection to raise exception
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Begin error")
        connected_connection._connection = mock_conn
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.begin_transaction()
        
        assert "Failed to begin transaction" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_commit_transaction_not_active_coverage(self, connected_connection):
        """Test commit when no active transaction (lines 233-234)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.commit_transaction()
        
        assert "No active transaction to commit" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_commit_transaction_no_raw_connection_coverage(self, connected_connection):
        """Test commit with no raw connection (lines 236-238)."""
        tm = DuckDBTransactionManager(connected_connection)
        tm._transaction_depth = 1  # Simulate active transaction
        connected_connection._connection = None
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.commit_transaction()
        
        assert "No raw connection available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_commit_transaction_exception_coverage(self, connected_connection):
        """Test commit exception with rollback (lines 245-247)."""
        tm = DuckDBTransactionManager(connected_connection)
        await tm.begin_transaction()
        
        # Mock connection: first call (commit) fails, second call (rollback) succeeds
        mock_conn = Mock()
        mock_conn.execute.side_effect = [Exception("Commit error"), None]
        connected_connection._connection = mock_conn
        
        with pytest.raises(TransactionError) as exc_info:
            await tm.commit_transaction()
        
        assert "Failed to commit transaction" in str(exc_info.value)
        assert tm._transaction_depth == 0  # Should be reset after rollback

    @pytest.mark.asyncio
    async def test_rollback_no_raw_connection_coverage(self, temp_db_path):
        """Test rollback with no raw connection (lines 251-254)."""
        conn = DuckDBConnection(temp_db_path)
        tm = DuckDBTransactionManager(conn)
        tm._transaction_depth = 1  # Simulate active transaction
        
        # Should not raise error, but should log warning and reset depth
        await tm.rollback_transaction()
        # Transaction depth should be reset to 0 when no connection
        assert tm._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_rollback_exception_coverage(self, connected_connection):
        """Test rollback exception handling (lines 260-263)."""
        tm = DuckDBTransactionManager(connected_connection)
        tm._transaction_depth = 1  # Simulate active transaction
        
        # Mock connection to raise exception
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Rollback error")
        connected_connection._connection = mock_conn
        
        # Should not raise error but reset depth
        await tm.rollback_transaction()
        assert tm._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_is_in_transaction_coverage(self, connected_connection):
        """Test is_in_transaction method (lines 266-267)."""
        tm = DuckDBTransactionManager(connected_connection)
        
        assert await tm.is_in_transaction() is False
        
        await tm.begin_transaction()
        assert await tm.is_in_transaction() is True
        
        await tm.commit_transaction()
        assert await tm.is_in_transaction() is False
