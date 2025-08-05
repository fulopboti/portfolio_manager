"""Comprehensive tests for DuckDB connection and transaction management."""

import asyncio
import os
import tempfile
import pytest
import pytest_asyncio
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

from stockapp.infrastructure.duckdb.connection import DuckDBConnection, DuckDBTransactionManager
from stockapp.infrastructure.data_access.exceptions import ConnectionError, TransactionError


class TestDuckDBConnection:
    """Test cases for DuckDBConnection."""

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
    async def connection(self, temp_db_path):
        """Create a DuckDB connection instance."""
        conn = DuckDBConnection(temp_db_path)
        yield conn
        # Cleanup
        try:
            await conn.disconnect()
        except:
            pass

    @pytest_asyncio.fixture
    async def connected_connection(self, connection):
        """Create a connected DuckDB connection instance."""
        await connection.connect()
        yield connection

    def test_initialization(self, temp_db_path):
        """Test connection initialization."""
        # Test default initialization
        conn = DuckDBConnection(temp_db_path)
        assert conn.database_path == temp_db_path
        assert conn.config.read_only is False
        assert conn._connection is None
        assert conn._is_connected is False

        # Test read-only initialization
        from stockapp.infrastructure.duckdb.config import DuckDBConfig
        ro_config = DuckDBConfig(read_only=True)
        conn_ro = DuckDBConnection(temp_db_path, ro_config)
        assert conn_ro.config.read_only is True

    @pytest.mark.asyncio
    async def test_connect_success(self, connection, temp_db_path):
        """Test successful connection establishment."""
        # Ensure parent directory exists
        parent_dir = Path(temp_db_path).parent
        assert parent_dir.exists()

        await connection.connect()
        
        assert connection._is_connected is True
        assert connection._connection is not None
        assert await connection.is_connected() is True

    @pytest.mark.asyncio
    async def test_connect_creates_directory(self):
        """Test that connect creates database directory if it doesn't exist."""
        temp_dir = tempfile.mkdtemp()
        import shutil
        shutil.rmtree(temp_dir)  # Remove the directory
        
        db_path = os.path.join(temp_dir, "subdir", "test.db")
        connection = DuckDBConnection(db_path)
        
        try:
            await connection.connect()
            assert os.path.exists(os.path.dirname(db_path))
            assert await connection.is_connected() is True
        finally:
            await connection.disconnect()
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        """Test connection failure handling."""
        # Use invalid path to trigger connection error
        invalid_path = "/invalid\x00path/test.db"  # null byte causes error
        connection = DuckDBConnection(invalid_path)
        
        with pytest.raises(ConnectionError) as exc_info:
            await connection.connect()
        
        assert "Failed to connect to DuckDB" in str(exc_info.value)
        assert connection._is_connected is False

    @pytest.mark.asyncio
    async def test_disconnect(self, connected_connection):
        """Test connection disconnection."""
        assert await connected_connection.is_connected() is True
        
        await connected_connection.disconnect()
        
        assert connected_connection._is_connected is False
        assert connected_connection._connection is None
        assert await connected_connection.is_connected() is False

    @pytest.mark.asyncio
    async def test_disconnect_not_connected(self, connection):
        """Test disconnecting when not connected."""
        assert await connection.is_connected() is False
        
        # Should not raise error
        await connection.disconnect()
        
        assert connection._is_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_with_error(self, connected_connection):
        """Test disconnect with connection close error."""
        # Mock the connection to raise error on close
        mock_conn = Mock()
        mock_conn.close.side_effect = Exception("Close error")
        connected_connection._connection = mock_conn
        
        # Should handle error gracefully
        await connected_connection.disconnect()
        
        assert connected_connection._is_connected is False
        assert connected_connection._connection is None

    @pytest.mark.asyncio
    async def test_is_connected(self, connection):
        """Test connection status checking."""
        # Initially not connected
        assert await connection.is_connected() is False
        
        # After connecting
        await connection.connect()
        assert await connection.is_connected() is True
        
        # After disconnecting
        await connection.disconnect()
        assert await connection.is_connected() is False

    @pytest.mark.asyncio
    async def test_ping_success(self, connected_connection):
        """Test successful database ping."""
        result = await connected_connection.ping()
        assert result is True

    @pytest.mark.asyncio
    async def test_ping_not_connected(self, connection):
        """Test ping when not connected."""
        result = await connection.ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_ping_failure(self, connected_connection):
        """Test ping failure handling."""
        # Mock connection to raise error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query error")
        connected_connection._connection = mock_conn
        
        result = await connected_connection.ping()
        assert result is False

    @pytest.mark.asyncio
    async def test_get_connection_info_connected(self, connected_connection, temp_db_path):
        """Test getting connection info when connected."""
        info = await connected_connection.get_connection_info()
        
        assert info["status"] == "connected"
        assert info["database_path"] == temp_db_path
        assert info["read_only"] is False
        assert "duckdb_version" in info
        assert "database_size_bytes" in info
        assert info["connection_type"] == "file"

    @pytest.mark.asyncio
    async def test_get_connection_info_memory(self):
        """Test getting connection info for memory database."""
        connection = DuckDBConnection(":memory:")
        await connection.connect()
        
        try:
            info = await connection.get_connection_info()
            assert info["connection_type"] == "memory"
        finally:
            await connection.disconnect()

    @pytest.mark.asyncio
    async def test_get_connection_info_not_connected(self, connection):
        """Test getting connection info when not connected."""
        info = await connection.get_connection_info()
        assert info["status"] == "disconnected"

    @pytest.mark.asyncio
    async def test_get_connection_info_error(self, connected_connection):
        """Test connection info with query error."""
        # Mock connection to raise error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Query error")
        connected_connection._connection = mock_conn
        
        info = await connected_connection.get_connection_info()
        assert info["status"] == "error"
        assert "error" in info

    @pytest.mark.asyncio
    async def test_configure_connection(self, connection, temp_db_path):
        """Test connection configuration."""
        await connection.connect()
        
        # Verify connection is configured (no errors during setup)
        assert await connection.is_connected() is True
        assert connection._connection is not None

    @pytest.mark.asyncio
    async def test_configure_connection_error(self, connection):
        """Test configuration error handling."""
        await connection.connect()
        
        # Mock connection to cause configuration error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Config error")
        connection._connection = mock_conn
        
        # Call private method directly
        await connection._configure_connection()
        # Should not raise error, just log warning

    def test_raw_connection_property(self, connection):
        """Test raw connection property access."""
        # Initially None
        assert connection.raw_connection is None
        
        # Mock connection
        mock_conn = Mock()
        connection._connection = mock_conn
        
        assert connection.raw_connection is mock_conn


class TestDuckDBTransactionManager:
    """Test cases for DuckDBTransactionManager."""

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
    async def connection(self, temp_db_path):
        """Create a connected DuckDB connection."""
        conn = DuckDBConnection(temp_db_path)
        await conn.connect()
        yield conn
        await conn.disconnect()

    @pytest.fixture
    def transaction_manager(self, connection):
        """Create a transaction manager."""
        return DuckDBTransactionManager(connection)

    def test_initialization(self, connection):
        """Test transaction manager initialization."""
        tm = DuckDBTransactionManager(connection)
        assert tm.connection is connection
        assert tm._transaction_depth == 0
        assert tm._savepoint_counter == 0

    @pytest.mark.asyncio
    async def test_transaction_success(self, transaction_manager):
        """Test successful transaction."""
        assert transaction_manager._transaction_depth == 0
        
        async with transaction_manager.transaction():
            assert transaction_manager._transaction_depth == 1
            # Do some work within transaction
            pass
        
        assert transaction_manager._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, transaction_manager):
        """Test transaction rollback on exception."""
        assert transaction_manager._transaction_depth == 0
        
        with pytest.raises(ValueError):
            async with transaction_manager.transaction():
                assert transaction_manager._transaction_depth == 1
                raise ValueError("Test error")
        
        assert transaction_manager._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_nested_transactions(self, transaction_manager):
        """Test nested transactions with savepoints."""
        async with transaction_manager.transaction():
            assert transaction_manager._transaction_depth == 1
            
            async with transaction_manager.transaction():
                # Nested transaction should use savepoint
                assert transaction_manager._transaction_depth == 1
                assert transaction_manager._savepoint_counter > 0

    @pytest.mark.asyncio
    async def test_transaction_not_connected(self, temp_db_path):
        """Test transaction when not connected."""
        connection = DuckDBConnection(temp_db_path)  # Not connected
        tm = DuckDBTransactionManager(connection)
        
        with pytest.raises(TransactionError) as exc_info:
            async with tm.transaction():
                pass
        
        assert "Database connection is not active" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_savepoint_success(self, transaction_manager):
        """Test successful savepoint creation and release."""
        # Start a transaction first
        await transaction_manager.begin_transaction()
        
        try:
            async with transaction_manager.savepoint("test_sp"):
                # Do some work within savepoint
                pass
        finally:
            await transaction_manager.rollback_transaction()

    @pytest.mark.asyncio
    async def test_savepoint_rollback(self, transaction_manager):
        """Test savepoint rollback on exception."""
        await transaction_manager.begin_transaction()
        
        try:
            with pytest.raises(ValueError):
                async with transaction_manager.savepoint("test_sp"):
                    raise ValueError("Test error")
        finally:
            await transaction_manager.rollback_transaction()

    @pytest.mark.asyncio
    async def test_savepoint_not_in_transaction(self, transaction_manager):
        """Test savepoint creation outside transaction."""
        with pytest.raises(TransactionError) as exc_info:
            async with transaction_manager.savepoint("test_sp"):
                pass
        
        assert "Cannot create savepoint outside of transaction" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_savepoint_rollback_error(self, transaction_manager):
        """Test savepoint rollback error handling."""
        await transaction_manager.begin_transaction()
        
        # Mock connection to cause rollback error
        mock_conn = Mock()
        mock_conn.execute.side_effect = [None, Exception("Rollback error")]
        transaction_manager.connection._connection = mock_conn
        
        try:
            with pytest.raises(TransactionError):
                async with transaction_manager.savepoint("test_sp"):
                    raise ValueError("Test error")
        finally:
            # Reset connection and cleanup
            await transaction_manager.connection.disconnect()

    @pytest.mark.asyncio
    async def test_begin_transaction(self, transaction_manager):
        """Test explicit transaction begin."""
        assert await transaction_manager.is_in_transaction() is False
        
        await transaction_manager.begin_transaction()
        assert await transaction_manager.is_in_transaction() is True
        assert transaction_manager._transaction_depth == 1
        
        await transaction_manager.rollback_transaction()

    @pytest.mark.asyncio
    async def test_begin_transaction_not_connected(self, temp_db_path):
        """Test begin transaction when not connected."""
        connection = DuckDBConnection(temp_db_path)
        tm = DuckDBTransactionManager(connection)
        
        with pytest.raises(TransactionError):
            await tm.begin_transaction()

    @pytest.mark.asyncio
    async def test_begin_transaction_error(self, transaction_manager):
        """Test begin transaction with error."""
        # Mock connection to cause error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Begin error")
        transaction_manager.connection._connection = mock_conn
        
        with pytest.raises(TransactionError):
            await transaction_manager.begin_transaction()

    @pytest.mark.asyncio
    async def test_commit_transaction(self, transaction_manager):
        """Test transaction commit."""
        await transaction_manager.begin_transaction()
        assert await transaction_manager.is_in_transaction() is True
        
        await transaction_manager.commit_transaction()
        assert await transaction_manager.is_in_transaction() is False

    @pytest.mark.asyncio
    async def test_commit_transaction_not_active(self, transaction_manager):
        """Test commit when no active transaction."""
        with pytest.raises(TransactionError):
            await transaction_manager.commit_transaction()

    @pytest.mark.asyncio
    async def test_commit_transaction_error(self, transaction_manager):
        """Test commit transaction with error."""
        await transaction_manager.begin_transaction()
        
        # Mock connection to cause commit error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Commit error")
        transaction_manager.connection._connection = mock_conn
        
        with pytest.raises(TransactionError):
            await transaction_manager.commit_transaction()

    @pytest.mark.asyncio
    async def test_rollback_transaction(self, transaction_manager):
        """Test transaction rollback."""
        await transaction_manager.begin_transaction()
        assert await transaction_manager.is_in_transaction() is True
        
        await transaction_manager.rollback_transaction()
        assert await transaction_manager.is_in_transaction() is False

    @pytest.mark.asyncio
    async def test_rollback_transaction_no_connection(self, temp_db_path):
        """Test rollback when no connection."""
        connection = DuckDBConnection(temp_db_path)
        tm = DuckDBTransactionManager(connection)
        tm._transaction_depth = 1  # Simulate active transaction
        
        # Should not raise error
        await tm.rollback_transaction()
        assert tm._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_rollback_transaction_error(self, transaction_manager):
        """Test rollback with error."""
        await transaction_manager.begin_transaction()
        
        # Mock connection to cause rollback error
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Rollback error")
        transaction_manager.connection._connection = mock_conn
        
        # Should not raise error, but reset depth
        await transaction_manager.rollback_transaction()
        assert transaction_manager._transaction_depth == 0

    @pytest.mark.asyncio
    async def test_is_in_transaction(self, transaction_manager):
        """Test transaction status checking."""
        assert await transaction_manager.is_in_transaction() is False
        
        await transaction_manager.begin_transaction()
        assert await transaction_manager.is_in_transaction() is True
        
        await transaction_manager.commit_transaction()
        assert await transaction_manager.is_in_transaction() is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])