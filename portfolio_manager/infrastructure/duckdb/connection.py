"""DuckDB database connection and transaction management implementations."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncContextManager, Dict, Optional

import duckdb
from duckdb import DuckDBPyConnection

from portfolio_manager.infrastructure.data_access.connection import (
    DatabaseConnection,
    TransactionManager,
)
from portfolio_manager.infrastructure.data_access.exceptions import (
    ConnectionError,
    TransactionError,
)
from .config import DuckDBConfig

logger = logging.getLogger(__name__)


class DuckDBConnection(DatabaseConnection):
    """DuckDB implementation of database connection management."""

    def __init__(self, database_path: str, config: Optional[DuckDBConfig] = None):
        """Initialize DuckDB connection.

        Args:
            database_path: Path to DuckDB database file
            config: DuckDB configuration settings (uses defaults if None)
        """
        self.database_path = database_path
        self.config = config or DuckDBConfig.from_environment()
        self._connection: Optional[DuckDBPyConnection] = None
        self._is_connected = False

    async def connect(self) -> None:
        """Establish connection to the DuckDB database."""
        try:
            # Ensure database directory exists
            db_path = Path(self.database_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create DuckDB connection
            self._connection = duckdb.connect(
                database=self.database_path,
                read_only=self.config.read_only
            )

            # Configure connection settings
            await self._configure_connection()

            self._is_connected = True
            logger.info(f"Connected to DuckDB database: {self.database_path}")

        except Exception as e:
            self._is_connected = False
            raise ConnectionError(f"Failed to connect to DuckDB: {str(e)}") from e

    async def disconnect(self) -> None:
        """Close the DuckDB connection and clean up resources."""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Disconnected from DuckDB database")
            except Exception as e:
                logger.warning(f"Error during disconnect: {str(e)}")
            finally:
                self._connection = None
                self._is_connected = False

    async def is_connected(self) -> bool:
        """Check if database connection is active and healthy."""
        return self._is_connected and self._connection is not None

    async def ping(self) -> bool:
        """Ping the database to verify connectivity."""
        if not await self.is_connected():
            return False

        try:
            # Execute a simple query to test connectivity
            result = self._connection.execute("SELECT 1").fetchone()
            return result is not None and result[0] == 1
        except Exception as e:
            logger.warning(f"Database ping failed: {str(e)}")
            return False

    async def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current connection."""
        if not await self.is_connected():
            return {"status": "disconnected"}

        try:
            # Get DuckDB version
            version_result = self._connection.execute("SELECT version()").fetchone()
            version = version_result[0] if version_result else "unknown"

            # Get database size
            db_path = Path(self.database_path)
            db_size = db_path.stat().st_size if db_path.exists() else 0

            return {
                "status": "connected",
                "database_path": self.database_path,
                "read_only": self.config.read_only,
                "duckdb_version": version,
                "database_size_bytes": db_size,
                "connection_type": "file" if self.database_path != ":memory:" else "memory",
                "configuration": str(self.config)
            }
        except Exception as e:
            logger.error(f"Failed to get connection info: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def _configure_connection(self) -> None:
        """Configure DuckDB connection settings using configuration object."""
        if not self._connection:
            return

        try:
            # Apply all configuration settings
            for setting in self.config.get_connection_settings():
                try:
                    self._connection.execute(setting)
                except Exception as setting_error:
                    # Log individual setting failures but continue with others
                    logger.warning(f"Configuration setting failed: {setting} - {str(setting_error)}")

            logger.debug(f"DuckDB connection configured: {self.config}")

        except Exception as e:
            logger.warning(f"Failed to configure DuckDB connection: {str(e)}")

    @property
    def raw_connection(self) -> Optional[DuckDBPyConnection]:
        """Get the raw DuckDB connection for advanced usage."""
        return self._connection


class DuckDBTransactionManager(TransactionManager):
    """DuckDB implementation of transaction management."""

    def __init__(self, connection: DuckDBConnection):
        """Initialize transaction manager.

        Args:
            connection: DuckDB connection to manage transactions for
        """
        self.connection = connection
        self._transaction_depth = 0
        self._savepoint_counter = 0

    @asynccontextmanager
    async def transaction(self) -> AsyncContextManager[None]:
        """Create a new database transaction context."""
        if not await self.connection.is_connected():
            raise TransactionError("Database connection is not active")

        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise TransactionError("No raw connection available")

        # Handle nested transactions with savepoints
        if self._transaction_depth > 0:
            savepoint_name = f"sp_{self._savepoint_counter}"
            self._savepoint_counter += 1

            async with self.savepoint(savepoint_name):
                yield
        else:
            # Start new transaction
            try:
                await self.begin_transaction()
                yield
                await self.commit_transaction()
            except Exception:
                await self.rollback_transaction()
                raise

    @asynccontextmanager
    async def savepoint(self, name: str) -> AsyncContextManager[None]:
        """Create a savepoint within an existing transaction."""
        if not await self.is_in_transaction():
            raise TransactionError("Cannot create savepoint outside of transaction")

        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise TransactionError("No raw connection available")

        try:
            # Try to create savepoint - DuckDB doesn't support this
            try:
                raw_conn.execute(f"SAVEPOINT {name}")
                savepoint_supported = True
                logger.debug(f"Created savepoint: {name}")
            except Exception as sp_error:
                # DuckDB doesn't support savepoints, so we'll use transaction semantics
                # This means nested transactions will share the same transaction scope
                savepoint_supported = False
                logger.debug(f"Savepoints not supported, using transaction semantics: {sp_error}")

            yield

            # Release savepoint on success (if supported)
            if savepoint_supported:
                raw_conn.execute(f"RELEASE SAVEPOINT {name}")
                logger.debug(f"Released savepoint: {name}")

        except Exception as e:
            # Rollback to savepoint on error (if supported)
            if savepoint_supported:
                try:
                    raw_conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                    logger.debug(f"Rolled back to savepoint: {name}")
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback to savepoint {name}: {rollback_error}")
                raise TransactionError(f"Savepoint {name} failed: {str(e)}") from e
            else:
                # Without savepoint support, we can't do partial rollbacks
                # The entire transaction will need to be rolled back
                # But we should let the original exception propagate
                logger.warning(f"Savepoint rollback not supported - nested transaction failed: {e}")
                raise  # Re-raise the original exception

    async def begin_transaction(self) -> None:
        """Explicitly begin a new transaction."""
        if not await self.connection.is_connected():
            raise TransactionError("Database connection is not active")

        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise TransactionError("No raw connection available")

        try:
            raw_conn.execute("BEGIN TRANSACTION")
            self._transaction_depth += 1
            logger.debug(f"Transaction started (depth: {self._transaction_depth})")
        except Exception as e:
            raise TransactionError(f"Failed to begin transaction: {str(e)}") from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not await self.is_in_transaction():
            raise TransactionError("No active transaction to commit")

        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise TransactionError("No raw connection available")

        try:
            raw_conn.execute("COMMIT")
            self._transaction_depth = max(0, self._transaction_depth - 1)
            logger.debug(f"Transaction committed (depth: {self._transaction_depth})")
        except Exception as e:
            # Try to rollback on commit failure
            await self.rollback_transaction()
            raise TransactionError(f"Failed to commit transaction: {str(e)}") from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        raw_conn = self.connection.raw_connection
        if not raw_conn:
            logger.warning("No raw connection available for rollback")
            # Still reset the depth even without connection
            self._transaction_depth = 0
            return

        try:
            raw_conn.execute("ROLLBACK")
            self._transaction_depth = max(0, self._transaction_depth - 1)
            logger.debug(f"Transaction rolled back (depth: {self._transaction_depth})")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {str(e)}")
            # Reset depth on rollback failure
            self._transaction_depth = 0

    async def is_in_transaction(self) -> bool:
        """Check if currently within a transaction."""
        return self._transaction_depth > 0
