"""Database connection and transaction management abstractions."""

from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Any


class DatabaseConnection(ABC):
    """Abstract interface for database connection management.

    Provides core database connectivity operations including connection
    lifecycle, health checking, and resource management.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database.

        Raises:
            ConnectionError: If connection cannot be established
            DatabaseError: If database is unavailable or misconfigured
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the database connection and clean up resources.

        Should be idempotent - safe to call multiple times.
        """
        pass

    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if database connection is active and healthy.

        Returns:
            bool: True if connection is active, False otherwise
        """
        pass

    @abstractmethod
    async def ping(self) -> bool:
        """Ping the database to verify connectivity.

        Returns:
            bool: True if database responds, False otherwise
        """
        pass

    @abstractmethod
    async def get_connection_info(self) -> dict[str, Any]:
        """Get information about the current connection.

        Returns:
            Dict containing connection metadata like database version,
            connection pool status, etc.
        """
        pass


class TransactionManager(ABC):
    """Abstract interface for database transaction management.

    Provides transaction lifecycle management with support for
    nested transactions, savepoints, and rollback operations.
    """

    @abstractmethod
    def transaction(self) -> AbstractAsyncContextManager[None]:
        """Create a new database transaction context.

        Usage:
            async with transaction_manager.transaction():
                # Database operations here
                # Automatic commit on success, rollback on exception

        Yields:
            None: Transaction context for database operations

        Raises:
            TransactionError: If transaction cannot be started
            DatabaseError: If database connection fails
        """
        pass

    @abstractmethod
    def savepoint(self, name: str) -> AbstractAsyncContextManager[None]:
        """Create a savepoint within an existing transaction.

        Args:
            name: Unique name for the savepoint

        Usage:
            async with transaction_manager.transaction():
                # Some operations
                async with transaction_manager.savepoint("checkpoint1"):
                    # More operations that might fail
                    # Rollback to savepoint on exception

        Yields:
            None: Savepoint context for database operations

        Raises:
            TransactionError: If savepoint cannot be created
            ValueError: If name is invalid or already exists
        """
        pass

    @abstractmethod
    async def begin_transaction(self) -> None:
        """Explicitly begin a new transaction.

        Use this for manual transaction management when context managers
        are not suitable.

        Raises:
            TransactionError: If transaction cannot be started
        """
        pass

    @abstractmethod
    async def commit_transaction(self) -> None:
        """Commit the current transaction.

        Raises:
            TransactionError: If no active transaction or commit fails
        """
        pass

    @abstractmethod
    async def rollback_transaction(self) -> None:
        """Rollback the current transaction.

        Should be safe to call even if no transaction is active.
        """
        pass

    @abstractmethod
    async def is_in_transaction(self) -> bool:
        """Check if currently within a transaction.

        Returns:
            bool: True if in transaction, False otherwise
        """
        pass
