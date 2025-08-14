"""Query execution and result handling abstractions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QueryResult:
    """Represents the result of a database query.

    Provides a consistent interface for query results regardless
    of the underlying database implementation.
    """

    rows: list[dict[str, Any]]
    row_count: int
    column_names: list[str]
    execution_time_ms: float | None = None
    affected_rows: int | None = None

    def first(self) -> dict[str, Any] | None:
        """Get the first row of results.

        Returns:
            First row as dictionary, or None if no results
        """
        return self.rows[0] if self.rows else None

    def scalar(self) -> Any:
        """Get a single scalar value from the first row, first column.

        Returns:
            Scalar value, or None if no results

        Note:
            If multiple columns are present, returns the first column value
        """
        if not self.rows:
            return None
        if not self.column_names:
            return None
        return self.rows[0][self.column_names[0]]

    def is_empty(self) -> bool:
        """Check if query returned no results.

        Returns:
            bool: True if no rows returned
        """
        return self.row_count == 0


class QueryExecutor(ABC):
    """Abstract interface for executing database queries.

    Provides methods for executing various types of database operations
    with parameter binding, result mapping, and error handling.
    """

    @abstractmethod
    async def execute_query(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None
    ) -> QueryResult:
        """Execute a SELECT query and return results.

        Args:
            sql: SQL SELECT statement
            parameters: Named parameters for the query

        Returns:
            QueryResult containing rows and metadata

        Raises:
            QueryError: If query execution fails
            ParameterError: If parameters are invalid
        """
        pass

    @abstractmethod
    async def execute_command(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None
    ) -> int:
        """Execute a non-query command (INSERT, UPDATE, DELETE).

        Args:
            sql: SQL command statement
            parameters: Named parameters for the command

        Returns:
            Number of affected rows

        Raises:
            QueryError: If command execution fails
            ParameterError: If parameters are invalid
        """
        pass

    @abstractmethod
    async def execute_batch(
        self,
        sql: str,
        parameters_list: list[dict[str, Any]]
    ) -> list[int]:
        """Execute a command multiple times with different parameters.

        Args:
            sql: SQL command statement
            parameters_list: List of parameter dictionaries

        Returns:
            List of affected row counts for each execution

        Raises:
            QueryError: If any execution fails
            ParameterError: If any parameters are invalid
        """
        pass

    @abstractmethod
    async def execute_scalar(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None
    ) -> Any:
        """Execute a query and return a single scalar value.

        Args:
            sql: SQL query returning single value
            parameters: Named parameters for the query

        Returns:
            Single scalar value from first row, first column

        Raises:
            QueryError: If query execution fails
            ValueError: If query returns multiple values
        """
        pass

    @abstractmethod
    async def execute_transaction(
        self,
        operations: list[tuple[str, dict[str, Any] | None]]
    ) -> list[Any]:
        """Execute multiple operations within a single transaction.

        Args:
            operations: List of (sql, parameters) tuples

        Returns:
            List of results from each operation

        Raises:
            TransactionError: If transaction fails and is rolled back
        """
        pass

    @abstractmethod
    def validate_parameters(self, parameters: dict[str, Any]) -> bool:
        """Validate query parameters for type safety.

        Args:
            parameters: Parameters to validate

        Returns:
            bool: True if parameters are valid

        Raises:
            ParameterError: If parameters are invalid with details
        """
        pass

    @abstractmethod
    def escape_identifier(self, identifier: str) -> str:
        """Escape a database identifier (table, column name).

        Args:
            identifier: Database identifier to escape

        Returns:
            Properly escaped identifier for the database
        """
        pass

    @abstractmethod
    def format_value(self, value: Any) -> str:
        """Format a Python value for database insertion.

        Args:
            value: Python value to format

        Returns:
            String representation suitable for database
        """
        pass
