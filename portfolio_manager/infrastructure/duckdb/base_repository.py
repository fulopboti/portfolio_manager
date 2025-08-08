"""
Base repository class providing common DuckDB infrastructure functionality.

This module provides abstract base classes and utilities that eliminate
code duplication across DuckDB repository implementations.
"""

import logging
from abc import ABC
from typing import Any, Dict, List, Optional, Union
from contextlib import asynccontextmanager

from portfolio_manager.infrastructure.data_access.exceptions import (
    DataAccessError,
    NotFoundError,
)
from .connection import DuckDBConnection
from .query_executor import DuckDBQueryExecutor
from .query_builder import DuckDBQueryBuilder, QueryParameterBuilder


class BaseDuckDBRepository(ABC):
    """
    Base class for all DuckDB repository implementations.

    Provides common functionality for:
    - Connection and query executor management
    - Structured logging with context
    - Standardized error handling
    - Common database operation patterns
    - Transaction management utilities
    """

    def __init__(self, connection: DuckDBConnection, query_executor: DuckDBQueryExecutor):
        """
        Initialize base DuckDB repository.

        Args:
            connection: Active DuckDB connection
            query_executor: Query executor for database operations
        """
        self.connection = connection
        self.query_executor = query_executor
        self.query_builder = DuckDBQueryBuilder()
        self.param_builder = QueryParameterBuilder()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._setup_logging_context()

    def _setup_logging_context(self) -> None:
        """Setup logging context for this repository."""
        self._log_prefix = f"[{self.__class__.__name__}]"

    async def _execute_query(
        self, 
        query: str, 
        parameters: Optional[List[Any]] = None,
        operation_name: str = "query"
    ) -> None:
        """
        Execute a query with standardized error handling and logging.

        Args:
            query: SQL query to execute
            parameters: Query parameters
            operation_name: Name of the operation for logging

        Raises:
            DataAccessError: If query execution fails
        """
        try:
            self.logger.debug(f"{self._log_prefix} Executing {operation_name}")
            await self.query_executor.execute_query(query, parameters or [])
            self.logger.debug(f"{self._log_prefix} Successfully completed {operation_name}")

        except Exception as e:
            error_msg = f"Failed to execute {operation_name}: {str(e)}"
            self.logger.error(f"{self._log_prefix} {error_msg}")
            raise DataAccessError(error_msg) from e

    async def _fetch_one(
        self, 
        query: str, 
        parameters: Optional[List[Any]] = None,
        operation_name: str = "fetch_one"
    ) -> Optional[Any]:
        """
        Fetch a single result with standardized error handling.

        Args:
            query: SQL query to execute
            parameters: Query parameters
            operation_name: Name of the operation for logging

        Returns:
            Query result or None if no result found

        Raises:
            DataAccessError: If query execution fails
        """
        try:
            self.logger.debug(f"{self._log_prefix} Executing {operation_name}")
            result = await self.query_executor.fetch_one(query, parameters or [])
            self.logger.debug(f"{self._log_prefix} Successfully completed {operation_name}")
            return result

        except Exception as e:
            error_msg = f"Failed to execute {operation_name}: {str(e)}"
            self.logger.error(f"{self._log_prefix} {error_msg}")
            raise DataAccessError(error_msg) from e

    async def _fetch_all(
        self, 
        query: str, 
        parameters: Optional[List[Any]] = None,
        operation_name: str = "fetch_all"
    ) -> List[Any]:
        """
        Fetch all results with standardized error handling.

        Args:
            query: SQL query to execute
            parameters: Query parameters
            operation_name: Name of the operation for logging

        Returns:
            List of query results (empty list if no results)

        Raises:
            DataAccessError: If query execution fails
        """
        try:
            self.logger.debug(f"{self._log_prefix} Executing {operation_name}")
            results = await self.query_executor.fetch_all(query, parameters or [])
            self.logger.debug(f"{self._log_prefix} Successfully completed {operation_name} - {len(results)} rows")
            return results

        except Exception as e:
            error_msg = f"Failed to execute {operation_name}: {str(e)}"
            self.logger.error(f"{self._log_prefix} {error_msg}")
            raise DataAccessError(error_msg) from e

    async def _execute_with_result(
        self, 
        query: str, 
        parameters: Optional[List[Any]] = None,
        operation_name: str = "execute_with_result"
    ) -> Any:
        """
        Execute a query and return the result with standardized error handling.

        Args:
            query: SQL query to execute
            parameters: Query parameters
            operation_name: Name of the operation for logging

        Returns:
            Query execution result

        Raises:
            DataAccessError: If query execution fails
        """
        try:
            self.logger.debug(f"{self._log_prefix} Executing {operation_name}")
            result = await self.query_executor.execute_with_result(query, parameters or [])
            self.logger.debug(f"{self._log_prefix} Successfully completed {operation_name}")
            return result

        except Exception as e:
            error_msg = f"Failed to execute {operation_name}: {str(e)}"
            self.logger.error(f"{self._log_prefix} {error_msg}")
            raise DataAccessError(error_msg) from e

    @asynccontextmanager
    async def _transaction(self, operation_name: str = "transaction"):
        """
        Context manager for database transactions with automatic rollback.

        Args:
            operation_name: Name of the operation for logging

        Usage:
            async with self._transaction("save_portfolio"):
                await self._execute_query("INSERT INTO ...")
                await self._execute_query("UPDATE ...")
        """
        self.logger.debug(f"{self._log_prefix} Starting {operation_name}")

        try:
            await self.connection.begin_transaction()
            yield
            await self.connection.commit_transaction()
            self.logger.debug(f"{self._log_prefix} Successfully committed {operation_name}")

        except Exception as e:
            await self.connection.rollback_transaction()
            error_msg = f"Transaction failed for {operation_name}: {str(e)}"
            self.logger.error(f"{self._log_prefix} {error_msg}")
            raise DataAccessError(error_msg) from e

    def _build_error_context(self, operation: str, entity_info: str, error: Exception) -> Dict[str, Any]:
        """
        Build error context for structured logging.

        Args:
            operation: The operation that failed
            entity_info: Information about the entity being operated on
            error: The exception that occurred

        Returns:
            Dictionary with error context information
        """
        return {
            'operation': operation,
            'entity_info': entity_info,
            'error_type': error.__class__.__name__,
            'repository_class': self.__class__.__name__,
        }

    def _log_operation_start(self, operation: str, entity_info: str) -> None:
        """Log the start of a repository operation."""
        self.logger.debug(f"{self._log_prefix} Starting {operation} for {entity_info}")

    def _log_operation_success(self, operation: str, entity_info: str) -> None:
        """Log the successful completion of a repository operation."""
        self.logger.debug(f"{self._log_prefix} Successfully completed {operation} for {entity_info}")

    def _log_operation_error(self, operation: str, entity_info: str, error: Exception) -> None:
        """Log an error during a repository operation."""
        error_context = self._build_error_context(operation, entity_info, error)
        self.logger.error(
            f"{self._log_prefix} Failed {operation} for {entity_info}: {error}",
            extra=error_context
        )


class EntityMapperMixin:
    """
    Mixin class providing common entity mapping utilities.

    This mixin provides helper methods for converting between
    database rows and domain entities.
    """

    def _safe_uuid_convert(self, value: Union[str, Any]) -> Any:
        """
        Safely convert string to UUID if needed.

        Args:
            value: Value that might be a UUID string

        Returns:
            UUID object if value was string, original value otherwise
        """
        from uuid import UUID

        if isinstance(value, str):
            try:
                return UUID(value)
            except ValueError:
                return value
        return value

    def _safe_decimal_convert(self, value: Union[float, str, Any]) -> Any:
        """
        Safely convert numeric value to Decimal if needed.

        Args:
            value: Value that might need Decimal conversion

        Returns:
            Decimal object if value was numeric, original value otherwise
        """
        from decimal import Decimal

        if isinstance(value, (int, float)):
            return Decimal(str(value))
        elif isinstance(value, str):
            try:
                return Decimal(value)
            except (ValueError, TypeError):
                return value
        return value

    def _safe_enum_convert(self, value: str, enum_class: type) -> Any:
        """
        Safely convert string to enum value.

        Args:
            value: String value to convert
            enum_class: Enum class to convert to

        Returns:
            Enum instance

        Raises:
            DataAccessError: If conversion fails
        """
        try:
            return enum_class(value)
        except (ValueError, TypeError) as e:
            raise DataAccessError(f"Invalid enum value '{value}' for {enum_class.__name__}") from e


class QueryBuilderMixin:
    """
    Mixin class providing common query building utilities.

    This mixin provides helper methods for building common SQL patterns.
    """

    def _build_insert_query(
        self, 
        table: str, 
        columns: List[str], 
        on_conflict_update: bool = True
    ) -> str:
        """
        Build an INSERT query with optional ON CONFLICT clause.

        Args:
            table: Table name
            columns: List of column names
            on_conflict_update: Whether to include ON CONFLICT DO UPDATE

        Returns:
            SQL INSERT query string
        """
        placeholders = ", ".join(["?" for _ in columns])
        base_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        if on_conflict_update:
            # Exclude primary key columns from update (assume first column is PK)
            update_columns = columns[1:] if len(columns) > 1 else []
            if update_columns:
                updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                base_query += f" ON CONFLICT ({columns[0]}) DO UPDATE SET {updates}"

        return base_query

    def _build_select_query(
        self, 
        table: str, 
        columns: List[str], 
        where_conditions: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Build a SELECT query with optional WHERE, ORDER BY, and LIMIT clauses.

        Args:
            table: Table name
            columns: List of column names to select
            where_conditions: List of WHERE conditions
            order_by: List of columns to order by
            limit: Maximum number of rows to return

        Returns:
            SQL SELECT query string
        """
        query = f"SELECT {', '.join(columns)} FROM {table}"

        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"

        if order_by:
            query += f" ORDER BY {', '.join(order_by)}"

        if limit:
            query += f" LIMIT {limit}"

        return query

    def _build_delete_query(
        self, 
        table: str, 
        where_conditions: List[str]
    ) -> str:
        """
        Build a DELETE query with WHERE clause.

        Args:
            table: Table name
            where_conditions: List of WHERE conditions

        Returns:
            SQL DELETE query string
        """
        if not where_conditions:
            raise ValueError("DELETE queries must have WHERE conditions")

        return f"DELETE FROM {table} WHERE {' AND '.join(where_conditions)}"

    # Advanced query pattern methods using DuckDBQueryBuilder

    async def _find_by_id_pattern(self, table: str, columns: List[str], 
                                  id_value: Any, id_column: str = "id") -> Optional[List[Any]]:
        """
        Find single record by ID using common pattern.

        Args:
            table: Table name
            columns: Columns to select  
            id_value: ID value to search for
            id_column: ID column name

        Returns:
            Row data if found, None otherwise
        """
        query = self.query_builder.select_by_id(table, columns, id_column)
        parameters = self.param_builder.build_parameters([id_value])
        return await self._fetch_one(query, parameters)

    async def _find_all_pattern(self, table: str, columns: List[str], 
                               order_by: Optional[str] = None) -> List[List[Any]]:
        """
        Find all records using common pattern.

        Args:
            table: Table name
            columns: Columns to select
            order_by: Optional ordering column

        Returns:
            List of row data
        """
        query = self.query_builder.select_all(table, columns, order_by)
        return await self._fetch_all(query)

    async def _find_by_criteria_pattern(self, table: str, columns: List[str], 
                                       criteria: Dict[str, Any],
                                       order_by: Optional[str] = None,
                                       limit: Optional[int] = None) -> List[List[Any]]:
        """
        Find records by criteria using common pattern.

        Args:
            table: Table name
            columns: Columns to select
            criteria: WHERE conditions as dict
            order_by: Optional ordering column
            limit: Optional limit

        Returns:
            List of row data
        """
        query, raw_parameters = self.query_builder.select_by_criteria(
            table, columns, criteria, order_by, limit
        )
        parameters = self.param_builder.build_parameters(raw_parameters)
        return await self._fetch_all(query, parameters)

    async def _count_pattern(self, table: str, criteria: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records using common pattern.

        Args:
            table: Table name
            criteria: Optional WHERE conditions

        Returns:
            Record count
        """
        query, raw_parameters = self.query_builder.count_records(table, criteria)
        parameters = self.param_builder.build_parameters(raw_parameters)
        result = await self._fetch_one(query, parameters)
        return result[0] if result else 0

    async def _exists_pattern(self, table: str, criteria: Dict[str, Any]) -> bool:
        """
        Check if record exists using common pattern.

        Args:
            table: Table name
            criteria: WHERE conditions for existence check

        Returns:
            True if record exists, False otherwise
        """
        query, raw_parameters = self.query_builder.exists_check(table, criteria)
        parameters = self.param_builder.build_parameters(raw_parameters)
        result = await self._fetch_one(query, parameters)
        return result is not None

    async def _upsert_pattern(self, table: str, columns: List[str], values: List[Any],
                             conflict_column: str, update_columns: Optional[List[str]] = None) -> None:
        """
        Upsert record using common pattern.

        Args:
            table: Table name
            columns: Column names
            values: Values to insert/update
            conflict_column: Column that triggers conflict resolution
            update_columns: Columns to update on conflict
        """
        query = self.query_builder.upsert(table, columns, conflict_column, update_columns)
        parameters = self.param_builder.build_parameters(values)
        await self._execute_query(query, parameters, f"upsert_{table}")

    async def _time_series_pattern(self, table: str, symbol_column: str, timestamp_column: str,
                                  data_columns: List[str], symbol: str,
                                  start_date=None, end_date=None, 
                                  order_desc: bool = False, limit: Optional[int] = None) -> List[List[Any]]:
        """
        Get time series data using common pattern.

        Args:
            table: Table name
            symbol_column: Column containing symbol/identifier  
            timestamp_column: Column containing timestamp
            data_columns: Data columns to select
            symbol: Symbol to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter  
            order_desc: Whether to order by timestamp DESC
            limit: Optional limit

        Returns:
            List of time series row data
        """
        query, raw_parameters = self.query_builder.time_series_query(
            table, symbol_column, timestamp_column, data_columns, symbol,
            start_date, end_date, order_desc, limit
        )
        parameters = self.param_builder.build_parameters(raw_parameters)
        return await self._fetch_all(query, parameters)

    async def _latest_record_pattern(self, table: str, partition_column: str, timestamp_column: str,
                                    data_columns: List[str], partition_value: Optional[str] = None) -> Optional[List[Any]]:
        """
        Get latest record per partition using common pattern.

        Args:
            table: Table name
            partition_column: Column to partition by (e.g., symbol)
            timestamp_column: Column containing timestamp
            data_columns: Data columns to select
            partition_value: Optional specific partition value

        Returns:
            Latest record data if found, None otherwise
        """
        query, raw_parameters = self.query_builder.latest_record_query(
            table, partition_column, timestamp_column, data_columns, partition_value
        )
        parameters = self.param_builder.build_parameters(raw_parameters)

        if partition_value:
            return await self._fetch_one(query, parameters)
        else:
            results = await self._fetch_all(query, parameters)
            return results[0] if results else None

    async def _delete_by_criteria_pattern(self, table: str, criteria: Dict[str, Any]) -> int:
        """
        Delete records by criteria using common pattern.

        Args:
            table: Table name
            criteria: WHERE conditions for deletion

        Returns:
            Number of records deleted
        """
        query, raw_parameters = self.query_builder.delete_by_criteria(table, criteria)
        parameters = self.param_builder.build_parameters(raw_parameters)

        # Execute and return affected rows count
        await self._execute_query(query, parameters, f"delete_from_{table}")
        # Note: DuckDB doesn't return affected rows count directly, so we return 1 if successful
        return 1
