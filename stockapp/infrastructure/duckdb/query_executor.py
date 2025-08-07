"""DuckDB query execution implementation."""

import logging
import re
import time
from functools import lru_cache
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import duckdb

from stockapp.infrastructure.data_access.query_executor import QueryExecutor, QueryResult
from stockapp.infrastructure.data_access.exceptions import (
    QueryError,
    ParameterError,
    TransactionError,
)
from .connection import DuckDBConnection, DuckDBTransactionManager

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1000)
def _validate_parameter_name(name: str) -> bool:
    """Cached parameter name validation to improve performance.
    
    Args:
        name: Parameter name to validate
        
    Returns:
        True if parameter name is valid
        
    Raises:
        ParameterError: If parameter name is invalid
    """
    if not isinstance(name, str) or not name:
        raise ParameterError(f"Parameter name must be non-empty string: {name}")
        
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ParameterError(f"Invalid parameter name: {name}")
    
    return True


class DuckDBQueryExecutor(QueryExecutor):
    """DuckDB implementation of query execution."""

    def __init__(self, connection: DuckDBConnection):
        """Initialize query executor.
        
        Args:
            connection: DuckDB connection to execute queries on
        """
        self.connection = connection
        self.transaction_manager = DuckDBTransactionManager(connection)

    async def execute_query(
        self, 
        sql: str, 
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> QueryResult:
        """Execute a SELECT query and return results."""
        if not await self.connection.is_connected():
            raise QueryError("Database connection is not active")
            
        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise QueryError("No raw connection available")

        try:
            start_time = time.perf_counter()
            
            # Validate and prepare parameters
            safe_params = self._prepare_parameters(parameters)
            
            # Execute query
            if safe_params:
                result = raw_conn.execute(sql, safe_params)
            else:
                result = raw_conn.execute(sql)
            
            # Fetch all results
            rows = result.fetchall()
            column_names = [desc[0] for desc in result.description] if result.description else []
            
            # Convert rows to list of dictionaries
            dict_rows = []
            for row in rows:
                dict_row = {}
                for i, value in enumerate(row):
                    if i < len(column_names):
                        dict_row[column_names[i]] = self._convert_value(value)
                dict_rows.append(dict_row)
            
            execution_time = (time.perf_counter() - start_time) * 1000  # Convert to ms
            
            query_result = QueryResult(
                rows=dict_rows,
                row_count=len(dict_rows),
                column_names=column_names,
                execution_time_ms=execution_time
            )
            
            logger.debug(f"Query executed successfully: {len(dict_rows)} rows in {execution_time:.2f}ms")
            return query_result
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise QueryError(f"Failed to execute query: {str(e)}") from e

    async def execute_command(
        self, 
        sql: str, 
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> int:
        """Execute a non-query command (INSERT, UPDATE, DELETE)."""
        if not await self.connection.is_connected():
            raise QueryError("Database connection is not active")
            
        raw_conn = self.connection.raw_connection
        if not raw_conn:
            raise QueryError("No raw connection available")

        try:
            start_time = time.perf_counter()
            
            # Validate and prepare parameters
            safe_params = self._prepare_parameters(parameters)
            
            # Execute command
            if safe_params:
                result = raw_conn.execute(sql, safe_params)
            else:
                result = raw_conn.execute(sql)
            
            # Get affected row count - DuckDB may return -1 for some operations
            if hasattr(result, 'rowcount') and result.rowcount >= 0:
                affected_rows = result.rowcount
            else:
                # For operations that don't return a meaningful rowcount, return 0
                affected_rows = 0
            
            execution_time = (time.perf_counter() - start_time) * 1000
            logger.debug(f"Command executed: {affected_rows} rows affected in {execution_time:.2f}ms")
            
            return affected_rows
            
        except Exception as e:
            logger.error(f"Command execution failed: {str(e)}")
            raise QueryError(f"Failed to execute command: {str(e)}") from e

    async def execute_batch(
        self, 
        sql: str, 
        parameters_list: List[Dict[str, Any]]
    ) -> List[int]:
        """Execute a command multiple times with different parameters."""
        if not await self.connection.is_connected():
            raise QueryError("Database connection is not active")
            
        if not parameters_list:
            return []

        results = []
        
        # Use transaction for batch operations
        async with self.transaction_manager.transaction():
            for params in parameters_list:
                affected_rows = await self.execute_command(sql, params)
                results.append(affected_rows)
        
        logger.debug(f"Batch executed: {len(parameters_list)} commands, {sum(results)} total rows affected")
        return results

    async def execute_scalar(
        self, 
        sql: str, 
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> Any:
        """Execute a query and return a single scalar value."""
        result = await self.execute_query(sql, parameters)
        return result.scalar()
    
    async def fetch_one(
        self, 
        sql: str, 
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> Optional[tuple]:
        """Execute a query and return the first row as a tuple, or None."""
        result = await self.execute_query(sql, parameters)
        if not result.rows:
            return None
        # Convert first row dict to tuple of values
        first_row = result.rows[0]
        return tuple(first_row.values()) if isinstance(first_row, dict) else first_row
    
    async def fetch_all(
        self, 
        sql: str, 
        parameters: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> List[tuple]:
        """Execute a query and return all rows as tuples."""
        result = await self.execute_query(sql, parameters)
        if not result.rows:
            return []
        # Convert row dicts to tuples of values
        if isinstance(result.rows[0], dict):
            return [tuple(row.values()) for row in result.rows]
        return result.rows

    async def execute_transaction(
        self, 
        operations: List[tuple[str, Optional[Dict[str, Any]]]]
    ) -> List[Any]:
        """Execute multiple operations within a single transaction."""
        if not operations:
            return []

        results = []
        
        try:
            async with self.transaction_manager.transaction():
                for sql, params in operations:
                    # Determine if this is a query or command based on SQL
                    if self._is_select_query(sql):
                        result = await self.execute_query(sql, params)
                        results.append(result)
                    else:
                        affected_rows = await self.execute_command(sql, params)
                        results.append(affected_rows)
            
            logger.debug(f"Transaction executed successfully: {len(operations)} operations")
            return results
            
        except Exception as e:
            logger.error(f"Transaction failed: {str(e)}")
            raise TransactionError(f"Transaction execution failed: {str(e)}") from e

    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate query parameters for type safety."""
        try:
            self._prepare_parameters(parameters)
            return True
        except ParameterError:
            return False

    def escape_identifier(self, identifier: str) -> str:
        """Escape a database identifier (table, column name)."""
        if not identifier:
            raise ParameterError("Identifier cannot be empty")
            
        # Check for valid identifier characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ParameterError(f"Invalid identifier: {identifier}")
            
        # DuckDB uses double quotes for identifiers
        return f'"{identifier}"'

    def format_value(self, value: Any) -> str:
        """Format a Python value for database insertion."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes in strings
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float, Decimal)):
            return str(value)
        elif isinstance(value, datetime):
            # Format datetime in ISO format
            return f"'{value.isoformat()}'"
        else:
            # Convert to string and treat as string
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

    def _prepare_parameters(self, parameters: Optional[Union[Dict[str, Any], List[Any]]]) -> Union[Dict[str, Any], List[Any], None]:
        """Validate and prepare parameters for safe execution."""
        if parameters is None:
            return None
            
        if isinstance(parameters, list):
            # Handle positional parameters (for DuckDB prepared statements)
            if not parameters:  # Empty list
                return []
            return [self._convert_parameter_value(value) for value in parameters]
        
        elif isinstance(parameters, dict):
            # Handle named parameters
            safe_params = {}
            for key, value in parameters.items():
                # Validate parameter name (cached for performance)
                _validate_parameter_name(key)
                
                # Convert and validate parameter value
                safe_params[key] = self._convert_parameter_value(value)
            
            return safe_params
        
        else:
            raise ParameterError(f"Parameters must be list or dict, got: {type(parameters)}")

    def _convert_parameter_value(self, value: Any) -> Any:
        """Convert a parameter value to a DuckDB-compatible type."""
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, Decimal):
            return float(value)  # DuckDB handles decimals as floats
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, tuple)):
            # Convert collections to list of converted values
            return [self._convert_parameter_value(item) for item in value]
        else:
            # Try to convert to string as fallback
            try:
                return str(value)
            except Exception as e:
                raise ParameterError(f"Cannot convert parameter value: {value}") from e

    def _convert_value(self, value: Any) -> Any:
        """Convert a database value to appropriate Python type."""
        if value is None:
            return None
        elif isinstance(value, str):
            # Try to parse datetime strings
            if self._looks_like_datetime(value):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    return value
            return value
        elif isinstance(value, (int, bool)):
            return value
        elif isinstance(value, float):
            # Convert to Decimal for financial precision
            return Decimal(str(value))
        else:
            return value

    def _looks_like_datetime(self, value: str) -> bool:
        """Check if a string looks like a datetime."""
        # Simple heuristic for ISO datetime format
        return (
            len(value) >= 10 and
            '-' in value and
            ('T' in value or ' ' in value or ':' in value)
        )

    def _is_select_query(self, sql: str) -> bool:
        """Determine if SQL is a SELECT query."""
        trimmed = sql.strip().upper()
        return trimmed.startswith('SELECT') or trimmed.startswith('WITH')