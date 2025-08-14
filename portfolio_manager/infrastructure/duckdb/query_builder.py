"""Advanced query builder for DuckDB operations with common patterns."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)


class DuckDBQueryBuilder:
    """Advanced query builder that encapsulates common DuckDB query patterns."""

    def __init__(self):
        """Initialize the query builder."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # Common SELECT patterns
    def select_by_id(self, table: str, columns: list[str], id_column: str = "id") -> str:
        """Build SELECT query to find record by ID.

        Args:
            table: Table name
            columns: Columns to select
            id_column: ID column name (default: "id")

        Returns:
            SQL SELECT query string
        """
        return f"SELECT {', '.join(columns)} FROM {table} WHERE {id_column} = ?"

    def select_all(self, table: str, columns: list[str], order_by: str | None = None) -> str:
        """Build SELECT query for all records.

        Args:
            table: Table name
            columns: Columns to select
            order_by: Optional ORDER BY column

        Returns:
            SQL SELECT query string
        """
        query = f"SELECT {', '.join(columns)} FROM {table}"
        if order_by:
            query += f" ORDER BY {order_by}"
        return query

    def select_by_criteria(self, table: str, columns: list[str],
                          where_conditions: dict[str, Any],
                          order_by: str | None = None,
                          limit: int | None = None) -> tuple[str, list[Any]]:
        """Build SELECT query with WHERE conditions.

        Args:
            table: Table name
            columns: Columns to select
            where_conditions: Dict of column->value conditions
            order_by: Optional ORDER BY column
            limit: Optional LIMIT value

        Returns:
            Tuple of (SQL query, parameters list)
        """
        query = f"SELECT {', '.join(columns)} FROM {table}"
        parameters = []

        if where_conditions:
            conditions = []
            for column, value in where_conditions.items():
                conditions.append(f"{column} = ?")
                parameters.append(value)
            query += f" WHERE {' AND '.join(conditions)}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return query, parameters

    def count_records(self, table: str, where_conditions: dict[str, Any] | None = None) -> tuple[str, list[Any]]:
        """Build COUNT query for records.

        Args:
            table: Table name
            where_conditions: Optional WHERE conditions

        Returns:
            Tuple of (SQL query, parameters list)
        """
        query = f"SELECT COUNT(*) FROM {table}"
        parameters = []

        if where_conditions:
            conditions = []
            for column, value in where_conditions.items():
                conditions.append(f"{column} = ?")
                parameters.append(value)
            query += f" WHERE {' AND '.join(conditions)}"

        return query, parameters

    def exists_check(self, table: str, where_conditions: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build EXISTS check query.

        Args:
            table: Table name
            where_conditions: WHERE conditions for existence check

        Returns:
            Tuple of (SQL query, parameters list)
        """
        parameters = []
        conditions = []
        for column, value in where_conditions.items():
            conditions.append(f"{column} = ?")
            parameters.append(value)

        query = f"SELECT 1 FROM {table} WHERE {' AND '.join(conditions)} LIMIT 1"
        return query, parameters

    # Common INSERT patterns
    def upsert(self, table: str, columns: list[str],
              conflict_columns: str | list[str],
              update_columns: list[str] | None = None) -> str:
        """Build UPSERT (INSERT ... ON CONFLICT) query.

        Args:
            table: Table name
            columns: All columns for INSERT
            conflict_columns: Column(s) that trigger conflict resolution (string or list)
            update_columns: Columns to update on conflict (excludes conflict_columns)

        Returns:
            SQL INSERT ... ON CONFLICT query string
        """
        placeholders = ', '.join(['?' for _ in columns])
        base_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        # Handle both single column and multiple columns
        if isinstance(conflict_columns, str):
            conflict_columns_list = [conflict_columns]
            conflict_clause = conflict_columns
        else:
            conflict_columns_list = conflict_columns
            conflict_clause = ', '.join(conflict_columns)

        if update_columns is None:
            # Update all columns except conflict columns
            update_columns = [col for col in columns if col not in conflict_columns_list]

        if update_columns:
            updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            base_query += f" ON CONFLICT ({conflict_clause}) DO UPDATE SET {updates}"

        return base_query

    def insert_ignore_duplicate(self, table: str, columns: list[str], conflict_column: str) -> str:
        """Build INSERT query that ignores duplicates.

        Args:
            table: Table name
            columns: Columns for INSERT
            conflict_column: Column that may cause conflicts

        Returns:
            SQL INSERT query string
        """
        placeholders = ', '.join(['?' for _ in columns])
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT ({conflict_column}) DO NOTHING"

    def batch_insert(self, table: str, columns: list[str], batch_size: int = 1000) -> str:
        """Build batch INSERT query for multiple records.

        Args:
            table: Table name
            columns: Columns for INSERT
            batch_size: Number of records per batch

        Returns:
            SQL INSERT query string with placeholders for batch
        """
        single_row = '(' + ', '.join(['?' for _ in columns]) + ')'
        all_rows = ', '.join([single_row for _ in range(batch_size)])
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {all_rows}"

    # Common UPDATE patterns
    def update_by_id(self, table: str, update_columns: list[str], id_column: str = "id") -> str:
        """Build UPDATE query for single record by ID.

        Args:
            table: Table name
            update_columns: Columns to update
            id_column: ID column name

        Returns:
            SQL UPDATE query string
        """
        set_clauses = ', '.join([f"{col} = ?" for col in update_columns])
        return f"UPDATE {table} SET {set_clauses} WHERE {id_column} = ?"

    def update_by_criteria(self, table: str, update_columns: list[str],
                          where_conditions: dict[str, Any]) -> tuple[str, int]:
        """Build UPDATE query with WHERE conditions.

        Args:
            table: Table name
            update_columns: Columns to update
            where_conditions: WHERE conditions

        Returns:
            Tuple of (SQL query, total parameter count)
        """
        set_clauses = ', '.join([f"{col} = ?" for col in update_columns])
        where_clauses = ' AND '.join([f"{col} = ?" for col in where_conditions.keys()])

        query = f"UPDATE {table} SET {set_clauses} WHERE {where_clauses}"
        param_count = len(update_columns) + len(where_conditions)

        return query, param_count

    # Common DELETE patterns
    def delete_by_id(self, table: str, id_column: str = "id") -> str:
        """Build DELETE query for single record by ID.

        Args:
            table: Table name
            id_column: ID column name

        Returns:
            SQL DELETE query string
        """
        return f"DELETE FROM {table} WHERE {id_column} = ?"

    def delete_by_criteria(self, table: str, where_conditions: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build DELETE query with WHERE conditions.

        Args:
            table: Table name
            where_conditions: WHERE conditions (must not be empty)

        Returns:
            Tuple of (SQL query, parameters list)
        """
        if not where_conditions:
            raise ValueError("DELETE queries must have WHERE conditions for safety")

        parameters = []
        conditions = []
        for column, value in where_conditions.items():
            conditions.append(f"{column} = ?")
            parameters.append(value)

        query = f"DELETE FROM {table} WHERE {' AND '.join(conditions)}"
        return query, parameters

    # Specialized domain patterns
    def time_series_query(self, table: str, symbol_column: str, timestamp_column: str,
                         data_columns: list[str], symbol: str,
                         start_date: datetime | None = None,
                         end_date: datetime | None = None,
                         order_desc: bool = False,
                         limit: int | None = None) -> tuple[str, list[Any]]:
        """Build time series data query.

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
            Tuple of (SQL query, parameters list)
        """
        all_columns = [symbol_column, timestamp_column] + data_columns
        query = f"SELECT {', '.join(all_columns)} FROM {table} WHERE {symbol_column} = ?"
        parameters = [symbol]

        if start_date:
            query += f" AND {timestamp_column} >= ?"
            parameters.append(start_date)

        if end_date:
            query += f" AND {timestamp_column} <= ?"
            parameters.append(end_date)

        order_direction = "DESC" if order_desc else "ASC"
        query += f" ORDER BY {timestamp_column} {order_direction}"

        if limit:
            query += f" LIMIT {limit}"

        return query, parameters

    def latest_record_query(self, table: str, partition_column: str, timestamp_column: str,
                           data_columns: list[str], partition_value: str | None = None) -> tuple[str, list[Any]]:
        """Build query to get latest record per partition (e.g., latest price per symbol).

        Args:
            table: Table name
            partition_column: Column to partition by (e.g., symbol)
            timestamp_column: Column containing timestamp
            data_columns: Data columns to select
            partition_value: Optional specific partition value to filter by

        Returns:
            Tuple of (SQL query, parameters list)
        """
        all_columns = [partition_column, timestamp_column] + data_columns
        column_list = ', '.join(all_columns)

        if partition_value:
            # Get latest for specific partition
            query = f"""
            SELECT {column_list}
            FROM {table}
            WHERE {partition_column} = ?
            ORDER BY {timestamp_column} DESC
            LIMIT 1
            """
            parameters = [partition_value]
        else:
            # Get latest for each partition using window function
            query = f"""
            SELECT {column_list}
            FROM (
                SELECT {column_list},
                       ROW_NUMBER() OVER (PARTITION BY {partition_column} ORDER BY {timestamp_column} DESC) as rn
                FROM {table}
            ) ranked
            WHERE rn = 1
            """
            parameters = []

        return query, parameters

    def aggregation_query(self, table: str, group_columns: list[str],
                         aggregations: dict[str, str],
                         where_conditions: dict[str, Any] | None = None,
                         having_conditions: dict[str, Any] | None = None) -> tuple[str, list[Any]]:
        """Build aggregation query with GROUP BY.

        Args:
            table: Table name
            group_columns: Columns to group by
            aggregations: Dict of {alias: aggregation_expr} (e.g., {"avg_price": "AVG(price)"})
            where_conditions: Optional WHERE conditions
            having_conditions: Optional HAVING conditions

        Returns:
            Tuple of (SQL query, parameters list)
        """
        # Build SELECT clause
        select_parts = group_columns.copy()
        for alias, expr in aggregations.items():
            select_parts.append(f"{expr} as {alias}")

        query = f"SELECT {', '.join(select_parts)} FROM {table}"
        parameters = []

        # Add WHERE clause
        if where_conditions:
            conditions = []
            for column, value in where_conditions.items():
                conditions.append(f"{column} = ?")
                parameters.append(value)
            query += f" WHERE {' AND '.join(conditions)}"

        # Add GROUP BY clause
        query += f" GROUP BY {', '.join(group_columns)}"

        # Add HAVING clause
        if having_conditions:
            conditions = []
            for expr, value in having_conditions.items():
                conditions.append(f"{expr} = ?")
                parameters.append(value)
            query += f" HAVING {' AND '.join(conditions)}"

        return query, parameters


class QueryParameterBuilder:
    """Helper class for building parameter lists with type conversion."""

    @staticmethod
    def prepare_uuid(value: UUID) -> str:
        """Convert UUID to string for database."""
        return str(value)

    @staticmethod
    def prepare_decimal(value: Decimal) -> str:
        """Convert Decimal to string for database to preserve precision."""
        return str(value)

    @staticmethod
    def prepare_datetime(value: datetime) -> datetime:
        """Pass datetime as-is (DuckDB handles it natively)."""
        return value

    @staticmethod
    def prepare_enum(value) -> str:
        """Convert enum to its string value."""
        return value.value if hasattr(value, 'value') else str(value)

    @classmethod
    def build_parameters(cls, values: list[Any]) -> list[Any]:
        """Convert a list of values to database-compatible parameters.

        Args:
            values: List of values to convert

        Returns:
            List of database-compatible parameter values
        """
        parameters = []
        for value in values:
            if isinstance(value, UUID):
                parameters.append(cls.prepare_uuid(value))
            elif isinstance(value, Decimal):
                parameters.append(cls.prepare_decimal(value))
            elif isinstance(value, datetime):
                parameters.append(cls.prepare_datetime(value))
            elif hasattr(value, 'value'):  # Enum-like objects
                parameters.append(cls.prepare_enum(value))
            else:
                parameters.append(value)

        return parameters
