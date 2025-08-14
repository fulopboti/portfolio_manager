"""Data Access Layer for database operations.

This module provides abstract interfaces for all database operations,
following the Repository pattern and enabling dependency injection.
"""

from .asset_data_access import AssetDataAccess
from .audit_data_access import AuditDataAccess
from .connection import DatabaseConnection, TransactionManager
from .exceptions import (
    ConnectionError,
    DataAccessError,
    MigrationError,
    NotFoundError,
    ParameterError,
    QueryError,
    SchemaError,
    TransactionError,
)
from .metrics_data_access import MetricsDataAccess
from .portfolio_data_access import PortfolioDataAccess
from .query_executor import QueryExecutor, QueryResult
from .schema_manager import MigrationManager, SchemaManager

__all__ = [
    # Core database abstractions
    "DatabaseConnection",
    "TransactionManager",
    "QueryExecutor",
    "QueryResult",
    "SchemaManager",
    "MigrationManager",

    # Data access interfaces
    "AssetDataAccess",
    "PortfolioDataAccess",
    "MetricsDataAccess",
    "AuditDataAccess",

    # Exceptions
    "DataAccessError",
    "ConnectionError",
    "TransactionError",
    "QueryError",
    "ParameterError",
    "SchemaError",
    "MigrationError",
    "NotFoundError",
]
