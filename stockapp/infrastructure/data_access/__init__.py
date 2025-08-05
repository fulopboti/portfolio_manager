"""Data Access Layer for database operations.

This module provides abstract interfaces for all database operations,
following the Repository pattern and enabling dependency injection.
"""

from .connection import DatabaseConnection, TransactionManager
from .query_executor import QueryExecutor, QueryResult
from .schema_manager import SchemaManager, MigrationManager
from .asset_data_access import AssetDataAccess
from .portfolio_data_access import PortfolioDataAccess
from .metrics_data_access import MetricsDataAccess
from .audit_data_access import AuditDataAccess
from .exceptions import (
    DataAccessError,
    ConnectionError,
    TransactionError,
    QueryError,
    ParameterError,
    SchemaError,
    MigrationError,
    NotFoundError,
)

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