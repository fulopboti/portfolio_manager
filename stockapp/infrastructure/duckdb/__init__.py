"""DuckDB concrete implementations for the data access layer."""

from .connection import DuckDBConnection, DuckDBTransactionManager
from .query_executor import DuckDBQueryExecutor
from .schema.schema_manager import DuckDBSchemaManager
from .schema.migration_manager import DuckDBMigrationManager
from .base_repository import BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin
from .asset_repository import DuckDBAssetRepository
from .portfolio_repository import DuckDBPortfolioRepository
from .repository_factory import DuckDBRepositoryFactory, create_repository_factory

__all__ = [
    "DuckDBConnection",
    "DuckDBTransactionManager",
    "DuckDBQueryExecutor", 
    "DuckDBSchemaManager",
    "DuckDBMigrationManager",
    "BaseDuckDBRepository",
    "EntityMapperMixin",
    "QueryBuilderMixin",
    "DuckDBAssetRepository",
    "DuckDBPortfolioRepository",
    "DuckDBRepositoryFactory",
    "create_repository_factory",
]