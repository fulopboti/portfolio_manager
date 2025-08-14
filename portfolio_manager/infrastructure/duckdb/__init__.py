"""DuckDB concrete implementations for the data access layer."""

from .asset_repository import DuckDBAssetRepository
from .base_repository import BaseDuckDBRepository, EntityMapperMixin, QueryBuilderMixin
from .connection import DuckDBConnection, DuckDBTransactionManager
from .portfolio_repository import DuckDBPortfolioRepository
from .query_executor import DuckDBQueryExecutor
from .repository_factory import DuckDBRepositoryFactory, create_repository_factory
from .schema.migration_manager import DuckDBMigrationManager
from .schema.schema_manager import DuckDBSchemaManager

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
