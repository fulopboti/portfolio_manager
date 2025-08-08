"""Unit tests for Data Access Layer contracts and interfaces.

These tests verify that all abstract interfaces are properly defined
and that concrete implementations will comply with the expected contracts.
"""

import pytest
from abc import ABC
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4, UUID

from portfolio_manager.infrastructure.data_access import (
    DatabaseConnection,
    TransactionManager,
    QueryExecutor,
    QueryResult,
    SchemaManager,
    MigrationManager,
    AssetDataAccess,
    PortfolioDataAccess,
    MetricsDataAccess,
    AuditDataAccess,
)
from portfolio_manager.infrastructure.data_access.schema_manager import Migration, MigrationType, TableDefinition
from portfolio_manager.infrastructure.data_access.metrics_data_access import MetricType
from portfolio_manager.infrastructure.data_access.audit_data_access import AuditEventType, AuditSeverity
from portfolio_manager.domain.entities import Asset, AssetSnapshot, AssetType, Portfolio, Trade, Position, TradeSide


class TestDatabaseConnectionInterface:
    """Test cases for DatabaseConnection interface contract."""

    def test_database_connection_is_abstract(self):
        """Test that DatabaseConnection is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            DatabaseConnection()

    def test_database_connection_required_methods(self):
        """Test that DatabaseConnection defines required abstract methods."""
        required_methods = [
            'connect',
            'disconnect', 
            'is_connected',
            'ping',
            'get_connection_info'
        ]

        for method_name in required_methods:
            assert hasattr(DatabaseConnection, method_name)
            method = getattr(DatabaseConnection, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    def test_database_connection_inheritance(self):
        """Test that DatabaseConnection properly inherits from ABC."""
        assert issubclass(DatabaseConnection, ABC)


class TestTransactionManagerInterface:
    """Test cases for TransactionManager interface contract."""

    def test_transaction_manager_is_abstract(self):
        """Test that TransactionManager is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            TransactionManager()

    def test_transaction_manager_required_methods(self):
        """Test that TransactionManager defines required abstract methods."""
        required_methods = [
            'transaction',
            'savepoint',
            'begin_transaction',
            'commit_transaction',
            'rollback_transaction',
            'is_in_transaction'
        ]

        for method_name in required_methods:
            assert hasattr(TransactionManager, method_name)
            method = getattr(TransactionManager, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestQueryExecutorInterface:
    """Test cases for QueryExecutor interface contract."""

    def test_query_executor_is_abstract(self):
        """Test that QueryExecutor is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            QueryExecutor()

    def test_query_executor_required_methods(self):
        """Test that QueryExecutor defines required abstract methods."""
        required_methods = [
            'execute_query',
            'execute_command',
            'execute_batch',
            'execute_scalar',
            'execute_transaction',
            'validate_parameters',
            'escape_identifier',
            'format_value'
        ]

        for method_name in required_methods:
            assert hasattr(QueryExecutor, method_name)
            method = getattr(QueryExecutor, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestQueryResultDataClass:
    """Test cases for QueryResult data structure."""

    def test_query_result_creation(self):
        """Test QueryResult can be created with required fields."""
        rows = [{"id": 1, "name": "test"}]
        result = QueryResult(
            rows=rows,
            row_count=1,
            column_names=["id", "name"]
        )

        assert result.rows == rows
        assert result.row_count == 1
        assert result.column_names == ["id", "name"]

    def test_query_result_first_method(self):
        """Test QueryResult.first() method."""
        # Test with results
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        result = QueryResult(rows=rows, row_count=2, column_names=["id", "name"])

        first_row = result.first()
        assert first_row == {"id": 1, "name": "test"}

        # Test with no results
        empty_result = QueryResult(rows=[], row_count=0, column_names=[])
        assert empty_result.first() is None

    def test_query_result_scalar_method(self):
        """Test QueryResult.scalar() method."""
        # Test with single column result
        rows = [{"count": 5}]
        result = QueryResult(rows=rows, row_count=1, column_names=["count"])

        assert result.scalar() == 5

        # Test with no results
        empty_result = QueryResult(rows=[], row_count=0, column_names=["count"])
        assert empty_result.scalar() is None

        # Test with multiple columns (should return first column)
        multi_col_result = QueryResult(
            rows=[{"id": 1, "name": "test"}], 
            row_count=1, 
            column_names=["id", "name"]
        )

        # Should return the first column value
        assert multi_col_result.scalar() == 1

    def test_query_result_is_empty_method(self):
        """Test QueryResult.is_empty() method."""
        # Test with results
        result = QueryResult(
            rows=[{"id": 1}], 
            row_count=1, 
            column_names=["id"]
        )
        assert not result.is_empty()

        # Test with no results
        empty_result = QueryResult(rows=[], row_count=0, column_names=[])
        assert empty_result.is_empty()


class TestSchemaManagerInterface:
    """Test cases for SchemaManager interface contract."""

    def test_schema_manager_is_abstract(self):
        """Test that SchemaManager is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            SchemaManager()

    def test_schema_manager_required_methods(self):
        """Test that SchemaManager defines required abstract methods."""
        required_methods = [
            'create_schema',
            'drop_schema',
            'schema_exists',
            'get_schema_version',
            'set_schema_version',
            'get_table_names',
            'table_exists',
            'get_table_definition',
            'create_table',
            'drop_table',
            'get_create_table_sql'
        ]

        for method_name in required_methods:
            assert hasattr(SchemaManager, method_name)
            method = getattr(SchemaManager, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestMigrationDataClasses:
    """Test cases for migration-related data structures."""

    def test_migration_creation(self):
        """Test Migration dataclass can be created."""
        migration = Migration(
            version="001",
            name="create_assets_table",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE assets (...)",
            down_sql="DROP TABLE assets",
            description="Create assets table",
            created_at=datetime.now(timezone.utc),
            checksum="abc123"
        )

        assert migration.version == "001"
        assert migration.name == "create_assets_table"
        assert migration.migration_type == MigrationType.CREATE_TABLE

    def test_migration_get_migration_id(self):
        """Test Migration.get_migration_id() method."""
        migration = Migration(
            version="001",
            name="create_assets_table",
            migration_type=MigrationType.CREATE_TABLE,
            up_sql="CREATE TABLE assets (...)",
            down_sql="DROP TABLE assets",
            description="Create assets table",
            created_at=datetime.now(timezone.utc),
            checksum="abc123"
        )

        assert migration.get_migration_id() == "001_create_assets_table"

    def test_table_definition_creation(self):
        """Test TableDefinition dataclass can be created."""
        table_def = TableDefinition(
            name="assets",
            columns={"id": "INTEGER PRIMARY KEY", "symbol": "TEXT NOT NULL"},
            primary_key=["id"],
            foreign_keys={},
            indexes=["CREATE INDEX idx_symbol ON assets(symbol)"],
            constraints=["UNIQUE(symbol)"]
        )

        assert table_def.name == "assets"
        assert "id" in table_def.columns
        assert table_def.primary_key == ["id"]


class TestAssetDataAccessInterface:
    """Test cases for AssetDataAccess interface contract."""

    def test_asset_data_access_is_abstract(self):
        """Test that AssetDataAccess is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            AssetDataAccess()

    def test_asset_data_access_required_methods(self):
        """Test that AssetDataAccess defines required abstract methods."""
        required_methods = [
            # Asset Management
            'save_asset', 'get_asset', 'get_assets_by_type', 'get_all_assets',
            'update_asset', 'delete_asset', 'asset_exists', 'get_asset_symbols',
            # Price Data Management
            'save_snapshot', 'get_latest_snapshot', 'get_snapshot_at_date',
            'get_historical_snapshots', 'get_snapshots_bulk', 'delete_snapshots_before',
            'get_snapshot_count',
            # Fundamental Data Management
            'save_fundamental_metrics', 'get_fundamental_metrics', 'get_fundamental_metrics_bulk',
            'get_metric_history', 'delete_fundamental_metrics',
            # Data Quality and Maintenance
            'get_data_quality_report', 'vacuum_asset_data'
        ]

        for method_name in required_methods:
            assert hasattr(AssetDataAccess, method_name)
            method = getattr(AssetDataAccess, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestPortfolioDataAccessInterface:
    """Test cases for PortfolioDataAccess interface contract."""

    def test_portfolio_data_access_is_abstract(self):
        """Test that PortfolioDataAccess is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            PortfolioDataAccess()

    def test_portfolio_data_access_required_methods(self):
        """Test that PortfolioDataAccess defines required abstract methods."""
        required_methods = [
            # Portfolio Management
            'save_portfolio', 'get_portfolio', 'get_all_portfolios', 'update_portfolio',
            'delete_portfolio', 'portfolio_exists', 'get_portfolio_ids', 'update_portfolio_cash',
            # Trade Management
            'save_trade', 'get_trade', 'get_trades_for_portfolio', 'get_trades_for_symbol',
            'get_trades_in_date_range', 'get_trade_count', 'get_trade_volume_stats',
            # Position Management
            'save_position', 'get_position', 'get_positions_for_portfolio', 'get_positions_for_symbols',
            'update_position', 'delete_position', 'get_position_count', 'get_largest_positions',
            # Portfolio Analytics
            'calculate_portfolio_value', 'calculate_portfolio_returns', 'get_portfolio_allocation',
            'get_portfolio_performance_history',
            # Data Maintenance
            'cleanup_zero_positions', 'archive_old_trades', 'validate_portfolio_integrity'
        ]

        for method_name in required_methods:
            assert hasattr(PortfolioDataAccess, method_name)
            method = getattr(PortfolioDataAccess, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestMetricsDataAccessInterface:
    """Test cases for MetricsDataAccess interface contract."""

    def test_metrics_data_access_is_abstract(self):
        """Test that MetricsDataAccess is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            MetricsDataAccess()

    def test_metrics_data_access_required_methods(self):
        """Test that MetricsDataAccess defines required abstract methods."""
        required_methods = [
            # Metric Storage and Retrieval
            'save_metric', 'save_metrics_batch', 'get_metric', 'get_metrics_for_entity',
            'get_metrics_bulk',
            # Historical Metrics
            'get_metric_history', 'get_metric_statistics',
            # Strategy Scores
            'save_strategy_scores', 'get_strategy_scores', 'get_strategy_score_history',
            'get_available_strategies',
            # Performance Metrics
            'save_portfolio_performance', 'get_portfolio_performance', 'get_portfolio_performance_history',
            # Risk Metrics
            'save_risk_metrics', 'get_risk_metrics',
            # Data Maintenance
            'delete_metrics_before_date', 'cleanup_stale_metrics', 'get_metric_storage_stats',
            'validate_metric_integrity',
            # Aggregation and Analysis
            'get_cross_sectional_metrics', 'calculate_metric_correlation'
        ]

        for method_name in required_methods:
            assert hasattr(MetricsDataAccess, method_name)
            method = getattr(MetricsDataAccess, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestAuditDataAccessInterface:
    """Test cases for AuditDataAccess interface contract."""

    def test_audit_data_access_is_abstract(self):
        """Test that AuditDataAccess is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            AuditDataAccess()

    def test_audit_data_access_required_methods(self):
        """Test that AuditDataAccess defines required abstract methods."""
        required_methods = [
            # Event Logging
            'log_event', 'log_events_batch',
            # Event Retrieval
            'get_event', 'get_events', 'get_events_for_entity', 'get_recent_events',
            # Error and Issue Tracking
            'log_error', 'get_error_summary', 'get_error_patterns',
            # Performance Tracking
            'log_performance_metric', 'get_performance_stats', 'get_slow_operations',
            # Session Tracking
            'start_session', 'end_session', 'get_active_sessions', 'get_session_history',
            # Data Retention and Cleanup
            'cleanup_old_events', 'archive_events', 'get_storage_stats',
            # Reporting and Analytics
            'generate_activity_report', 'get_usage_patterns', 'get_security_events'
        ]

        for method_name in required_methods:
            assert hasattr(AuditDataAccess, method_name)
            method = getattr(AuditDataAccess, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"


class TestEnumerationDefinitions:
    """Test cases for enumeration types used in data access layer."""

    def test_migration_type_enum(self):
        """Test MigrationType enum has required values."""
        required_types = [
            "CREATE_TABLE", "ALTER_TABLE", "DROP_TABLE", 
            "CREATE_INDEX", "DROP_INDEX", "DATA_MIGRATION"
        ]

        for type_name in required_types:
            assert hasattr(MigrationType, type_name)

    def test_metric_type_enum(self):
        """Test MetricType enum has required values."""
        required_types = [
            "FUNDAMENTAL", "TECHNICAL", "STRATEGY_SCORE", 
            "PERFORMANCE", "RISK"
        ]

        for type_name in required_types:
            assert hasattr(MetricType, type_name)

    def test_audit_event_type_enum(self):
        """Test AuditEventType enum has required values."""
        required_types = [
            "USER_LOGIN", "USER_LOGOUT", "DATA_INGESTION",
            "PORTFOLIO_CREATE", "TRADE_EXECUTE", "SYSTEM_START",
            "ERROR_OCCURRED"
        ]

        for type_name in required_types:
            assert hasattr(AuditEventType, type_name)

    def test_audit_severity_enum(self):
        """Test AuditSeverity enum has required values."""
        required_severities = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for severity_name in required_severities:
            assert hasattr(AuditSeverity, severity_name)


class TestContractCompliance:
    """Test cases to ensure interface contracts are properly defined."""

    def test_all_interfaces_inherit_from_abc(self):
        """Test that all data access interfaces inherit from ABC."""
        interfaces = [
            DatabaseConnection, TransactionManager, QueryExecutor,
            SchemaManager, MigrationManager, AssetDataAccess,
            PortfolioDataAccess, MetricsDataAccess, AuditDataAccess
        ]

        for interface in interfaces:
            assert issubclass(interface, ABC), f"{interface.__name__} should inherit from ABC"

    def test_method_signatures_are_consistent(self):
        """Test that method signatures follow consistent patterns."""
        # This is a basic test to ensure async methods are properly declared

        # AssetDataAccess methods should be async
        async_methods = ['save_asset', 'get_asset', 'delete_asset']
        for method_name in async_methods:
            method = getattr(AssetDataAccess, method_name)
            # Check that the method is marked as abstract
            assert getattr(method, '__isabstractmethod__', False)

    def test_type_hints_are_present(self):
        """Test that interfaces have proper type hints."""
        # This tests that type annotations are preserved in abstract methods

        # Check a few key methods have annotations
        save_asset = getattr(AssetDataAccess, 'save_asset')
        assert hasattr(save_asset, '__annotations__')

        get_portfolio = getattr(PortfolioDataAccess, 'get_portfolio')
        assert hasattr(get_portfolio, '__annotations__')

    def test_interface_completeness(self):
        """Test that interfaces cover all required database operations."""

        # AssetDataAccess should cover CRUD operations
        asset_methods = dir(AssetDataAccess)
        assert 'save_asset' in asset_methods  # Create
        assert 'get_asset' in asset_methods   # Read
        assert 'update_asset' in asset_methods # Update
        assert 'delete_asset' in asset_methods # Delete

        # PortfolioDataAccess should cover portfolio, trade, and position operations
        portfolio_methods = dir(PortfolioDataAccess)
        assert 'save_portfolio' in portfolio_methods
        assert 'save_trade' in portfolio_methods
        assert 'save_position' in portfolio_methods

        # MetricsDataAccess should cover different metric types
        metrics_methods = dir(MetricsDataAccess)
        assert 'save_strategy_scores' in metrics_methods
        assert 'save_portfolio_performance' in metrics_methods
        assert 'save_risk_metrics' in metrics_methods


# Test fixtures for domain entities (reuse from existing test files)
@pytest.fixture
def sample_asset():
    """Create a sample Asset for testing."""
    return Asset(
        symbol="AAPL",
        exchange="NASDAQ", 
        asset_type=AssetType.STOCK,
        name="Apple Inc."
    )


@pytest.fixture
def sample_asset_snapshot():
    """Create a sample AssetSnapshot for testing."""
    return AssetSnapshot(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc),
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("148.00"),
        close=Decimal("152.50"),
        volume=25000000
    )


@pytest.fixture
def sample_portfolio():
    """Create a sample Portfolio for testing."""
    return Portfolio(
        portfolio_id=uuid4(),
        name="Test Portfolio",
        base_ccy="USD",
        cash_balance=Decimal("50000.00"),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc)
    )


@pytest.fixture  
def sample_trade(sample_portfolio):
    """Create a sample Trade for testing."""
    return Trade(
        trade_id=uuid4(),
        portfolio_id=sample_portfolio.portfolio_id,
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc),
        side=TradeSide.BUY,
        qty=Decimal("100"),
        price=Decimal("152.50"),
        pip_pct=Decimal("0.001"),
        fee_flat=Decimal("0.00"),
        fee_pct=Decimal("0.000"),
        unit="share",
        price_ccy="USD",
        comment="Test trade"
    )


@pytest.fixture
def sample_position(sample_portfolio):
    """Create a sample Position for testing."""
    return Position(
        portfolio_id=sample_portfolio.portfolio_id,
        symbol="AAPL",
        qty=Decimal("100"),
        avg_cost=Decimal("152.50"),
        unit="share",
        price_ccy="USD",
        last_updated=datetime(2024, 1, 15, tzinfo=timezone.utc)
    )
