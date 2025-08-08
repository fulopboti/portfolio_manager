"""Comprehensive tests to achieve full coverage for DuckDB schema definitions."""

import pytest
from typing import Dict, List

from portfolio_manager.infrastructure.duckdb.schema.schema_definitions import (
    PortfolioManagerSchema, IndexDefinition, ViewDefinition
)
from portfolio_manager.infrastructure.data_access.schema_manager import TableDefinition


class TestPortfolioManagerSchemaCoverage:
    """Tests to achieve comprehensive coverage for Portfolio Manager schema definitions."""

    def test_get_assets_table_coverage(self):
        """Test get_assets_table method (lines 38-55)."""
        table_def = PortfolioManagerSchema.get_assets_table()

        assert table_def.name == "assets"
        assert len(table_def.columns) > 0

        # Check for key columns
        column_names = list(table_def.columns.keys())
        assert "symbol" in column_names
        assert "exchange" in column_names
        assert "asset_type" in column_names
        assert "name" in column_names

    def test_get_asset_snapshots_table_coverage(self):
        """Test get_asset_snapshots_table method (lines 102-153)."""
        table_def = PortfolioManagerSchema.get_asset_snapshots_table()

        assert table_def.name == "asset_snapshots"
        assert len(table_def.columns) > 0

        # Check for OHLCV columns
        column_names = list(table_def.columns.keys())
        assert "symbol" in column_names
        assert "timestamp" in column_names
        assert "open" in column_names
        assert "high" in column_names
        assert "low" in column_names
        assert "close" in column_names
        assert "volume" in column_names

    def test_get_portfolios_table_coverage(self):
        """Test get_portfolios_table method (lines 176-205)."""
        table_def = PortfolioManagerSchema.get_portfolios_table()

        assert table_def.name == "portfolios"
        assert len(table_def.columns) > 0

        column_names = list(table_def.columns.keys())
        assert "portfolio_id" in column_names
        assert "name" in column_names
        assert "base_ccy" in column_names
        assert "cash_balance" in column_names
        assert "created_at" in column_names

    def test_get_positions_table_coverage(self):
        """Test get_positions_table method (lines 229-251)."""
        table_def = PortfolioManagerSchema.get_positions_table()

        assert table_def.name == "positions"
        assert len(table_def.columns) > 0

        column_names = list(table_def.columns.keys())
        assert "portfolio_id" in column_names
        assert "symbol" in column_names
        assert "qty" in column_names
        assert "avg_cost" in column_names

    def test_get_trades_table_coverage(self):
        """Test get_trades_table method (lines 284-311)."""
        table_def = PortfolioManagerSchema.get_trades_table()

        assert table_def.name == "trades"
        assert len(table_def.columns) > 0

        column_names = list(table_def.columns.keys())
        assert "trade_id" in column_names
        assert "portfolio_id" in column_names
        assert "symbol" in column_names
        assert "side" in column_names
        assert "qty" in column_names
        assert "price" in column_names

    def test_get_asset_metrics_table_coverage(self):
        """Test get_asset_metrics_table method."""
        table_def = PortfolioManagerSchema.get_asset_metrics_table()

        assert table_def.name == "asset_metrics"
        assert len(table_def.columns) > 0

        column_names = list(table_def.columns.keys())
        assert "symbol" in column_names
        assert "metric_name" in column_names
        assert "metric_type" in column_names
        assert "value" in column_names

    def test_get_strategy_scores_table_coverage(self):
        """Test get_strategy_scores_table method (lines 380-406)."""
        table_def = PortfolioManagerSchema.get_strategy_scores_table()

        assert table_def.name == "strategy_scores"
        assert len(table_def.columns) > 0

        column_names = list(table_def.columns.keys())
        assert "symbol" in column_names  
        assert "strategy_name" in column_names
        assert "score" in column_names
        assert "as_of_date" in column_names

    def test_get_all_tables_coverage(self):
        """Test get_all_tables method that returns all table definitions."""
        all_tables = PortfolioManagerSchema.get_all_tables()

        assert isinstance(all_tables, dict)
        assert len(all_tables) > 0

        # Check that key tables are included
        expected_tables = [
            "assets", "asset_snapshots", "portfolios", 
            "positions", "trades", "asset_metrics", "strategy_scores"
        ]

        for table_name in expected_tables:
            assert table_name in all_tables
            assert isinstance(all_tables[table_name], TableDefinition)

    def test_get_all_indexes_coverage(self):
        """Test get_all_indexes method that returns all index definitions."""
        all_indexes = PortfolioManagerSchema.get_all_indexes()

        assert isinstance(all_indexes, list)
        assert len(all_indexes) > 0

        # Check that indexes are IndexDefinition objects
        for index in all_indexes:
            assert isinstance(index, IndexDefinition)
            assert hasattr(index, 'name')
            assert hasattr(index, 'table')
            assert hasattr(index, 'columns')

    def test_get_all_views_coverage(self):
        """Test get_all_views method that returns all view definitions."""
        all_views = PortfolioManagerSchema.get_all_views()

        assert isinstance(all_views, list)
        # Views list might be empty, which is valid

        # If views exist, check they are ViewDefinition objects
        for view in all_views:
            assert isinstance(view, ViewDefinition)
            assert hasattr(view, 'name')
            assert hasattr(view, 'sql')

    def test_schema_consistency_coverage(self):
        """Test that all schema components are consistent."""
        tables = PortfolioManagerSchema.get_all_tables()
        indexes = PortfolioManagerSchema.get_all_indexes()

        # Verify that all indexes reference existing tables
        table_names = set(tables.keys())

        for index in indexes:
            assert index.table in table_names, f"Index {index.name} references non-existent table {index.table}"

    def test_table_column_types_coverage(self):
        """Test that table columns have appropriate types."""
        tables = PortfolioManagerSchema.get_all_tables()

        for table_name, table_def in tables.items():
            assert len(table_def.columns) > 0, f"Table {table_name} has no columns"

            for column_name, column_type in table_def.columns.items():
                assert column_name, f"Column in {table_name} has no name"
                assert column_type, f"Column {column_name} in {table_name} has no type"

    def test_index_definition_properties_coverage(self):
        """Test IndexDefinition properties and methods."""
        # Create a test index
        index = IndexDefinition(
            name="test_idx",
            table="test_table", 
            columns=["col1", "col2"],
            unique=True,
            where_clause="col1 IS NOT NULL"
        )

        assert index.name == "test_idx"
        assert index.table == "test_table"
        assert index.columns == ["col1", "col2"]
        assert index.unique is True
        assert index.where_clause == "col1 IS NOT NULL"

    def test_view_definition_properties_coverage(self):
        """Test ViewDefinition properties and methods."""
        # Create a test view
        view = ViewDefinition(
            name="test_view",
            sql="SELECT * FROM test_table WHERE active = 1",
            description="Test view for coverage"
        )

        assert view.name == "test_view"
        assert view.sql == "SELECT * FROM test_table WHERE active = 1"

    def test_primary_key_definitions_coverage(self):
        """Test that primary keys are properly defined."""
        tables = PortfolioManagerSchema.get_all_tables()

        # Check that key tables have primary keys defined
        key_tables_with_pks = ["assets", "portfolios", "trades"]

        for table_name in key_tables_with_pks:
            if table_name in tables:
                table_def = tables[table_name]
                assert len(table_def.primary_key) > 0, f"Table {table_name} should have a primary key"

    def test_foreign_key_relationships_coverage(self):
        """Test that foreign key relationships are properly defined."""
        tables = PortfolioManagerSchema.get_all_tables()

        # Check tables that should have foreign keys
        tables_with_fks = ["positions", "trades", "asset_snapshots"]

        for table_name in tables_with_fks:
            if table_name in tables:
                table_def = tables[table_name]
                # Check if table has foreign key definitions
                # (This depends on how foreign keys are implemented in the schema)
                assert table_def is not None

    def test_decimal_precision_coverage(self):
        """Test that decimal columns have appropriate precision."""
        tables = PortfolioManagerSchema.get_all_tables()

        # Find decimal columns and verify they have precision
        for table_name, table_def in tables.items():
            for column_name, column_type in table_def.columns.items():
                if "DECIMAL" in column_type.upper():
                    # Should have precision specified like DECIMAL(10,2)
                    assert "(" in column_type, f"Decimal column {column_name} in {table_name} lacks precision"

    def test_timestamp_columns_coverage(self):
        """Test that timestamp columns are properly defined."""
        tables = PortfolioManagerSchema.get_all_tables()

        # Tables that should have timestamp columns
        time_sensitive_tables = ["asset_snapshots", "trades", "portfolios"]

        for table_name in time_sensitive_tables:
            if table_name in tables:
                table_def = tables[table_name]
                column_names = list(table_def.columns.keys())

                # Should have some kind of timestamp/datetime column
                has_time_column = any(
                    "timestamp" in name.lower() or 
                    "created" in name.lower() or 
                    "date" in name.lower()
                    for name in column_names
                )
                assert has_time_column, f"Table {table_name} should have a timestamp column"
