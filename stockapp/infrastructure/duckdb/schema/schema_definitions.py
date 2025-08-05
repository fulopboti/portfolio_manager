"""Centralized schema definitions for the StockApp database."""

from dataclasses import dataclass
from typing import Dict, List
from stockapp.infrastructure.data_access.schema_manager import TableDefinition


@dataclass(frozen=True)
class IndexDefinition:
    """Represents a database index definition."""
    name: str
    table: str
    columns: List[str]
    unique: bool = False
    where_clause: str = ""


@dataclass(frozen=True)
class ViewDefinition:
    """Represents a database view definition."""
    name: str
    sql: str
    description: str


class StockAppSchema:
    """Centralized schema definitions for the StockApp database.
    
    Contains all table, index, and view definitions optimized for DuckDB's
    columnar storage and analytical workloads.
    """
    
    SCHEMA_VERSION = "1.0.0"
    
    @classmethod
    def get_all_tables(cls) -> Dict[str, TableDefinition]:
        """Get all table definitions for the application schema."""
        return {
            "assets": cls.get_assets_table(),
            "asset_snapshots": cls.get_asset_snapshots_table(),
            "asset_metrics": cls.get_asset_metrics_table(),
            "portfolios": cls.get_portfolios_table(),
            "trades": cls.get_trades_table(),
            "positions": cls.get_positions_table(),
            "strategy_scores": cls.get_strategy_scores_table(),
            "portfolio_metrics": cls.get_portfolio_metrics_table(),
            "risk_metrics": cls.get_risk_metrics_table(),
            "audit_events": cls.get_audit_events_table(),
            "schema_migrations": cls.get_schema_migrations_table(),
        }
    
    @classmethod
    def get_all_indexes(cls) -> List[IndexDefinition]:
        """Get all index definitions for optimal query performance."""
        return [
            # Asset indexes
            IndexDefinition("idx_assets_type", "assets", ["asset_type"]),
            IndexDefinition("idx_assets_exchange", "assets", ["exchange"]),
            
            # Asset snapshot indexes (time-series optimized)
            IndexDefinition("idx_snapshots_symbol_time", "asset_snapshots", ["symbol", "timestamp"]),
            IndexDefinition("idx_snapshots_timestamp", "asset_snapshots", ["timestamp"]),
            
            # Asset metrics indexes
            IndexDefinition("idx_metrics_symbol_type", "asset_metrics", ["symbol", "metric_type"]),
            IndexDefinition("idx_metrics_date", "asset_metrics", ["as_of_date"]),
            
            # Portfolio indexes
            IndexDefinition("idx_portfolios_currency", "portfolios", ["base_ccy"]),
            
            # Trades indexes (optimized for portfolio queries)
            IndexDefinition("idx_trades_portfolio_time", "trades", ["portfolio_id", "timestamp"]),
            IndexDefinition("idx_trades_symbol", "trades", ["symbol"]),
            IndexDefinition("idx_trades_timestamp", "trades", ["timestamp"]),
            
            # Position indexes
            IndexDefinition("idx_positions_portfolio", "positions", ["portfolio_id"]),
            IndexDefinition("idx_positions_symbol", "positions", ["symbol"]),
            
            # Strategy scores indexes
            IndexDefinition("idx_scores_strategy_date", "strategy_scores", ["strategy_name", "as_of_date"]),
            IndexDefinition("idx_scores_symbol", "strategy_scores", ["symbol"]),
            
            # Portfolio metrics indexes
            IndexDefinition("idx_portfolio_metrics_id_date", "portfolio_metrics", ["portfolio_id", "as_of_date"]),
            
            # Risk metrics indexes
            IndexDefinition("idx_risk_metrics_entity", "risk_metrics", ["entity_id", "entity_type"]),
            
            # Audit event indexes
            IndexDefinition("idx_audit_timestamp", "audit_events", ["timestamp"]),
            IndexDefinition("idx_audit_entity", "audit_events", ["entity_id"]),
            IndexDefinition("idx_audit_type_severity", "audit_events", ["event_type", "severity"]),
            
            # Migration tracking
            IndexDefinition("idx_migrations_version", "schema_migrations", ["version"], unique=True),
        ]
    
    @classmethod
    def get_all_views(cls) -> List[ViewDefinition]:
        """Get all view definitions for common analytical queries."""
        return [
            ViewDefinition(
                name="portfolio_summary",
                sql="""
                SELECT 
                    p.portfolio_id,
                    p.name,
                    p.base_ccy,
                    p.cash_balance,
                    COUNT(pos.symbol) as position_count,
                    SUM(pos.qty * pos.avg_cost) as invested_value,
                    p.cash_balance + COALESCE(SUM(pos.qty * pos.avg_cost), 0) as total_value
                FROM portfolios p
                LEFT JOIN positions pos ON p.portfolio_id = pos.portfolio_id
                WHERE pos.qty > 0 OR pos.qty IS NULL
                GROUP BY p.portfolio_id, p.name, p.base_ccy, p.cash_balance
                """,
                description="Summary view of all portfolios with position counts and values"
            ),
            
            ViewDefinition(
                name="latest_asset_prices",
                sql="""
                SELECT DISTINCT
                    symbol,
                    LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY timestamp) as latest_price,
                    LAST_VALUE(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as price_timestamp
                FROM asset_snapshots
                """,
                description="Latest price for each asset"
            ),
            
            ViewDefinition(
                name="daily_portfolio_performance",
                sql="""
                SELECT 
                    portfolio_id,
                    DATE(timestamp) as date,
                    SUM(CASE WHEN side = 'BUY' THEN qty * price ELSE -qty * price END) as net_flow,
                    COUNT(*) as trade_count
                FROM trades
                GROUP BY portfolio_id, DATE(timestamp)
                ORDER BY portfolio_id, date
                """,
                description="Daily trading activity and cash flows by portfolio"
            ),
        ]
    
    @classmethod
    def get_assets_table(cls) -> TableDefinition:
        """Asset master table definition."""
        return TableDefinition(
            name="assets",
            columns={
                "symbol": "VARCHAR PRIMARY KEY",
                "exchange": "VARCHAR NOT NULL",
                "asset_type": "VARCHAR NOT NULL CHECK (asset_type IN ('STOCK', 'ETF', 'CRYPTO', 'COMMODITY'))",
                "name": "VARCHAR NOT NULL",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["symbol"],
            foreign_keys={},
            indexes=[],
            constraints=[
                "CHECK (length(symbol) > 0)",
                "CHECK (length(exchange) > 0)",
                "CHECK (length(name) > 0)"
            ]
        )
    
    @classmethod
    def get_asset_snapshots_table(cls) -> TableDefinition:
        """Asset price snapshot table optimized for time-series data."""
        return TableDefinition(
            name="asset_snapshots",
            columns={
                "symbol": "VARCHAR NOT NULL",
                "timestamp": "TIMESTAMP NOT NULL",
                "open": "DECIMAL(18,6) NOT NULL CHECK (open > 0)",
                "high": "DECIMAL(18,6) NOT NULL CHECK (high > 0)",
                "low": "DECIMAL(18,6) NOT NULL CHECK (low > 0)",
                "close": "DECIMAL(18,6) NOT NULL CHECK (close > 0)",
                "volume": "BIGINT NOT NULL CHECK (volume >= 0)",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["symbol", "timestamp"],
            foreign_keys={
                "symbol": "assets.symbol"
            },
            indexes=[],
            constraints=[
                "CHECK (high >= low)",
                "CHECK (high >= open)",
                "CHECK (high >= close)",
                "CHECK (low <= open)",
                "CHECK (low <= close)"
            ]
        )
    
    @classmethod
    def get_asset_metrics_table(cls) -> TableDefinition:
        """Asset fundamental and technical metrics table."""
        return TableDefinition(
            name="asset_metrics",
            columns={
                "symbol": "VARCHAR NOT NULL",
                "metric_name": "VARCHAR NOT NULL",
                "metric_type": "VARCHAR NOT NULL CHECK (metric_type IN ('FUNDAMENTAL', 'TECHNICAL', 'STRATEGY_SCORE', 'PERFORMANCE', 'RISK'))",
                "value": "DECIMAL(18,6) NOT NULL",
                "as_of_date": "TIMESTAMP NOT NULL",
                "metadata": "JSON",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["symbol", "metric_name", "as_of_date"],
            foreign_keys={
                "symbol": "assets.symbol"
            },
            indexes=[],
            constraints=[
                "CHECK (length(metric_name) > 0)"
            ]
        )
    
    @classmethod
    def get_portfolios_table(cls) -> TableDefinition:
        """Portfolio master table definition."""
        return TableDefinition(
            name="portfolios",
            columns={
                "portfolio_id": "UUID PRIMARY KEY",
                "name": "VARCHAR NOT NULL",
                "base_ccy": "VARCHAR NOT NULL CHECK (base_ccy IN ('USD', 'EUR', 'RON'))",
                "cash_balance": "DECIMAL(18,2) NOT NULL CHECK (cash_balance >= 0)",
                "created_at": "TIMESTAMP NOT NULL",
                "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["portfolio_id"],
            foreign_keys={},
            indexes=[],
            constraints=[
                "CHECK (length(name) > 0)",
                "UNIQUE (name)"
            ]
        )
    
    @classmethod
    def get_trades_table(cls) -> TableDefinition:
        """Trade execution log table."""
        return TableDefinition(
            name="trades",
            columns={
                "trade_id": "UUID PRIMARY KEY",
                "portfolio_id": "UUID NOT NULL",
                "symbol": "VARCHAR NOT NULL",
                "timestamp": "TIMESTAMP NOT NULL",
                "side": "VARCHAR NOT NULL CHECK (side IN ('BUY', 'SELL'))",
                "qty": "DECIMAL(18,6) NOT NULL CHECK (qty > 0)",
                "price": "DECIMAL(18,6) NOT NULL CHECK (price > 0)",
                "pip_pct": "DECIMAL(8,6) NOT NULL CHECK (pip_pct >= 0)",
                "fee_flat": "DECIMAL(18,2) NOT NULL CHECK (fee_flat >= 0)",
                "fee_pct": "DECIMAL(8,6) NOT NULL CHECK (fee_pct >= 0)",
                "unit": "VARCHAR NOT NULL DEFAULT 'share'",
                "price_ccy": "VARCHAR NOT NULL",
                "comment": "TEXT",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["trade_id"],
            foreign_keys={
                "portfolio_id": "portfolios.portfolio_id",
                "symbol": "assets.symbol"
            },
            indexes=[],
            constraints=[
                "CHECK (length(unit) > 0)",
                "CHECK (length(price_ccy) > 0)"
            ]
        )
    
    @classmethod
    def get_positions_table(cls) -> TableDefinition:
        """Current portfolio positions table."""
        return TableDefinition(
            name="positions",
            columns={
                "portfolio_id": "UUID NOT NULL",
                "symbol": "VARCHAR NOT NULL",
                "qty": "DECIMAL(18,6) NOT NULL",
                "avg_cost": "DECIMAL(18,6) NOT NULL CHECK (avg_cost > 0)",
                "unit": "VARCHAR NOT NULL DEFAULT 'share'",
                "price_ccy": "VARCHAR NOT NULL",
                "last_updated": "TIMESTAMP NOT NULL",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["portfolio_id", "symbol"],
            foreign_keys={
                "portfolio_id": "portfolios.portfolio_id",
                "symbol": "assets.symbol"
            },
            indexes=[],
            constraints=[
                "CHECK (length(unit) > 0)",
                "CHECK (length(price_ccy) > 0)"
            ]
        )
    
    @classmethod
    def get_strategy_scores_table(cls) -> TableDefinition:
        """Strategy ranking scores table."""
        return TableDefinition(
            name="strategy_scores",
            columns={
                "strategy_name": "VARCHAR NOT NULL",
                "symbol": "VARCHAR NOT NULL", 
                "score": "DECIMAL(8,2) NOT NULL CHECK (score >= 0 AND score <= 100)",
                "as_of_date": "TIMESTAMP NOT NULL",
                "metadata": "JSON",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["strategy_name", "symbol", "as_of_date"],
            foreign_keys={
                "symbol": "assets.symbol"
            },
            indexes=[],
            constraints=[
                "CHECK (length(strategy_name) > 0)"
            ]
        )
    
    @classmethod
    def get_portfolio_metrics_table(cls) -> TableDefinition:
        """Portfolio performance metrics table."""
        return TableDefinition(
            name="portfolio_metrics",
            columns={
                "portfolio_id": "UUID NOT NULL",
                "metric_name": "VARCHAR NOT NULL",
                "value": "DECIMAL(18,6) NOT NULL",
                "as_of_date": "TIMESTAMP NOT NULL",
                "metadata": "JSON",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["portfolio_id", "metric_name", "as_of_date"],
            foreign_keys={
                "portfolio_id": "portfolios.portfolio_id"
            },
            indexes=[],
            constraints=[
                "CHECK (length(metric_name) > 0)"
            ]
        )
    
    @classmethod
    def get_risk_metrics_table(cls) -> TableDefinition:
        """Risk metrics table for assets and portfolios."""
        return TableDefinition(
            name="risk_metrics",
            columns={
                "entity_id": "VARCHAR NOT NULL",
                "entity_type": "VARCHAR NOT NULL CHECK (entity_type IN ('ASSET', 'PORTFOLIO'))",
                "metric_name": "VARCHAR NOT NULL",
                "value": "DECIMAL(18,6) NOT NULL",
                "as_of_date": "TIMESTAMP NOT NULL",
                "metadata": "JSON",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["entity_id", "entity_type", "metric_name", "as_of_date"],
            foreign_keys={},
            indexes=[],
            constraints=[
                "CHECK (length(entity_id) > 0)",
                "CHECK (length(metric_name) > 0)"
            ]
        )
    
    @classmethod
    def get_audit_events_table(cls) -> TableDefinition:
        """Audit event log table."""
        return TableDefinition(
            name="audit_events",
            columns={
                "event_id": "UUID PRIMARY KEY",
                "event_type": "VARCHAR NOT NULL",
                "severity": "VARCHAR NOT NULL CHECK (severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))",
                "message": "TEXT NOT NULL",
                "entity_id": "VARCHAR",
                "user_id": "VARCHAR",
                "session_id": "VARCHAR",
                "timestamp": "TIMESTAMP NOT NULL",
                "details": "JSON",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key=["event_id"],
            foreign_keys={},
            indexes=[],
            constraints=[
                "CHECK (length(event_type) > 0)",
                "CHECK (length(message) > 0)"
            ]
        )
    
    @classmethod
    def get_schema_migrations_table(cls) -> TableDefinition:
        """Schema migration tracking table."""
        return TableDefinition(
            name="schema_migrations",
            columns={
                "version": "VARCHAR PRIMARY KEY",
                "name": "VARCHAR NOT NULL",
                "migration_type": "VARCHAR NOT NULL",
                "applied_at": "TIMESTAMP NOT NULL",
                "checksum": "VARCHAR NOT NULL",
                "execution_time_ms": "INTEGER",
                "success": "BOOLEAN NOT NULL DEFAULT TRUE"
            },
            primary_key=["version"],
            foreign_keys={},
            indexes=[],
            constraints=[
                "CHECK (length(name) > 0)",
                "CHECK (length(checksum) > 0)",
                "CHECK (execution_time_ms >= 0)"
            ]
        )