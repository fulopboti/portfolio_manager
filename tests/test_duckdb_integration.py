"""Integration tests for DuckDB repository implementations."""

import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)
from stockapp.infrastructure.database import DatabaseConnection, SchemaManager
from stockapp.infrastructure.db.duckdb_connection import DuckDBConnection, DuckDBSchemaManager
from stockapp.infrastructure.db.duckdb_repositories import (
    DuckDBAssetRepository,
    DuckDBPortfolioRepository,
)


@pytest.fixture
async def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
async def db_connection(temp_db_path):
    """Create a DuckDB connection for testing."""
    connection = DuckDBConnection(temp_db_path)
    await connection.connect()
    yield connection
    await connection.disconnect()


@pytest.fixture
async def schema_manager(db_connection):
    """Create a schema manager for testing."""
    manager = DuckDBSchemaManager(db_connection)
    await manager.create_schema()
    yield manager
    await manager.drop_schema()


@pytest.fixture
async def asset_repository(db_connection, schema_manager):
    """Create an asset repository for testing."""
    return DuckDBAssetRepository(db_connection)


@pytest.fixture
async def portfolio_repository(db_connection, schema_manager):
    """Create a portfolio repository for testing."""
    return DuckDBPortfolioRepository(db_connection)


class TestDuckDBConnection:
    """Test DuckDB connection implementation."""

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, temp_db_path):
        """Test database connection lifecycle."""
        connection = DuckDBConnection(temp_db_path)
        
        # Test connection
        await connection.connect()
        assert connection.is_connected()
        
        # Test simple query
        result = await connection.fetch_one("SELECT 1 as test")
        assert result["test"] == 1
        
        # Test disconnection
        await connection.disconnect()
        assert not connection.is_connected()

    @pytest.mark.asyncio
    async def test_transaction_commit(self, db_connection):
        """Test transaction commit functionality."""
        await db_connection.execute("""
            CREATE TABLE test_transaction (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)
        
        async with db_connection.transaction():
            await db_connection.execute(
                "INSERT INTO test_transaction (id, value) VALUES (?, ?)",
                {"id": 1, "value": "test"}
            )
        
        # Verify data was committed
        result = await db_connection.fetch_one("SELECT * FROM test_transaction WHERE id = 1")
        assert result is not None
        assert result["value"] == "test"

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, db_connection):
        """Test transaction rollback functionality."""
        await db_connection.execute("""
            CREATE TABLE test_rollback (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)
        
        try:
            async with db_connection.transaction():
                await db_connection.execute(
                    "INSERT INTO test_rollback (id, value) VALUES (?, ?)",
                    {"id": 1, "value": "test"}
                )
                # Force an error to trigger rollback
                raise Exception("Test rollback")
        except Exception:
            pass
        
        # Verify data was rolled back
        result = await db_connection.fetch_one("SELECT * FROM test_rollback WHERE id = 1")
        assert result is None

    @pytest.mark.asyncio
    async def test_execute_many(self, db_connection):
        """Test batch execution functionality."""
        await db_connection.execute("""
            CREATE TABLE test_batch (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)
        
        parameters_list = [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"},
            {"id": 3, "value": "test3"},
        ]
        
        await db_connection.execute_many(
            "INSERT INTO test_batch (id, value) VALUES (?, ?)",
            parameters_list
        )
        
        results = await db_connection.fetch_all("SELECT * FROM test_batch ORDER BY id")
        assert len(results) == 3
        assert results[0]["value"] == "test1"
        assert results[2]["value"] == "test3"


class TestDuckDBSchemaManager:
    """Test DuckDB schema management."""

    @pytest.mark.asyncio
    async def test_schema_creation(self, db_connection):
        """Test database schema creation."""
        schema_manager = DuckDBSchemaManager(db_connection)
        
        # Create schema
        await schema_manager.create_schema()
        
        # Verify tables exist
        tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """
        tables = await db_connection.fetch_all(tables_query)
        table_names = {table["table_name"] for table in tables}
        
        expected_tables = {
            "assets", "asset_snapshots", "asset_metrics",
            "portfolios", "trades", "positions"
        }
        assert expected_tables.issubset(table_names)

    @pytest.mark.asyncio
    async def test_schema_version_management(self, db_connection):
        """Test schema version tracking."""
        schema_manager = DuckDBSchemaManager(db_connection)
        await schema_manager.create_schema()
        
        # Test version retrieval
        version = await schema_manager.get_schema_version()
        assert version is not None
        assert isinstance(version, str)

    @pytest.mark.asyncio
    async def test_get_create_table_sql(self, db_connection):
        """Test SQL generation for table creation."""
        schema_manager = DuckDBSchemaManager(db_connection)
        
        sql_statements = schema_manager.get_create_table_sql()
        
        # Verify all required tables have SQL
        required_tables = ["assets", "asset_snapshots", "portfolios", "trades", "positions"]
        for table in required_tables:
            assert table in sql_statements
            assert "CREATE TABLE" in sql_statements[table]


class TestDuckDBAssetRepository:
    """Test DuckDB asset repository implementation."""

    @pytest.mark.asyncio
    async def test_save_and_get_asset(self, asset_repository):
        """Test saving and retrieving assets."""
        asset = Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )
        
        # Save asset
        await asset_repository.save_asset(asset)
        
        # Retrieve asset
        retrieved = await asset_repository.get_asset("AAPL")
        
        assert retrieved is not None
        assert retrieved.symbol == asset.symbol
        assert retrieved.exchange == asset.exchange
        assert retrieved.asset_type == asset.asset_type
        assert retrieved.name == asset.name

    @pytest.mark.asyncio
    async def test_asset_exists(self, asset_repository):
        """Test asset existence checking."""
        asset = Asset(
            symbol="MSFT",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Microsoft Corporation"
        )
        
        # Should not exist initially
        assert not await asset_repository.asset_exists("MSFT")
        
        # Save asset
        await asset_repository.save_asset(asset)
        
        # Should exist now
        assert await asset_repository.asset_exists("MSFT")

    @pytest.mark.asyncio
    async def test_get_all_assets(self, asset_repository):
        """Test retrieving all assets."""
        assets = [
            Asset("AAPL", "NASDAQ", AssetType.STOCK, "Apple Inc."),
            Asset("SPY", "NYSE", AssetType.ETF, "SPDR S&P 500 ETF"),
            Asset("BTC-USD", "CRYPTO", AssetType.CRYPTO, "Bitcoin"),
        ]
        
        # Save all assets
        for asset in assets:
            await asset_repository.save_asset(asset)
        
        # Retrieve all assets
        all_assets = await asset_repository.get_all_assets()
        assert len(all_assets) == 3
        
        # Retrieve by type
        stocks = await asset_repository.get_all_assets(AssetType.STOCK)
        assert len(stocks) == 1
        assert stocks[0].symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, asset_repository):
        """Test saving and retrieving asset snapshots."""
        # First save an asset
        asset = Asset("GOOGL", "NASDAQ", AssetType.STOCK, "Alphabet Inc.")
        await asset_repository.save_asset(asset)
        
        # Create and save snapshot
        snapshot = AssetSnapshot(
            symbol="GOOGL",
            timestamp=datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc),
            open=Decimal("140.00"),
            high=Decimal("145.00"),
            low=Decimal("138.00"),
            close=Decimal("142.50"),
            volume=25000000
        )
        
        await asset_repository.save_snapshot(snapshot)
        
        # Retrieve latest snapshot
        latest = await asset_repository.get_latest_snapshot("GOOGL")
        
        assert latest is not None
        assert latest.symbol == snapshot.symbol
        assert latest.close == snapshot.close
        assert latest.volume == snapshot.volume

    @pytest.mark.asyncio
    async def test_get_historical_snapshots(self, asset_repository):
        """Test retrieving historical snapshots."""
        # First save an asset
        asset = Asset("TSLA", "NASDAQ", AssetType.STOCK, "Tesla Inc.")
        await asset_repository.save_asset(asset)
        
        # Create multiple snapshots
        snapshots = [
            AssetSnapshot(
                symbol="TSLA",
                timestamp=datetime(2024, 1, 10, 16, 0, tzinfo=timezone.utc),
                open=Decimal("200.00"),
                high=Decimal("205.00"),
                low=Decimal("198.00"),
                close=Decimal("202.00"),
                volume=10000000
            ),
            AssetSnapshot(
                symbol="TSLA",
                timestamp=datetime(2024, 1, 15, 16, 0, tzinfo=timezone.utc),
                open=Decimal("202.00"),
                high=Decimal("210.00"),
                low=Decimal("200.00"),
                close=Decimal("208.00"),
                volume=12000000
            ),
            AssetSnapshot(
                symbol="TSLA",
                timestamp=datetime(2024, 1, 20, 16, 0, tzinfo=timezone.utc),
                open=Decimal("208.00"),
                high=Decimal("215.00"),
                low=Decimal("205.00"),
                close=Decimal("212.00"),
                volume=8000000
            ),
        ]
        
        # Save all snapshots
        for snapshot in snapshots:
            await asset_repository.save_snapshot(snapshot)
        
        # Retrieve historical data
        historical = await asset_repository.get_historical_snapshots(
            "TSLA",
            start_date=datetime(2024, 1, 12, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 18, tzinfo=timezone.utc)
        )
        
        assert len(historical) == 1  # Only middle snapshot in range
        assert historical[0].close == Decimal("208.00")

    @pytest.mark.asyncio
    async def test_fundamental_metrics(self, asset_repository):
        """Test saving and retrieving fundamental metrics."""
        # First save an asset
        asset = Asset("NVDA", "NASDAQ", AssetType.STOCK, "NVIDIA Corporation")
        await asset_repository.save_asset(asset)
        
        # Save fundamental metrics
        metrics = {
            "pe_ratio": Decimal("65.5"),
            "peg_ratio": Decimal("1.2"),
            "dividend_yield": Decimal("0.008"),
            "revenue_growth": Decimal("0.35"),
            "fcf_growth": Decimal("0.28"),
            "debt_equity": Decimal("0.15")
        }
        
        await asset_repository.save_fundamental_metrics("NVDA", metrics)
        
        # Retrieve metrics
        retrieved_metrics = await asset_repository.get_fundamental_metrics("NVDA")
        
        assert retrieved_metrics is not None
        assert retrieved_metrics["pe_ratio"] == Decimal("65.5")
        assert retrieved_metrics["dividend_yield"] == Decimal("0.008")

    @pytest.mark.asyncio
    async def test_delete_asset(self, asset_repository):
        """Test asset deletion."""
        # Create and save asset with snapshot
        asset = Asset("DELETE_ME", "NYSE", AssetType.STOCK, "Test Delete")
        await asset_repository.save_asset(asset)
        
        snapshot = AssetSnapshot(
            symbol="DELETE_ME",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("99.00"),
            close=Decimal("102.00"),
            volume=1000000
        )
        await asset_repository.save_snapshot(snapshot)
        
        # Verify asset exists
        assert await asset_repository.asset_exists("DELETE_ME")
        assert await asset_repository.get_latest_snapshot("DELETE_ME") is not None
        
        # Delete asset
        await asset_repository.delete_asset("DELETE_ME")
        
        # Verify asset and related data are deleted
        assert not await asset_repository.asset_exists("DELETE_ME")
        assert await asset_repository.get_latest_snapshot("DELETE_ME") is None


class TestDuckDBPortfolioRepository:
    """Test DuckDB portfolio repository implementation."""

    @pytest.mark.asyncio
    async def test_save_and_get_portfolio(self, portfolio_repository):
        """Test saving and retrieving portfolios."""
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("50000.00"),
            created=datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
        
        # Save portfolio
        await portfolio_repository.save_portfolio(portfolio)
        
        # Retrieve portfolio
        retrieved = await portfolio_repository.get_portfolio(portfolio.portfolio_id)
        
        assert retrieved is not None
        assert retrieved.portfolio_id == portfolio.portfolio_id
        assert retrieved.name == portfolio.name
        assert retrieved.base_ccy == portfolio.base_ccy
        assert retrieved.cash_balance == portfolio.cash_balance

    @pytest.mark.asyncio
    async def test_portfolio_exists(self, portfolio_repository):
        """Test portfolio existence checking."""
        portfolio_id = uuid4()
        
        # Should not exist initially
        assert not await portfolio_repository.portfolio_exists(portfolio_id)
        
        # Create and save portfolio
        portfolio = Portfolio(
            portfolio_id=portfolio_id,
            name="Existence Test",
            base_ccy="EUR",
            cash_balance=Decimal("25000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Should exist now
        assert await portfolio_repository.portfolio_exists(portfolio_id)

    @pytest.mark.asyncio
    async def test_get_all_portfolios(self, portfolio_repository):
        """Test retrieving all portfolios."""
        portfolios = [
            Portfolio(
                portfolio_id=uuid4(),
                name="Growth Portfolio",
                base_ccy="USD",
                cash_balance=Decimal("100000.00"),
                created=datetime.now(timezone.utc)
            ),
            Portfolio(
                portfolio_id=uuid4(),
                name="Conservative Portfolio",
                base_ccy="USD",
                cash_balance=Decimal("75000.00"),
                created=datetime.now(timezone.utc)
            ),
        ]
        
        # Save all portfolios
        for portfolio in portfolios:
            await portfolio_repository.save_portfolio(portfolio)
        
        # Retrieve all portfolios
        all_portfolios = await portfolio_repository.get_all_portfolios()
        assert len(all_portfolios) == 2
        
        names = {p.name for p in all_portfolios}
        assert "Growth Portfolio" in names
        assert "Conservative Portfolio" in names

    @pytest.mark.asyncio
    async def test_save_and_get_trade(self, portfolio_repository):
        """Test saving and retrieving trades."""
        # First create a portfolio
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Trade Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("50000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Create and save trade
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=portfolio.portfolio_id,
            symbol="AAPL",
            timestamp=datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Test trade"
        )
        
        await portfolio_repository.save_trade(trade)
        
        # Retrieve trade
        retrieved = await portfolio_repository.get_trade(trade.trade_id)
        
        assert retrieved is not None
        assert retrieved.trade_id == trade.trade_id
        assert retrieved.portfolio_id == trade.portfolio_id
        assert retrieved.symbol == trade.symbol
        assert retrieved.side == trade.side
        assert retrieved.qty == trade.qty

    @pytest.mark.asyncio
    async def test_get_trades_for_portfolio(self, portfolio_repository):
        """Test retrieving trades for a portfolio."""
        # Create portfolio
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Multi-Trade Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("100000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Create multiple trades
        trades = [
            Trade(
                trade_id=uuid4(),
                portfolio_id=portfolio.portfolio_id,
                symbol="AAPL",
                timestamp=datetime(2024, 1, 10, 14, 30, tzinfo=timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("100"),
                price=Decimal("150.00"),
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="First trade"
            ),
            Trade(
                trade_id=uuid4(),
                portfolio_id=portfolio.portfolio_id,
                symbol="MSFT",
                timestamp=datetime(2024, 1, 15, 10, 15, tzinfo=timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("50"),
                price=Decimal("380.00"),
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Second trade"
            ),
        ]
        
        # Save all trades
        for trade in trades:
            await portfolio_repository.save_trade(trade)
        
        # Retrieve trades for portfolio
        portfolio_trades = await portfolio_repository.get_trades_for_portfolio(portfolio.portfolio_id)
        assert len(portfolio_trades) == 2
        
        # Test with limit
        limited_trades = await portfolio_repository.get_trades_for_portfolio(portfolio.portfolio_id, limit=1)
        assert len(limited_trades) == 1

    @pytest.mark.asyncio
    async def test_save_and_get_position(self, portfolio_repository):
        """Test saving and retrieving positions."""
        # Create portfolio
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Position Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("50000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Create and save position
        position = Position(
            portfolio_id=portfolio.portfolio_id,
            symbol="GOOGL",
            qty=Decimal("25"),
            avg_cost=Decimal("2800.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        await portfolio_repository.save_position(position)
        
        # Retrieve position
        retrieved = await portfolio_repository.get_position(portfolio.portfolio_id, "GOOGL")
        
        assert retrieved is not None
        assert retrieved.portfolio_id == position.portfolio_id
        assert retrieved.symbol == position.symbol
        assert retrieved.qty == position.qty
        assert retrieved.avg_cost == position.avg_cost

    @pytest.mark.asyncio
    async def test_get_positions_for_portfolio(self, portfolio_repository):
        """Test retrieving all positions for a portfolio."""
        # Create portfolio
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Multi-Position Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("100000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Create multiple positions
        positions = [
            Position(
                portfolio_id=portfolio.portfolio_id,
                symbol="AAPL",
                qty=Decimal("100"),
                avg_cost=Decimal("150.00"),
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            ),
            Position(
                portfolio_id=portfolio.portfolio_id,
                symbol="MSFT",
                qty=Decimal("50"),
                avg_cost=Decimal("380.00"),
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            ),
        ]
        
        # Save all positions
        for position in positions:
            await portfolio_repository.save_position(position)
        
        # Retrieve positions for portfolio
        portfolio_positions = await portfolio_repository.get_positions_for_portfolio(portfolio.portfolio_id)
        assert len(portfolio_positions) == 2
        
        symbols = {p.symbol for p in portfolio_positions}
        assert "AAPL" in symbols
        assert "MSFT" in symbols

    @pytest.mark.asyncio
    async def test_delete_position(self, portfolio_repository):
        """Test position deletion."""
        # Create portfolio and position
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Delete Position Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("50000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        position = Position(
            portfolio_id=portfolio.portfolio_id,
            symbol="DELETE_POS",
            qty=Decimal("100"),
            avg_cost=Decimal("100.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_position(position)
        
        # Verify position exists
        assert await portfolio_repository.get_position(portfolio.portfolio_id, "DELETE_POS") is not None
        
        # Delete position
        await portfolio_repository.delete_position(portfolio.portfolio_id, "DELETE_POS")
        
        # Verify position is deleted
        assert await portfolio_repository.get_position(portfolio.portfolio_id, "DELETE_POS") is None

    @pytest.mark.asyncio
    async def test_delete_portfolio(self, portfolio_repository):
        """Test portfolio deletion with cascade."""
        # Create portfolio with trades and positions
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Delete Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("50000.00"),
            created=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_portfolio(portfolio)
        
        # Add trade
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=portfolio.portfolio_id,
            symbol="DELETE_TRADE",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("100.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Delete test"
        )
        await portfolio_repository.save_trade(trade)
        
        # Add position
        position = Position(
            portfolio_id=portfolio.portfolio_id,
            symbol="DELETE_POS",
            qty=Decimal("100"),
            avg_cost=Decimal("100.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        await portfolio_repository.save_position(position)
        
        # Verify everything exists
        assert await portfolio_repository.portfolio_exists(portfolio.portfolio_id)
        assert await portfolio_repository.get_trade(trade.trade_id) is not None
        assert await portfolio_repository.get_position(portfolio.portfolio_id, "DELETE_POS") is not None
        
        # Delete portfolio
        await portfolio_repository.delete_portfolio(portfolio.portfolio_id)
        
        # Verify everything is deleted
        assert not await portfolio_repository.portfolio_exists(portfolio.portfolio_id)
        assert await portfolio_repository.get_trade(trade.trade_id) is None
        assert await portfolio_repository.get_position(portfolio.portfolio_id, "DELETE_POS") is None