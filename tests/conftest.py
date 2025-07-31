"""Pytest configuration and shared fixtures for the Stock Analysis Platform."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Generator
from uuid import UUID, uuid4

import pytest

from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    BrokerProfile,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)


@pytest.fixture
def sample_asset() -> Asset:
    """Create a sample Asset for testing."""
    return Asset(
        symbol="AAPL",
        exchange="NASDAQ",
        asset_type=AssetType.STOCK,
        name="Apple Inc.",
    )


@pytest.fixture
def sample_etf_asset() -> Asset:
    """Create a sample ETF Asset for testing."""
    return Asset(
        symbol="SPY",
        exchange="NYSE",
        asset_type=AssetType.ETF,
        name="SPDR S&P 500 ETF Trust",
    )


@pytest.fixture
def sample_crypto_asset() -> Asset:
    """Create a sample Crypto Asset for testing."""
    return Asset(
        symbol="BTC-USD",
        exchange="CRYPTO",
        asset_type=AssetType.CRYPTO,
        name="Bitcoin USD",
    )


@pytest.fixture
def sample_commodity_asset() -> Asset:
    """Create a sample Commodity Asset for testing."""
    return Asset(
        symbol="GLD",
        exchange="NYSE",
        asset_type=AssetType.COMMODITY,
        name="SPDR Gold Trust",
    )


@pytest.fixture
def sample_asset_snapshot(sample_asset: Asset) -> AssetSnapshot:
    """Create a sample AssetSnapshot for testing."""
    return AssetSnapshot(
        symbol=sample_asset.symbol,
        timestamp=datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc),
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("149.50"),
        close=Decimal("152.75"),
        volume=50000000,
    )


@pytest.fixture
def sample_portfolio() -> Portfolio:
    """Create a sample Portfolio for testing."""
    return Portfolio(
        portfolio_id=uuid4(),
        name="Growth Portfolio",
        base_ccy="USD",
        cash_balance=Decimal("100000.00"),
        created=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_broker_profile() -> BrokerProfile:
    """Create a sample BrokerProfile for testing."""
    return BrokerProfile(
        broker_id="ROBINHOOD",
        name="Robinhood",
        pip_pct=Decimal("0.001"),  # 0.10%
        fee_flat=Decimal("0.00"),
        fee_pct=Decimal("0.000"),  # 0%
        min_order_value=Decimal("1.00"),
        supported_currencies=["USD"],
        supports_fractional=True,
    )


@pytest.fixture
def sample_ibkr_broker() -> BrokerProfile:
    """Create a sample IBKR Lite BrokerProfile for testing."""
    return BrokerProfile(
        broker_id="IBKR_LITE",
        name="Interactive Brokers Lite",
        pip_pct=Decimal("0.0005"),  # 0.05%
        fee_flat=Decimal("0.005"),  # $0.005 per share
        fee_pct=Decimal("0.000"),  # 0%
        min_order_value=Decimal("1.00"),
        supported_currencies=["USD", "EUR"],
        supports_fractional=False,
    )


@pytest.fixture
def sample_buy_trade(sample_portfolio: Portfolio, sample_asset: Asset) -> Trade:
    """Create a sample Buy Trade for testing."""
    return Trade(
        trade_id=uuid4(),
        portfolio_id=sample_portfolio.portfolio_id,
        symbol=sample_asset.symbol,
        timestamp=datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
        side=TradeSide.BUY,
        qty=Decimal("100"),
        price=Decimal("150.00"),
        pip_pct=Decimal("0.001"),
        fee_flat=Decimal("0.00"),
        fee_pct=Decimal("0.000"),
        unit="share",
        price_ccy="USD",
        comment="Initial purchase",
    )


@pytest.fixture
def sample_sell_trade(sample_portfolio: Portfolio, sample_asset: Asset) -> Trade:
    """Create a sample Sell Trade for testing."""
    return Trade(
        trade_id=uuid4(),
        portfolio_id=sample_portfolio.portfolio_id,
        symbol=sample_asset.symbol,
        timestamp=datetime(2024, 1, 20, 10, 15, 0, tzinfo=timezone.utc),
        side=TradeSide.SELL,
        qty=Decimal("50"),
        price=Decimal("155.00"),
        pip_pct=Decimal("0.001"),
        fee_flat=Decimal("0.00"),
        fee_pct=Decimal("0.000"),
        unit="share",
        price_ccy="USD",
        comment="Partial profit taking",
    )


@pytest.fixture
def sample_position(
    sample_portfolio: Portfolio, sample_asset: Asset, sample_buy_trade: Trade
) -> Position:
    """Create a sample Position for testing."""
    return Position(
        portfolio_id=sample_portfolio.portfolio_id,
        symbol=sample_asset.symbol,
        qty=sample_buy_trade.qty,
        avg_cost=sample_buy_trade.price,
        unit=sample_buy_trade.unit,
        price_ccy=sample_buy_trade.price_ccy,
        last_updated=sample_buy_trade.timestamp,
    )


@pytest.fixture
def multiple_assets() -> list[Asset]:
    """Create multiple assets for testing."""
    return [
        Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc.",
        ),
        Asset(
            symbol="MSFT",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Microsoft Corporation",
        ),
        Asset(
            symbol="SPY",
            exchange="NYSE",
            asset_type=AssetType.ETF,
            name="SPDR S&P 500 ETF Trust",
        ),
        Asset(
            symbol="BTC-USD",
            exchange="CRYPTO",
            asset_type=AssetType.CRYPTO,
            name="Bitcoin USD",
        ),
    ]


@pytest.fixture
def multiple_snapshots(multiple_assets: list[Asset]) -> list[AssetSnapshot]:
    """Create multiple asset snapshots for testing."""
    base_time = datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)
    
    return [
        AssetSnapshot(
            symbol="AAPL",
            timestamp=base_time,
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000,
        ),
        AssetSnapshot(
            symbol="MSFT",
            timestamp=base_time,
            open=Decimal("380.00"),
            high=Decimal("385.50"),
            low=Decimal("378.25"),
            close=Decimal("382.10"),
            volume=25000000,
        ),
        AssetSnapshot(
            symbol="SPY",
            timestamp=base_time,
            open=Decimal("480.00"),
            high=Decimal("482.15"),
            low=Decimal("479.80"),
            close=Decimal("481.50"),
            volume=75000000,
        ),
    ]


class TestDataFactory:
    """Factory for creating test data objects."""

    @staticmethod
    def create_asset(
        symbol: str = "TEST",
        exchange: str = "NASDAQ",
        asset_type: AssetType = AssetType.STOCK,
        name: str = "Test Asset",
    ) -> Asset:
        """Create an Asset with custom parameters."""
        return Asset(
            symbol=symbol,
            exchange=exchange,
            asset_type=asset_type,
            name=name,
        )

    @staticmethod
    def create_portfolio(
        name: str = "Test Portfolio",
        base_ccy: str = "USD",
        cash_balance: Decimal = Decimal("10000.00"),
        portfolio_id: UUID | None = None,
    ) -> Portfolio:
        """Create a Portfolio with custom parameters."""
        return Portfolio(
            portfolio_id=portfolio_id or uuid4(),
            name=name,
            base_ccy=base_ccy,
            cash_balance=cash_balance,
            created=datetime.now(timezone.utc),
        )

    @staticmethod
    def create_trade(
        portfolio_id: UUID,
        symbol: str = "TEST",
        side: TradeSide = TradeSide.BUY,
        qty: Decimal = Decimal("100"),
        price: Decimal = Decimal("50.00"),
        trade_id: UUID | None = None,
    ) -> Trade:
        """Create a Trade with custom parameters."""
        return Trade(
            trade_id=trade_id or uuid4(),
            portfolio_id=portfolio_id,
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            side=side,
            qty=qty,
            price=price,
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Test trade",
        )


@pytest.fixture
def test_factory() -> TestDataFactory:
    """Provide the test data factory."""
    return TestDataFactory