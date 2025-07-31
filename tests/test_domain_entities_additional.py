"""Additional unit tests for domain entities to improve coverage."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

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
from stockapp.domain.exceptions import (
    DomainValidationError,
    InsufficientFundsError,
    InvalidPositionError,
    InvalidTradeError,
)


class TestAssetAdditionalCoverage:
    """Additional tests for Asset entity to improve coverage."""

    def test_asset_equality_with_non_asset(self):
        """Test Asset equality with non-Asset objects."""
        asset = Asset(
            symbol="AAPL",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )
        
        # Test equality with non-Asset objects
        assert asset != "AAPL"  # string
        assert asset != 123     # number
        assert asset != None    # None
        assert asset != {"symbol": "AAPL"}  # dict


class TestAssetSnapshotAdditionalCoverage:
    """Additional tests for AssetSnapshot entity to improve coverage."""

    def test_asset_snapshot_daily_return_zero_open(self):
        """Test daily return calculation when open price is zero."""
        snapshot = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("0.01"),  # Very small but not zero
            high=Decimal("0.02"),
            low=Decimal("0.01"),
            close=Decimal("0.015"),
            volume=1000
        )
        
        daily_return = snapshot.daily_return()
        expected = (Decimal("0.015") - Decimal("0.01")) / Decimal("0.01")
        assert daily_return == expected

    def test_asset_snapshot_daily_return_exactly_zero_open(self):
        """Test daily return calculation when open price is exactly zero."""
        # We need to manually set this since validation prevents zero prices
        snapshot = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("1.00"),
            high=Decimal("1.10"),
            low=Decimal("0.90"),
            close=Decimal("1.05"),
            volume=1000
        )
        
        # Manually override open to zero to test the edge case
        object.__setattr__(snapshot, 'open', Decimal('0'))
        
        daily_return = snapshot.daily_return()
        assert daily_return == Decimal('0')

    def test_asset_snapshot_validation_edge_cases(self):
        """Test AssetSnapshot validation edge cases."""
        base_time = datetime.now(timezone.utc)
        
        # Test high == open (valid)
        AssetSnapshot(
            symbol="TEST",
            timestamp=base_time,
            open=Decimal("100.00"),
            high=Decimal("100.00"),  # high == open (valid)
            low=Decimal("95.00"),
            close=Decimal("98.00"),
            volume=0  # Zero volume is valid
        )
        
        # Test low == close (valid)
        AssetSnapshot(
            symbol="TEST",
            timestamp=base_time,
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("98.00"),
            close=Decimal("98.00"),  # close == low (valid)
            volume=1000
        )


class TestPortfolioAdditionalCoverage:
    """Additional tests for Portfolio entity to improve coverage."""

    def test_portfolio_validation_whitespace_name(self):
        """Test Portfolio validation with whitespace-only name."""
        with pytest.raises(DomainValidationError, match="Portfolio name cannot be empty"):
            Portfolio(
                portfolio_id=uuid4(),
                name="   ",  # Only whitespace
                base_ccy="USD",
                cash_balance=Decimal("10000.00"),
                created=datetime.now(timezone.utc)
            )

    def test_portfolio_deduct_zero_cash(self):
        """Test deducting zero cash amount."""
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        
        with pytest.raises(DomainValidationError, match="Cash amount must be positive"):
            portfolio.deduct_cash(Decimal("0"))

    def test_portfolio_add_zero_cash(self):
        """Test adding zero cash amount."""
        portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        
        with pytest.raises(DomainValidationError, match="Cash amount must be positive"):
            portfolio.add_cash(Decimal("0"))


class TestTradeAdditionalCoverage:
    """Additional tests for Trade entity to improve coverage."""

    def test_trade_validation_negative_flat_fee(self):
        """Test Trade validation with negative flat fee."""
        with pytest.raises(InvalidTradeError, match="Flat fees must be non-negative"):
            Trade(
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="AAPL",
                timestamp=datetime.now(timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("100"),
                price=Decimal("150.00"),
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("-5.00"),  # Negative flat fee
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Test"
            )

    def test_trade_commission_cost_calculation(self):
        """Test commission cost calculation separately."""
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("5.00"),
            fee_pct=Decimal("0.002"),  # 0.2% commission
            unit="share",
            price_ccy="USD",
            comment="Test"
        )
        
        commission = trade.commission_cost()
        expected = Decimal("100") * Decimal("150.00") * Decimal("0.002")  # 30.00
        assert commission == expected


class TestPositionAdditionalCoverage:
    """Additional tests for Position entity to improve coverage."""

    def test_position_add_shares_zero_quantity(self):
        """Test adding zero shares to position."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        with pytest.raises(InvalidPositionError, match="Additional quantity must be positive"):
            position.add_shares(Decimal("0"), Decimal("160.00"))

    def test_position_add_shares_zero_price(self):
        """Test adding shares with zero price."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        with pytest.raises(InvalidPositionError, match="Price must be positive"):
            position.add_shares(Decimal("50"), Decimal("0"))

    def test_position_reduce_shares_zero_quantity(self):
        """Test reducing zero shares from position."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        with pytest.raises(InvalidPositionError, match="Reduction quantity must be positive"):
            position.reduce_shares(Decimal("0"))

    def test_position_unrealized_pnl_pct_zero_avg_cost(self):
        """Test unrealized P&L percentage with zero average cost."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("0.01"),  # Very small but not zero
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        # Manually set avg_cost to zero to test edge case
        object.__setattr__(position, 'avg_cost', Decimal('0'))
        
        pnl_pct = position.unrealized_pnl_pct(Decimal("150.00"))
        assert pnl_pct == Decimal('0')


class TestBrokerProfileAdditionalCoverage:
    """Additional tests for BrokerProfile entity to improve coverage."""

    def test_broker_profile_validation_whitespace_id(self):
        """Test BrokerProfile validation with whitespace-only ID."""
        with pytest.raises(DomainValidationError, match="Broker ID cannot be empty"):
            BrokerProfile(
                broker_id="   ",  # Only whitespace
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            )

    def test_broker_profile_validation_whitespace_name(self):
        """Test BrokerProfile validation with whitespace-only name."""
        with pytest.raises(DomainValidationError, match="Broker name cannot be empty"):
            BrokerProfile(
                broker_id="TEST",
                name="   ",  # Only whitespace
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            )

    def test_broker_profile_validation_negative_min_order(self):
        """Test BrokerProfile validation with negative minimum order value."""
        with pytest.raises(DomainValidationError, match="Minimum order value must be non-negative"):
            BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("-1.00"),  # Negative minimum
                supported_currencies=["USD"],
                supports_fractional=True
            )

    def test_broker_profile_validation_empty_currencies(self):
        """Test BrokerProfile validation with empty supported currencies."""
        with pytest.raises(DomainValidationError, match="Supported currencies list cannot be empty"):
            BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=[],  # Empty list
                supports_fractional=True
            )

    def test_broker_profile_can_execute_order_edge_cases(self):
        """Test broker profile order execution edge cases."""
        broker = BrokerProfile(
            broker_id="TEST",
            name="Test Broker",
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            min_order_value=Decimal("100.00"),  # High minimum
            supported_currencies=["USD"],
            supports_fractional=False
        )
        
        # Test fractional shares when not supported
        assert broker.can_execute_order(Decimal("10.5"), Decimal("50.00")) is False
        
        # Test minimum order value exactly at threshold
        assert broker.can_execute_order(Decimal("2"), Decimal("50.00")) is True  # 2 * 50 = 100
        assert broker.can_execute_order(Decimal("1"), Decimal("50.00")) is False  # 1 * 50 = 50 < 100
        
        # Test whole shares meeting minimum
        assert broker.can_execute_order(Decimal("3"), Decimal("50.00")) is True


class TestAdditionalValidationScenarios:
    """Test additional validation scenarios not covered in main tests."""

    def test_asset_type_enum_coverage(self):
        """Test all AssetType enum values."""
        # Test creating assets with all asset types
        asset_types = [AssetType.STOCK, AssetType.ETF, AssetType.CRYPTO, AssetType.COMMODITY]
        
        for asset_type in asset_types:
            asset = Asset(
                symbol=f"TEST_{asset_type.value}",
                exchange="TEST",
                asset_type=asset_type,
                name=f"Test {asset_type.value}"
            )
            assert asset.asset_type == asset_type

    def test_trade_side_enum_coverage(self):
        """Test all TradeSide enum values."""
        # Test creating trades with both sides
        for side in [TradeSide.BUY, TradeSide.SELL]:
            trade = Trade(
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                side=side,
                qty=Decimal("100"),
                price=Decimal("150.00"),
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Test"
            )
            assert trade.side == side