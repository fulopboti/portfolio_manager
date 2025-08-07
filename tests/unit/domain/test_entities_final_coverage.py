"""Tests to achieve final 100% coverage for domain entities."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from portfolio_manager.domain.entities import (
    Asset, AssetType, AssetSnapshot, Portfolio, Position, 
    Trade, TradeSide, BrokerProfile
)
from portfolio_manager.domain.exceptions import (
    DomainValidationError, InsufficientFundsError, 
    InvalidPositionError, InvalidTradeError
)


class TestDomainEntitiesFinalCoverage:
    """Tests to cover the final missing lines in domain entities."""

    def test_asset_snapshot_high_less_than_close_validation_coverage(self):
        """Test AssetSnapshot validation when high < close (line 98)."""
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("100.00"),
                high=Decimal("95.00"),  # high < close (98.00)
                low=Decimal("90.00"),
                close=Decimal("98.00"),
                volume=1000
            )

    def test_position_market_value_with_minimal_quantity_coverage(self):
        """Test Position market_value method with minimal quantity."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="TEST",
            qty=Decimal("0.000001"),  # Minimal quantity to pass validation
            avg_cost=Decimal("100.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        current_price = Decimal("150.00")
        market_value = position.market_value(current_price)
        
        # Should return current_price * qty
        expected = Decimal("0.000001") * Decimal("150.00")
        assert market_value == expected

    def test_asset_validations_coverage(self):
        """Test Asset validation coverage (lines 44, 47, 50)."""
        # Test symbol validation
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            Asset(symbol="", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Test")
        
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            Asset(symbol="   ", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Test")
        
        # Test exchange validation
        with pytest.raises(DomainValidationError, match="Exchange cannot be empty"):
            Asset(symbol="TEST", exchange="", asset_type=AssetType.STOCK, name="Test")
        
        # Test name validation
        with pytest.raises(DomainValidationError, match="Name cannot be empty"):
            Asset(symbol="TEST", exchange="NASDAQ", asset_type=AssetType.STOCK, name="")

    def test_asset_equality_non_asset_coverage(self):
        """Test Asset equality with non-Asset object (lines 58-60)."""
        asset = Asset(symbol="TEST", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Test Company")
        
        # Test equality with non-Asset object
        assert asset != "not_an_asset"
        assert asset != 123
        assert asset != None

    def test_asset_hash_coverage(self):
        """Test Asset hash method (line 64)."""
        asset = Asset(symbol="TEST", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Test Company")
        hash_value = hash(asset)
        assert isinstance(hash_value, int)

    def test_asset_snapshot_validations_complete_coverage(self):
        """Test AssetSnapshot comprehensive validations (lines 87, 91, 98, 102, 105, 109)."""
        # Test high < low validation
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST", timestamp=datetime.now(timezone.utc),
                open=Decimal("100"), high=Decimal("95"), low=Decimal("100"),  # high < low
                close=Decimal("98"), volume=1000
            )
        
        # Test high < open validation
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST", timestamp=datetime.now(timezone.utc),
                open=Decimal("105"), high=Decimal("100"), low=Decimal("95"),  # high < open
                close=Decimal("98"), volume=1000
            )
        
        # Test low > open validation
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST", timestamp=datetime.now(timezone.utc),
                open=Decimal("95"), high=Decimal("110"), low=Decimal("100"),  # low > open
                close=Decimal("98"), volume=1000
            )
        
        # Test low > close validation
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST", timestamp=datetime.now(timezone.utc),
                open=Decimal("100"), high=Decimal("110"), low=Decimal("105"),  # low > close
                close=Decimal("98"), volume=1000
            )

    def test_asset_snapshot_methods_coverage(self):
        """Test AssetSnapshot methods (lines 113-115, 119, 123)."""
        snapshot = AssetSnapshot(
            symbol="TEST", timestamp=datetime.now(timezone.utc),
            open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
            close=Decimal("105"), volume=1000
        )
        
        # Test is_green_day (close > open)
        assert snapshot.is_green_day() is True
        
        # Test price_range
        assert snapshot.price_range() == Decimal("20")
        
        # Create snapshot with zero open to test daily_return edge case
        # Use object manipulation since validation prevents direct zero
        snapshot_zero_open = AssetSnapshot(
            symbol="TEST2", timestamp=datetime.now(timezone.utc),
            open=Decimal("0.01"), high=Decimal("110"), low=Decimal("0.01"),
            close=Decimal("100"), volume=1000
        )
        object.__setattr__(snapshot_zero_open, 'open', Decimal('0'))
        
        # Test daily_return with zero open
        daily_return = snapshot_zero_open.daily_return()
        assert daily_return == Decimal('0')

    def test_portfolio_validations_coverage(self):
        """Test Portfolio validations (lines 139, 144, 147)."""
        # Test name validation
        with pytest.raises(DomainValidationError, match="Portfolio name cannot be empty"):
            Portfolio(
                portfolio_id=uuid4(), name="", base_ccy="USD",
                cash_balance=Decimal("1000"), created=datetime.now(timezone.utc)
            )
        
        # Test invalid currency
        with pytest.raises(DomainValidationError, match="Invalid currency code"):
            Portfolio(
                portfolio_id=uuid4(), name="Test", base_ccy="INVALID",
                cash_balance=Decimal("1000"), created=datetime.now(timezone.utc)
            )
        
        # Test negative cash balance
        with pytest.raises(DomainValidationError, match="Cash balance cannot be negative"):
            Portfolio(
                portfolio_id=uuid4(), name="Test", base_ccy="USD",
                cash_balance=Decimal("-100"), created=datetime.now(timezone.utc)
            )

    def test_portfolio_cash_operations_coverage(self):
        """Test Portfolio cash operations (lines 152, 159, 162)."""
        portfolio = Portfolio(
            portfolio_id=uuid4(), name="Test", base_ccy="USD",
            cash_balance=Decimal("1000"), created=datetime.now(timezone.utc)
        )
        
        # Test add_cash with invalid amount
        with pytest.raises(DomainValidationError, match="Cash amount must be positive"):
            portfolio.add_cash(Decimal("0"))
        
        # Test deduct_cash with invalid amount
        with pytest.raises(DomainValidationError, match="Cash amount must be positive"):
            portfolio.deduct_cash(Decimal("-50"))
        
        # Test insufficient funds
        with pytest.raises(InsufficientFundsError, match="Insufficient funds"):
            portfolio.deduct_cash(Decimal("2000"))

    def test_trade_validations_coverage(self):
        """Test Trade validations (lines 194, 197, 200, 203)."""
        # Test invalid quantity
        with pytest.raises(InvalidTradeError, match="Trade quantity must be positive"):
            Trade(
                trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
                timestamp=datetime.now(timezone.utc), side=TradeSide.BUY,
                qty=Decimal("0"), price=Decimal("100"), pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), unit="share",
                price_ccy="USD", comment="Test"
            )
        
        # Test invalid price
        with pytest.raises(InvalidTradeError, match="Trade price must be positive"):
            Trade(
                trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
                timestamp=datetime.now(timezone.utc), side=TradeSide.BUY,
                qty=Decimal("10"), price=Decimal("0"), pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), unit="share",
                price_ccy="USD", comment="Test"
            )
        
        # Test negative pip_pct
        with pytest.raises(InvalidTradeError, match="Fee percentages must be non-negative"):
            Trade(
                trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
                timestamp=datetime.now(timezone.utc), side=TradeSide.BUY,
                qty=Decimal("10"), price=Decimal("100"), pip_pct=Decimal("-0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), unit="share",
                price_ccy="USD", comment="Test"
            )
        
        # Test negative fee_flat
        with pytest.raises(InvalidTradeError, match="Flat fees must be non-negative"):
            Trade(
                trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
                timestamp=datetime.now(timezone.utc), side=TradeSide.BUY,
                qty=Decimal("10"), price=Decimal("100"), pip_pct=Decimal("0.001"),
                fee_flat=Decimal("-1"), fee_pct=Decimal("0.001"), unit="share",
                price_ccy="USD", comment="Test"
            )

    def test_trade_methods_coverage(self):
        """Test Trade methods (lines 235, 239)."""
        trade = Trade(
            trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
            timestamp=datetime.now(timezone.utc), side=TradeSide.BUY,
            qty=Decimal("10"), price=Decimal("100"), pip_pct=Decimal("0.001"),
            fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), unit="share",
            price_ccy="USD", comment="Test"
        )
        
        # Test is_buy and is_sell
        assert trade.is_buy() is True
        assert trade.is_sell() is False

    def test_position_validations_coverage(self):
        """Test Position validations (lines 257, 260)."""
        # Test invalid quantity
        with pytest.raises(InvalidPositionError, match="Position quantity must be positive"):
            Position(
                portfolio_id=uuid4(), symbol="TEST", qty=Decimal("0"),
                avg_cost=Decimal("100"), unit="share", price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            )
        
        # Test invalid avg_cost
        with pytest.raises(InvalidPositionError, match="Average cost must be positive"):
            Position(
                portfolio_id=uuid4(), symbol="TEST", qty=Decimal("10"),
                avg_cost=Decimal("0"), unit="share", price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            )

    def test_position_unrealized_pnl_pct_zero_cost_coverage(self):
        """Test Position unrealized_pnl_pct with zero avg_cost (lines 276-278)."""
        position = Position(
            portfolio_id=uuid4(), symbol="TEST", qty=Decimal("10"),
            avg_cost=Decimal("0.000001"), unit="share", price_ccy="USD",  # Almost zero
            last_updated=datetime.now(timezone.utc)
        )
        
        # Test with actual zero cost using object manipulation (since validation prevents direct zero)
        object.__setattr__(position, 'avg_cost', Decimal('0'))
        
        pnl_pct = position.unrealized_pnl_pct(Decimal("100"))
        assert pnl_pct == Decimal('0')

    def test_position_add_shares_validations_coverage(self):
        """Test Position add_shares validations (lines 283, 286)."""
        position = Position(
            portfolio_id=uuid4(), symbol="TEST", qty=Decimal("10"),
            avg_cost=Decimal("100"), unit="share", price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        # Test invalid additional quantity
        with pytest.raises(InvalidPositionError, match="Additional quantity must be positive"):
            position.add_shares(Decimal("0"), Decimal("110"))
        
        # Test invalid price
        with pytest.raises(InvalidPositionError, match="Price must be positive"):
            position.add_shares(Decimal("5"), Decimal("0"))

    def test_position_reduce_shares_validations_coverage(self):
        """Test Position reduce_shares validations (lines 298, 301)."""
        position = Position(
            portfolio_id=uuid4(), symbol="TEST", qty=Decimal("10"),
            avg_cost=Decimal("100"), unit="share", price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        # Test invalid reduction quantity
        with pytest.raises(InvalidPositionError, match="Reduction quantity must be positive"):
            position.reduce_shares(Decimal("0"))
        
        # Test reduction exceeds position
        with pytest.raises(InvalidPositionError, match="Cannot reduce position"):
            position.reduce_shares(Decimal("15"))

    def test_broker_profile_validations_coverage(self):
        """Test BrokerProfile validations (lines 325, 328, 331, 334, 337, 340)."""
        # Test empty broker_id
        with pytest.raises(DomainValidationError, match="Broker ID cannot be empty"):
            BrokerProfile(
                broker_id="", name="Test Broker", pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
                supported_currencies=["USD"], supports_fractional=True
            )
        
        # Test empty name
        with pytest.raises(DomainValidationError, match="Broker name cannot be empty"):
            BrokerProfile(
                broker_id="TEST", name="", pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
                supported_currencies=["USD"], supports_fractional=True
            )
        
        # Test negative pip_pct
        with pytest.raises(DomainValidationError, match="Fee percentages must be non-negative"):
            BrokerProfile(
                broker_id="TEST", name="Test Broker", pip_pct=Decimal("-0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
                supported_currencies=["USD"], supports_fractional=True
            )
        
        # Test negative fee_flat
        with pytest.raises(DomainValidationError, match="Flat fees must be non-negative"):
            BrokerProfile(
                broker_id="TEST", name="Test Broker", pip_pct=Decimal("0.001"),
                fee_flat=Decimal("-1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
                supported_currencies=["USD"], supports_fractional=True
            )
        
        # Test negative min_order_value
        with pytest.raises(DomainValidationError, match="Minimum order value must be non-negative"):
            BrokerProfile(
                broker_id="TEST", name="Test Broker", pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("-100"),
                supported_currencies=["USD"], supports_fractional=True
            )
        
        # Test empty supported_currencies
        with pytest.raises(DomainValidationError, match="Supported currencies list cannot be empty"):
            BrokerProfile(
                broker_id="TEST", name="Test Broker", pip_pct=Decimal("0.001"),
                fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
                supported_currencies=[], supports_fractional=True
            )

    def test_broker_profile_methods_coverage(self):
        """Test BrokerProfile methods (lines 344-348, 352, 363)."""
        broker = BrokerProfile(
            broker_id="TEST", name="Test Broker", pip_pct=Decimal("0.001"),
            fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), min_order_value=Decimal("100"),
            supported_currencies=["USD", "EUR"], supports_fractional=False
        )
        
        # Test calculate_total_cost with sell trade
        sell_trade = Trade(
            trade_id=uuid4(), portfolio_id=uuid4(), symbol="TEST",
            timestamp=datetime.now(timezone.utc), side=TradeSide.SELL,
            qty=Decimal("10"), price=Decimal("100"), pip_pct=Decimal("0.001"),
            fee_flat=Decimal("1"), fee_pct=Decimal("0.001"), unit="share",
            price_ccy="USD", comment="Test"
        )
        cost = broker.calculate_total_cost(sell_trade)
        assert cost > 0  # Should return absolute value for sell
        
        # Test supports_currency
        assert broker.supports_currency("USD") is True
        assert broker.supports_currency("GBP") is False
        
        # Test can_execute_order with fractional shares (should fail)
        can_execute = broker.can_execute_order(Decimal("10.5"), Decimal("20"))
        assert can_execute is False

    def test_broker_profile_can_execute_order_exceeds_min_coverage(self):
        """Test BrokerProfile can_execute_order when order value exceeds minimum (line 348)."""
        broker = BrokerProfile(
            broker_id="TEST_BROKER",
            name="Test Broker",
            pip_pct=Decimal("0.0001"),
            fee_flat=Decimal("1.00"),
            fee_pct=Decimal("0.001"),
            min_order_value=Decimal("100.00"),  # Minimum order value
            supported_currencies=["USD"],
            supports_fractional=True
        )
        
        # Test order that meets minimum requirement
        quantity = Decimal("10")
        price = Decimal("15.00")  # 10 * 15 = 150, which is > 100
        
        can_execute = broker.can_execute_order(quantity, price)
        assert can_execute is True
