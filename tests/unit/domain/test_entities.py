"""Unit tests for domain entities."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from portfolio_manager.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    BrokerProfile,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)
from portfolio_manager.domain.exceptions import (
    DomainValidationError,
    InsufficientFundsError,
    InvalidPositionError,
    InvalidTradeError,
)


class TestAsset:
    """Test cases for Asset entity."""

    def test_asset_creation_valid(self, sample_asset: Asset):
        """Test creating a valid Asset."""
        assert sample_asset.symbol == "AAPL"
        assert sample_asset.exchange == "NASDAQ"
        assert sample_asset.asset_type == AssetType.STOCK
        assert sample_asset.name == "Apple Inc."

    def test_asset_equality(self):
        """Test Asset equality based on symbol."""
        asset1 = Asset(
            symbol="AAPL",
            exchange="NASDAQ", 
            asset_type=AssetType.STOCK,
            name="Apple Inc."
        )
        asset2 = Asset(
            symbol="AAPL",
            exchange="NYSE",  # Different exchange
            asset_type=AssetType.ETF,  # Different type
            name="Different Name"  # Different name
        )
        asset3 = Asset(
            symbol="MSFT",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            name="Microsoft"
        )

        assert asset1 == asset2  # Same symbol
        assert asset1 != asset3  # Different symbol
        assert hash(asset1) == hash(asset2)
        assert hash(asset1) != hash(asset3)

    def test_asset_validation_empty_symbol(self):
        """Test Asset validation with empty symbol."""
        with pytest.raises(DomainValidationError, match="Symbol cannot be empty"):
            Asset(
                symbol="",
                exchange="NASDAQ",
                asset_type=AssetType.STOCK,
                name="Test"
            )

    def test_asset_validation_empty_exchange(self):
        """Test Asset validation with empty exchange."""
        with pytest.raises(DomainValidationError, match="Exchange cannot be empty"):
            Asset(
                symbol="TEST",
                exchange="",
                asset_type=AssetType.STOCK,
                name="Test"
            )

    def test_asset_validation_empty_name(self):
        """Test Asset validation with empty name."""
        with pytest.raises(DomainValidationError, match="Name cannot be empty"):
            Asset(
                symbol="TEST",
                exchange="NASDAQ",
                asset_type=AssetType.STOCK,
                name=""
            )

    def test_asset_symbol_normalization(self):
        """Test Asset symbol is normalized to uppercase."""
        asset = Asset(
            symbol="aapl",
            exchange="nasdaq",
            asset_type=AssetType.STOCK,
            name="Apple"
        )
        assert asset.symbol == "AAPL"
        assert asset.exchange == "NASDAQ"

    def test_asset_repr(self, sample_asset: Asset):
        """Test Asset string representation."""
        repr_str = repr(sample_asset)
        assert "Asset" in repr_str
        assert "AAPL" in repr_str
        assert "NASDAQ" in repr_str


class TestAssetSnapshot:
    """Test cases for AssetSnapshot entity."""

    def test_asset_snapshot_creation_valid(self, sample_asset_snapshot: AssetSnapshot):
        """Test creating a valid AssetSnapshot."""
        assert sample_asset_snapshot.symbol == "AAPL"
        assert sample_asset_snapshot.open == Decimal("150.00")
        assert sample_asset_snapshot.high == Decimal("155.00")
        assert sample_asset_snapshot.low == Decimal("149.50")
        assert sample_asset_snapshot.close == Decimal("152.75")
        assert sample_asset_snapshot.volume == 50000000

    def test_asset_snapshot_validation_ohlc_relationships(self):
        """Test AssetSnapshot OHLC validation rules."""
        base_time = datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)

        # High should be >= Open, Close, Low
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("95.00"),  # High < Open
                low=Decimal("90.00"),
                close=Decimal("98.00"),
                volume=1000
            )

        # Low should be <= Open, Close, High
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("110.00"),  # Low > High
                close=Decimal("98.00"),
                volume=1000
            )

    def test_asset_snapshot_validation_negative_prices(self):
        """Test AssetSnapshot validation with negative prices."""
        base_time = datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(DomainValidationError, match="Prices must be positive"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("-100.00"),
                high=Decimal("105.00"),
                low=Decimal("95.00"),
                close=Decimal("98.00"),
                volume=1000
            )

    def test_asset_snapshot_validation_negative_volume(self):
        """Test AssetSnapshot validation with negative volume."""
        base_time = datetime(2024, 1, 15, 16, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(DomainValidationError, match="Volume must be non-negative"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("95.00"),
                close=Decimal("98.00"),
                volume=-1000
            )

    def test_asset_snapshot_daily_return_calculation(self, sample_asset_snapshot: AssetSnapshot):
        """Test daily return calculation."""
        daily_return = sample_asset_snapshot.daily_return()
        expected = (Decimal("152.75") - Decimal("150.00")) / Decimal("150.00")
        assert abs(daily_return - expected) < Decimal("0.0001")

    def test_asset_snapshot_is_green_day(self, sample_asset_snapshot: AssetSnapshot):
        """Test green day detection."""
        assert sample_asset_snapshot.is_green_day() is True

        # Create a red day snapshot
        red_snapshot = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("102.00"),
            low=Decimal("95.00"),
            close=Decimal("96.00"),  # Close < Open
            volume=1000
        )
        assert red_snapshot.is_green_day() is False

    def test_asset_snapshot_price_range(self, sample_asset_snapshot: AssetSnapshot):
        """Test price range calculation."""
        price_range = sample_asset_snapshot.price_range()
        expected = Decimal("155.00") - Decimal("149.50")
        assert price_range == expected


class TestPortfolio:
    """Test cases for Portfolio entity."""

    def test_portfolio_creation_valid(self, sample_portfolio: Portfolio):
        """Test creating a valid Portfolio."""
        assert isinstance(sample_portfolio.portfolio_id, UUID)
        assert sample_portfolio.name == "Growth Portfolio"
        assert sample_portfolio.base_ccy == "USD"
        assert sample_portfolio.cash_balance == Decimal("100000.00")

    def test_portfolio_validation_empty_name(self):
        """Test Portfolio validation with empty name."""
        with pytest.raises(DomainValidationError, match="Portfolio name cannot be empty"):
            Portfolio(
                portfolio_id=uuid4(),
                name="",
                base_ccy="USD",
                cash_balance=Decimal("10000.00"),
                created=datetime.now(timezone.utc)
            )

    def test_portfolio_validation_invalid_currency(self):
        """Test Portfolio validation with invalid currency."""
        with pytest.raises(DomainValidationError, match="Invalid currency code"):
            Portfolio(
                portfolio_id=uuid4(),
                name="Test Portfolio",
                base_ccy="XYZ",  # Invalid currency
                cash_balance=Decimal("10000.00"),
                created=datetime.now(timezone.utc)
            )

    def test_portfolio_validation_negative_cash(self):
        """Test Portfolio validation with negative cash balance."""
        with pytest.raises(DomainValidationError, match="Cash balance cannot be negative"):
            Portfolio(
                portfolio_id=uuid4(),
                name="Test Portfolio",
                base_ccy="USD",
                cash_balance=Decimal("-1000.00"),
                created=datetime.now(timezone.utc)
            )

    def test_portfolio_add_cash(self, sample_portfolio: Portfolio):
        """Test adding cash to portfolio."""
        original_balance = sample_portfolio.cash_balance
        sample_portfolio.add_cash(Decimal("5000.00"))
        assert sample_portfolio.cash_balance == original_balance + Decimal("5000.00")

    def test_portfolio_add_negative_cash(self, sample_portfolio: Portfolio):
        """Test adding negative cash amount."""
        with pytest.raises(DomainValidationError, match="Cash amount must be positive"):
            sample_portfolio.add_cash(Decimal("-1000.00"))

    def test_portfolio_deduct_cash_sufficient(self, sample_portfolio: Portfolio):
        """Test deducting cash with sufficient balance."""
        original_balance = sample_portfolio.cash_balance
        sample_portfolio.deduct_cash(Decimal("10000.00"))
        assert sample_portfolio.cash_balance == original_balance - Decimal("10000.00")

    def test_portfolio_deduct_cash_insufficient(self, sample_portfolio: Portfolio):
        """Test deducting cash with insufficient balance."""
        with pytest.raises(InsufficientFundsError):
            sample_portfolio.deduct_cash(Decimal("200000.00"))

    def test_portfolio_has_sufficient_cash(self, sample_portfolio: Portfolio):
        """Test checking sufficient cash availability."""
        assert sample_portfolio.has_sufficient_cash(Decimal("50000.00")) is True
        assert sample_portfolio.has_sufficient_cash(Decimal("150000.00")) is False


class TestTrade:
    """Test cases for Trade entity."""

    def test_trade_creation_valid(self, sample_buy_trade: Trade):
        """Test creating a valid Trade."""
        assert isinstance(sample_buy_trade.trade_id, UUID)
        assert isinstance(sample_buy_trade.portfolio_id, UUID)
        assert sample_buy_trade.symbol == "AAPL"
        assert sample_buy_trade.side == TradeSide.BUY
        assert sample_buy_trade.qty == Decimal("100")
        assert sample_buy_trade.price == Decimal("150.00")

    def test_trade_validation_zero_quantity(self):
        """Test Trade validation with zero quantity."""
        with pytest.raises(InvalidTradeError, match="Trade quantity must be positive"):
            Trade(
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="AAPL",
                timestamp=datetime.now(timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("0"),  # Invalid quantity
                price=Decimal("150.00"),
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Test"
            )

    def test_trade_validation_negative_price(self):
        """Test Trade validation with negative price."""
        with pytest.raises(InvalidTradeError, match="Trade price must be positive"):
            Trade(
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="AAPL",
                timestamp=datetime.now(timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("100"),
                price=Decimal("-150.00"),  # Invalid price
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Test"
            )

    def test_trade_validation_invalid_fees(self):
        """Test Trade validation with invalid fees."""
        with pytest.raises(InvalidTradeError, match="Fee percentages must be non-negative"):
            Trade(
                trade_id=uuid4(),
                portfolio_id=uuid4(),
                symbol="AAPL",
                timestamp=datetime.now(timezone.utc),
                side=TradeSide.BUY,
                qty=Decimal("100"),
                price=Decimal("150.00"),
                pip_pct=Decimal("-0.001"),  # Invalid pip
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                unit="share",
                price_ccy="USD",
                comment="Test"
            )

    def test_trade_gross_amount_calculation(self, sample_buy_trade: Trade):
        """Test gross amount calculation."""
        gross_amount = sample_buy_trade.gross_amount()
        expected = Decimal("100") * Decimal("150.00")  # qty * price
        assert gross_amount == expected

    def test_trade_pip_cost_calculation(self, sample_buy_trade: Trade):
        """Test pip cost calculation."""
        pip_cost = sample_buy_trade.pip_cost()
        expected = Decimal("100") * Decimal("150.00") * Decimal("0.001")
        assert pip_cost == expected

    def test_trade_total_fees_calculation(self):
        """Test total fees calculation."""
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),  # 0.1%
            fee_flat=Decimal("5.00"),   # $5 flat fee
            fee_pct=Decimal("0.001"),   # 0.1% commission
            unit="share",
            price_ccy="USD",
            comment="Test"
        )

        total_fees = trade.total_fees()
        gross = Decimal("100") * Decimal("150.00")  # 15000
        expected_pip = gross * Decimal("0.001")     # 15.00
        expected_commission = gross * Decimal("0.001")  # 15.00
        expected_total = expected_pip + Decimal("5.00") + expected_commission  # 35.00

        assert total_fees == expected_total

    def test_trade_net_amount_buy(self, sample_buy_trade: Trade):
        """Test net amount calculation for buy trade."""
        net_amount = sample_buy_trade.net_amount()
        gross = sample_buy_trade.gross_amount()
        fees = sample_buy_trade.total_fees()
        expected = gross + fees  # Buy: pay gross + fees
        assert net_amount == expected

    def test_trade_net_amount_sell(self, sample_sell_trade: Trade):
        """Test net amount calculation for sell trade."""
        net_amount = sample_sell_trade.net_amount()
        gross = sample_sell_trade.gross_amount()
        fees = sample_sell_trade.total_fees()
        expected = gross - fees  # Sell: receive gross - fees
        assert net_amount == expected

    def test_trade_is_buy_sell(self, sample_buy_trade: Trade, sample_sell_trade: Trade):
        """Test buy/sell detection methods."""
        assert sample_buy_trade.is_buy() is True
        assert sample_buy_trade.is_sell() is False
        assert sample_sell_trade.is_buy() is False
        assert sample_sell_trade.is_sell() is True


class TestPosition:
    """Test cases for Position entity."""

    def test_position_creation_valid(self, sample_position: Position):
        """Test creating a valid Position."""
        assert isinstance(sample_position.portfolio_id, UUID)
        assert sample_position.symbol == "AAPL"
        assert sample_position.qty == Decimal("100")
        assert sample_position.avg_cost == Decimal("150.00")

    def test_position_validation_zero_quantity(self):
        """Test Position validation with zero quantity."""
        with pytest.raises(InvalidPositionError, match="Position quantity must be positive"):
            Position(
                portfolio_id=uuid4(),
                symbol="AAPL",
                qty=Decimal("0"),  # Invalid quantity
                avg_cost=Decimal("150.00"),
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            )

    def test_position_validation_negative_avg_cost(self):
        """Test Position validation with negative average cost."""
        with pytest.raises(InvalidPositionError, match="Average cost must be positive"):
            Position(
                portfolio_id=uuid4(),
                symbol="AAPL",
                qty=Decimal("100"),
                avg_cost=Decimal("-150.00"),  # Invalid cost
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            )

    def test_position_market_value_calculation(self, sample_position: Position):
        """Test market value calculation."""
        current_price = Decimal("160.00")
        market_value = sample_position.market_value(current_price)
        expected = Decimal("100") * Decimal("160.00")
        assert market_value == expected

    def test_position_unrealized_pnl_calculation(self, sample_position: Position):
        """Test unrealized P&L calculation."""
        current_price = Decimal("160.00")
        pnl = sample_position.unrealized_pnl(current_price)

        market_value = Decimal("100") * Decimal("160.00")  # 16000
        cost_basis = Decimal("100") * Decimal("150.00")    # 15000
        expected = market_value - cost_basis                # 1000

        assert pnl == expected

    def test_position_unrealized_pnl_percentage(self, sample_position: Position):
        """Test unrealized P&L percentage calculation."""
        current_price = Decimal("160.00")
        pnl_pct = sample_position.unrealized_pnl_pct(current_price)

        expected = (Decimal("160.00") - Decimal("150.00")) / Decimal("150.00")
        assert abs(pnl_pct - expected) < Decimal("0.0001")

    def test_position_cost_basis_calculation(self, sample_position: Position):
        """Test cost basis calculation."""
        cost_basis = sample_position.cost_basis()
        expected = Decimal("100") * Decimal("150.00")
        assert cost_basis == expected

    def test_position_add_shares(self, sample_position: Position):
        """Test adding shares to position."""
        original_qty = sample_position.qty
        original_cost = sample_position.avg_cost

        # Add 50 shares at $160
        sample_position.add_shares(Decimal("50"), Decimal("160.00"))

        # New quantity should be 150
        assert sample_position.qty == Decimal("150")

        # New average cost should be weighted average
        total_cost = (original_qty * original_cost) + (Decimal("50") * Decimal("160.00"))
        expected_avg = total_cost / Decimal("150")
        assert sample_position.avg_cost == expected_avg

    def test_position_reduce_shares_partial(self, sample_position: Position):
        """Test reducing shares partially."""
        original_qty = sample_position.qty
        original_cost = sample_position.avg_cost

        # Reduce by 30 shares
        sample_position.reduce_shares(Decimal("30"))

        assert sample_position.qty == original_qty - Decimal("30")
        assert sample_position.avg_cost == original_cost  # Cost basis unchanged

    def test_position_reduce_shares_complete(self, sample_position: Position):
        """Test reducing all shares."""
        sample_position.reduce_shares(sample_position.qty)
        assert sample_position.qty == Decimal("0")

    def test_position_reduce_shares_oversell(self, sample_position: Position):
        """Test reducing more shares than available."""
        with pytest.raises(InvalidPositionError, match="Cannot reduce position"):
            sample_position.reduce_shares(Decimal("150"))  # More than the 100 available


class TestBrokerProfile:
    """Test cases for BrokerProfile entity."""

    def test_broker_profile_creation_valid(self, sample_broker_profile: BrokerProfile):
        """Test creating a valid BrokerProfile."""
        assert sample_broker_profile.broker_id == "ROBINHOOD"
        assert sample_broker_profile.name == "Robinhood"
        assert sample_broker_profile.pip_pct == Decimal("0.001")
        assert sample_broker_profile.fee_flat == Decimal("0.00")
        assert sample_broker_profile.fee_pct == Decimal("0.000")
        assert "USD" in sample_broker_profile.supported_currencies
        assert sample_broker_profile.supports_fractional is True

    def test_broker_profile_validation_empty_id(self):
        """Test BrokerProfile validation with empty ID."""
        with pytest.raises(DomainValidationError, match="Broker ID cannot be empty"):
            BrokerProfile(
                broker_id="",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            )

    def test_broker_profile_validation_negative_fees(self):
        """Test BrokerProfile validation with negative fees."""
        with pytest.raises(DomainValidationError, match="Fee percentages must be non-negative"):
            BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("-0.001"),  # Invalid
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            )

    def test_broker_profile_calculate_total_cost_buy(self, sample_broker_profile: BrokerProfile):
        """Test calculating total cost for buy order."""
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=sample_broker_profile.pip_pct,
            fee_flat=sample_broker_profile.fee_flat,
            fee_pct=sample_broker_profile.fee_pct,
            unit="share",
            price_ccy="USD",
            comment="Test"
        )

        total_cost = sample_broker_profile.calculate_total_cost(trade)
        gross = Decimal("100") * Decimal("150.00")  # 15000
        pip_cost = gross * Decimal("0.001")         # 15.00
        expected = gross + pip_cost                 # 15015.00

        assert total_cost == expected

    def test_broker_profile_supports_currency(self, sample_broker_profile: BrokerProfile):
        """Test currency support checking."""
        assert sample_broker_profile.supports_currency("USD") is True
        assert sample_broker_profile.supports_currency("EUR") is False

    def test_broker_profile_can_execute_fractional(self, sample_broker_profile: BrokerProfile, sample_ibkr_broker: BrokerProfile):
        """Test fractional share execution capability."""
        fractional_qty = Decimal("10.5")
        whole_qty = Decimal("10")

        # Robinhood supports fractional
        assert sample_broker_profile.can_execute_order(fractional_qty, Decimal("150.00")) is True
        assert sample_broker_profile.can_execute_order(whole_qty, Decimal("150.00")) is True

        # IBKR doesn't support fractional
        assert sample_ibkr_broker.can_execute_order(fractional_qty, Decimal("150.00")) is False
        assert sample_ibkr_broker.can_execute_order(whole_qty, Decimal("150.00")) is True

    def test_broker_profile_min_order_value_check(self, sample_broker_profile: BrokerProfile):
        """Test minimum order value validation."""
        # Order below minimum
        assert sample_broker_profile.can_execute_order(Decimal("0.001"), Decimal("150.00")) is False

        # Order above minimum
        assert sample_broker_profile.can_execute_order(Decimal("1"), Decimal("150.00")) is True


class TestEnums:
    """Test cases for enums and constants."""

    def test_asset_type_enum(self):
        """Test AssetType enum values."""
        assert AssetType.STOCK.value == "STOCK"
        assert AssetType.ETF.value == "ETF"
        assert AssetType.CRYPTO.value == "CRYPTO"
        assert AssetType.COMMODITY.value == "COMMODITY"

    def test_trade_side_enum(self):
        """Test TradeSide enum values."""
        assert TradeSide.BUY.value == "BUY"
        assert TradeSide.SELL.value == "SELL"

    def test_enum_string_conversion(self):
        """Test enum string representations."""
        assert str(AssetType.STOCK) == "AssetType.STOCK"
        assert str(TradeSide.BUY) == "TradeSide.BUY"


# Additional test cases for improved coverage
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


# Additional edge case tests for complete domain validation coverage
class TestAssetSnapshotValidationEdgeCases:
    """Tests for AssetSnapshot validation edge cases to achieve 100% coverage."""

    def test_asset_snapshot_high_less_than_close_validation(self):
        """Test AssetSnapshot validation when high is less than close."""
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

    def test_asset_snapshot_low_greater_than_open_validation(self):
        """Test AssetSnapshot validation when low is greater than open."""
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("95.00"),  # open < low (98.00)
                high=Decimal("105.00"),
                low=Decimal("98.00"),
                close=Decimal("100.00"),
                volume=1000
            )

    def test_asset_snapshot_low_greater_than_close_validation(self):
        """Test AssetSnapshot validation when low is greater than close."""
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("98.00"),  # low > close (95.00)
                close=Decimal("95.00"),
                volume=1000
            )


class TestPositionValidationEdgeCases:
    """Tests for Position validation edge cases to achieve 100% coverage."""

    def test_position_validation_insufficient_shares_exact_match(self):
        """Test position validation when trying to reduce exact amount of shares."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("50"),  # Exactly 50 shares
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        # Try to reduce exactly 50 shares (should work)
        position.reduce_shares(Decimal("50"))
        assert position.qty == Decimal("0")

    def test_position_validation_insufficient_shares_boundary(self):
        """Test position validation at exact boundary conditions."""
        position = Position(
            portfolio_id=uuid4(),
            symbol="AAPL",
            qty=Decimal("50.01"),  # Just over 50 shares
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        # Try to reduce exactly 50.01 shares (should work)
        position.reduce_shares(Decimal("50.01"))
        assert position.qty == Decimal("0")


class TestTradeCalculationEdgeCases:
    """Tests for Trade calculation edge cases to achieve 100% coverage."""

    def test_trade_zero_pip_percentage(self):
        """Test trade calculations with zero pip percentage."""
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.000"),  # Zero pip percentage
            fee_flat=Decimal("5.00"),
            fee_pct=Decimal("0.002"),
            unit="share",
            price_ccy="USD",
            comment="Test zero pip"
        )

        # Pip cost should be zero
        assert trade.pip_cost() == Decimal("0")

        # Net amount should only include gross amount and fees (no pip cost)
        gross = trade.gross_amount()
        fees = trade.total_fees()
        expected_net = gross + fees  # Buy trade adds fees
        assert trade.net_amount() == expected_net
