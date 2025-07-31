"""Edge case tests to achieve maximum coverage."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from stockapp.application.services.portfolio_simulator import PortfolioSimulatorService
from stockapp.application.services.strategy_scorer import StrategyScoreService
from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    BrokerProfile,
    Portfolio,
    Position,
    TradeSide,
)
from stockapp.domain.exceptions import DomainValidationError


class TestAssetSnapshotEdgeCases:
    """Test AssetSnapshot validation edge cases for full coverage."""

    def test_asset_snapshot_high_less_than_close(self):
        """Test AssetSnapshot validation when high < close."""
        base_time = datetime.now(timezone.utc)
        
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("98.00"),  # high < close
                low=Decimal("95.00"),
                close=Decimal("99.00"),
                volume=1000
            )

    def test_asset_snapshot_low_greater_than_open(self):
        """Test AssetSnapshot validation when low > open."""
        base_time = datetime.now(timezone.utc)
        
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("102.00"),  # low > open
                close=Decimal("103.00"),
                volume=1000
            )

    def test_asset_snapshot_low_greater_than_close(self):
        """Test AssetSnapshot validation when low > close."""
        base_time = datetime.now(timezone.utc)
        
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("103.00"),  # low > close
                close=Decimal("102.00"),
                volume=1000
            )


class TestBrokerProfileEdgeCases:
    """Test BrokerProfile edge cases for full coverage."""

    def test_broker_profile_fractional_detection_whole_number(self):
        """Test fractional share detection with whole numbers."""
        broker = BrokerProfile(
            broker_id="TEST",
            name="Test Broker",
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            min_order_value=Decimal("1.00"),
            supported_currencies=["USD"],
            supports_fractional=False
        )
        
        # Test with a decimal that represents a whole number
        whole_number_decimal = Decimal("10.0")
        assert broker.can_execute_order(whole_number_decimal, Decimal("50.00")) is True
        
        # Test with actual fractional
        fractional_decimal = Decimal("10.1")
        assert broker.can_execute_order(fractional_decimal, Decimal("50.00")) is False

    def test_broker_profile_calculate_total_cost_sell_trade(self):
        """Test broker profile total cost calculation for sell trades."""
        from stockapp.domain.entities import Trade
        
        broker = BrokerProfile(
            broker_id="TEST",
            name="Test Broker",
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("5.00"),
            fee_pct=Decimal("0.001"),
            min_order_value=Decimal("1.00"),
            supported_currencies=["USD"],
            supports_fractional=True
        )
        
        sell_trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.SELL,
            qty=Decimal("100"),
            price=Decimal("150.00"),
            pip_pct=broker.pip_pct,
            fee_flat=broker.fee_flat,
            fee_pct=broker.fee_pct,
            unit="share",
            price_ccy="USD",
            comment="Test sell"
        )
        
        total_cost = broker.calculate_total_cost(sell_trade)
        # For sell trades, should return absolute value of net proceeds
        expected_net = sell_trade.net_amount()  # This is positive for sells
        assert total_cost == abs(expected_net)


class TestPortfolioSimulatorEdgeCases:
    """Test PortfolioSimulator edge cases for full coverage."""

    @pytest.mark.asyncio
    async def test_execute_trade_generic_exception(self):
        """Test execute_trade with generic exception handling."""
        from stockapp.application.ports import AssetRepository, PortfolioRepository
        
        mock_portfolio_repo = Mock(spec=PortfolioRepository)
        mock_asset_repo = Mock(spec=AssetRepository)
        
        # Make get_portfolio raise a generic exception
        mock_portfolio_repo.get_portfolio = AsyncMock(side_effect=Exception("Generic error"))
        
        simulator = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )
        
        result = await simulator.execute_trade(
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            )
        )
        
        assert result.success is False
        assert isinstance(result.error, Exception)

    @pytest.mark.asyncio
    async def test_buy_trade_generic_exception(self):
        """Test _execute_buy_trade with generic exception."""
        from stockapp.application.ports import AssetRepository, PortfolioRepository
        
        mock_portfolio_repo = Mock(spec=PortfolioRepository)
        mock_asset_repo = Mock(spec=AssetRepository)
        
        # Make save_trade raise an exception
        mock_portfolio_repo.save_trade = AsyncMock(side_effect=Exception("Save error"))
        
        simulator = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )
        
        # Create a valid trade and portfolio
        from stockapp.domain.entities import Trade
        
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.BUY,
            qty=Decimal("10"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Test"
        )
        
        portfolio = Portfolio(
            portfolio_id=trade.portfolio_id,
            name="Test",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        
        result = await simulator._execute_buy_trade(trade, portfolio)
        
        assert result.success is False
        assert isinstance(result.error, Exception)

    @pytest.mark.asyncio
    async def test_sell_trade_generic_exception(self):
        """Test _execute_sell_trade with generic exception."""
        from stockapp.application.ports import AssetRepository, PortfolioRepository
        
        mock_portfolio_repo = Mock(spec=PortfolioRepository)
        mock_asset_repo = Mock(spec=AssetRepository)
        
        # Make save_trade raise an exception  
        mock_portfolio_repo.save_trade = AsyncMock(side_effect=Exception("Save error"))
        
        simulator = PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repo,
            asset_repository=mock_asset_repo
        )
        
        # Create valid trade, portfolio, and position
        from stockapp.domain.entities import Trade
        
        trade = Trade(
            trade_id=uuid4(),
            portfolio_id=uuid4(),
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            side=TradeSide.SELL,
            qty=Decimal("10"),
            price=Decimal("150.00"),
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            unit="share",
            price_ccy="USD",
            comment="Test"
        )
        
        portfolio = Portfolio(
            portfolio_id=trade.portfolio_id,
            name="Test",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )
        
        result = await simulator._execute_sell_trade(trade, portfolio)
        
        assert result.success is False
        assert isinstance(result.error, Exception)


class TestStrategyScoreServiceEdgeCases:
    """Test StrategyScoreService edge cases for full coverage."""

    @pytest.mark.asyncio
    async def test_calculate_strategy_scores_calculation_exception(self):
        """Test strategy score calculation with exception during calculation."""
        from stockapp.application.ports import AssetRepository, StrategyCalculator
        
        mock_calculator = Mock(spec=StrategyCalculator)
        mock_calculator.calculate_score = Mock(side_effect=Exception("Calculation error"))
        mock_calculator.validate_metrics = Mock(return_value=True)
        
        mock_repo = Mock(spec=AssetRepository)
        mock_repo.get_all_assets = AsyncMock(return_value=[
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple")
        ])
        mock_repo.get_latest_snapshot = AsyncMock(return_value=AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        ))
        mock_repo.get_fundamental_metrics = AsyncMock(return_value={
            "pe_ratio": Decimal("25.0")
        })
        
        service = StrategyScoreService(
            strategy_calculators={"TEST": mock_calculator},
            asset_repository=mock_repo
        )
        
        # Should handle calculation exceptions gracefully
        results = await service.calculate_strategy_scores("TEST")
        assert len(results) == 0  # Should skip assets that fail calculation

    def test_get_strategy_info_missing_strategy(self):
        """Test getting info for non-existent strategy."""
        from stockapp.application.ports import AssetRepository
        from stockapp.domain.exceptions import StrategyCalculationError
        
        mock_repo = Mock(spec=AssetRepository)
        service = StrategyScoreService(
            strategy_calculators={},
            asset_repository=mock_repo
        )
        
        with pytest.raises(StrategyCalculationError, match="Unknown strategy"):
            service.get_strategy_info("NONEXISTENT")

    @pytest.mark.asyncio
    async def test_backtest_strategy_missing_strategy(self):
        """Test backtesting non-existent strategy."""
        from stockapp.application.ports import AssetRepository
        from stockapp.domain.exceptions import StrategyCalculationError
        
        mock_repo = Mock(spec=AssetRepository)
        service = StrategyScoreService(
            strategy_calculators={},
            asset_repository=mock_repo
        )
        
        with pytest.raises(StrategyCalculationError, match="Unknown strategy"):
            await service.backtest_strategy(
                strategy_id="NONEXISTENT",
                start_date=datetime.now(timezone.utc),
                end_date=datetime.now(timezone.utc),
                initial_capital=Decimal("10000.00")
            )