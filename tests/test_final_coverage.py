"""Final tests to achieve 100% coverage on all remaining lines."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from stockapp.application.services.strategy_scorer import StrategyScoreService
from stockapp.domain.entities import Asset, AssetSnapshot, AssetType, BrokerProfile
from stockapp.domain.exceptions import DomainValidationError


class TestFinalCoverageTests:
    """Final tests to cover the last remaining lines."""

    def test_asset_snapshot_high_less_than_open_validation(self):
        """Test AssetSnapshot validation when high < open."""
        base_time = datetime.now(timezone.utc)
        
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("98.00"),  # high < open (line 98)
                low=Decimal("95.00"),
                close=Decimal("97.00"),
                volume=1000
            )

    def test_asset_snapshot_low_greater_than_close_validation(self):
        """Test AssetSnapshot validation when low > close."""
        base_time = datetime.now(timezone.utc)
        
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("99.00"),  # low > close (line 105)
                close=Decimal("98.00"),
                volume=1000
            )

    def test_broker_profile_fractional_shares_edge_case(self):
        """Test broker profile fractional shares detection with edge case."""
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
        
        # Test quantity that equals its integral value (whole number)
        # This specifically tests line 334 where we check if quantity != quantity.to_integral_value()
        whole_quantity = Decimal("10")  # This equals Decimal("10").to_integral_value()
        fractional_quantity = Decimal("10.5")  # This doesn't equal Decimal("10.5").to_integral_value()
        
        # Should allow whole numbers
        assert broker.can_execute_order(whole_quantity, Decimal("100.00")) is True
        
        # Should reject fractional numbers when not supported
        assert broker.can_execute_order(fractional_quantity, Decimal("100.00")) is False

    @pytest.mark.asyncio
    async def test_strategy_score_service_asset_calculation_skip(self):
        """Test strategy score service skipping assets during calculation exceptions."""
        from stockapp.application.ports import AssetRepository, StrategyCalculator
        
        # Create mock calculator that throws exception on calculate_score
        mock_calculator = Mock(spec=StrategyCalculator)
        mock_calculator.validate_metrics = Mock(return_value=True)
        mock_calculator.calculate_score = Mock(side_effect=Exception("Calculation failed"))
        
        # Create mock repository
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
            "pe_ratio": Decimal("25.0"),
            "dividend_yield": Decimal("0.02")
        })
        
        service = StrategyScoreService(
            strategy_calculators={"TEST": mock_calculator},
            asset_repository=mock_repo
        )
        
        # This should hit line 78 in strategy_scorer.py (continue statement)
        results = await service.calculate_strategy_scores("TEST")
        
        # Should return empty results since calculation failed and was skipped
        assert len(results) == 0