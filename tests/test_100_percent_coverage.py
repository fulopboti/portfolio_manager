"""Final tests to achieve 100% test coverage."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest

from stockapp.application.services.strategy_scorer import StrategyScoreService
from stockapp.domain.entities import AssetSnapshot, BrokerProfile
from stockapp.domain.exceptions import DomainValidationError


class TestFinal100PercentCoverage:
    """Tests to cover the final remaining lines for 100% coverage."""

    def test_asset_snapshot_high_less_than_open_exact_line_98(self):
        """Test the exact validation on line 98 in entities.py."""
        base_time = datetime.now(timezone.utc)
        
        # This should trigger line 98: if self.high < self.open:
        with pytest.raises(DomainValidationError, match="High price must be >= low price"):
            AssetSnapshot(
                symbol="TEST",
                timestamp=base_time,
                open=Decimal("101.00"),  # open > high, should trigger line 98
                high=Decimal("100.00"),  # high < open
                low=Decimal("99.00"),
                close=Decimal("100.00"),
                volume=1000
            )

    def test_broker_profile_fractional_check_exact_line_334(self):
        """Test the exact fractional share check on line 334."""
        # Test line 334: if not self.supports_fractional and quantity != quantity.to_integral_value():
        broker = BrokerProfile(
            broker_id="TEST",
            name="Test Broker", 
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            min_order_value=Decimal("1.00"),
            supported_currencies=["USD"],
            supports_fractional=False  # This will trigger the fractional check
        )
        
        # Create a quantity that is NOT equal to its integral value
        fractional_qty = Decimal("10.75")
        integral_value = fractional_qty.to_integral_value()  # This would be Decimal("10")
        
        # Verify they're not equal (this is the condition on line 334)
        assert fractional_qty != integral_value
        
        # This should return False because fractional shares aren't supported
        result = broker.can_execute_order(fractional_qty, Decimal("100.00"))
        assert result is False

    def test_strategy_scorer_exception_continue_line_78(self):
        """Test the continue statement on line 78 in strategy_scorer.py."""
        from stockapp.application.ports import AssetRepository, StrategyCalculator
        from stockapp.domain.entities import Asset, AssetType
        from unittest.mock import AsyncMock
        
        # Create a mock calculator that raises an exception
        mock_calculator = Mock(spec=StrategyCalculator)
        mock_calculator.validate_metrics = Mock(return_value=True)
        mock_calculator.calculate_score = Mock(side_effect=Exception("Score calculation failed"))
        
        # Create mock repository
        mock_repo = Mock(spec=AssetRepository)
        mock_repo.get_all_assets = AsyncMock(return_value=[
            Asset(symbol="FAIL", exchange="TEST", asset_type=AssetType.STOCK, name="Failing Asset")
        ])
        mock_repo.get_latest_snapshot = AsyncMock(return_value=AssetSnapshot(
            symbol="FAIL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("95.00"), 
            close=Decimal("102.00"),
            volume=1000
        ))
        mock_repo.get_fundamental_metrics = AsyncMock(return_value={
            "pe_ratio": Decimal("20.0")
        })
        
        service = StrategyScoreService(
            strategy_calculators={"FAIL": mock_calculator},
            asset_repository=mock_repo
        )
        
        # This should trigger the exception handling and continue statement on line 78
        import asyncio
        results = asyncio.run(service.calculate_strategy_scores("FAIL"))
        
        # Should return empty list since the calculation failed and was skipped
        assert len(results) == 0
        
        # Verify the calculation was attempted
        mock_calculator.calculate_score.assert_called_once()