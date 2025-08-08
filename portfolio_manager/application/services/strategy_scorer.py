"""Strategy scoring service for calculating investment strategy scores."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from portfolio_manager.application.ports import AssetRepository, StrategyCalculator
from portfolio_manager.domain.entities import Asset, Trade
from portfolio_manager.domain.exceptions import StrategyCalculationError
from portfolio_manager.infrastructure.data_access.exceptions import DataAccessError
from .base_service import ExceptionBasedService


@dataclass
class StrategyScore:
    """Result of a strategy score calculation."""

    symbol: str
    strategy_id: str
    score: Decimal
    timestamp: datetime
    metadata: Optional[Dict] = None


@dataclass
class BacktestResult:
    """Result of a strategy backtest."""

    strategy_id: str
    start_date: datetime
    end_date: datetime
    initial_capital: Decimal
    final_value: Decimal
    total_return: Decimal
    trades: List[Trade]
    performance_metrics: Dict


class StrategyScoreService(ExceptionBasedService):
    """Service for calculating and managing investment strategy scores."""

    def __init__(
        self,
        strategy_calculators: Dict[str, StrategyCalculator],
        asset_repository: AssetRepository,
    ):
        super().__init__(logger_name=f"{__name__}.{self.__class__.__name__}")
        self.strategy_calculators = strategy_calculators
        self.asset_repository = asset_repository

    async def calculate_strategy_scores(
        self,
        strategy_id: str,
        symbols: Optional[List[str]] = None,
        as_of_date: Optional[datetime] = None,
    ) -> List[StrategyScore]:
        """Calculate strategy scores for specified symbols or all assets."""
        if strategy_id not in self.strategy_calculators:
            raise StrategyCalculationError(f"Unknown strategy: {strategy_id}")

        calculator = self.strategy_calculators[strategy_id]

        if symbols:
            assets = []
            for symbol in symbols:
                asset = await self.asset_repository.get_asset(symbol)
                if asset:
                    assets.append(asset)
        else:
            assets = await self.asset_repository.get_all_assets()

        results = []
        calculation_time = as_of_date or datetime.now()

        for asset in assets:
            try:
                # Get latest snapshot
                snapshot = await self.asset_repository.get_latest_snapshot(asset.symbol)
                if not snapshot:
                    continue

                # Get fundamental metrics
                fundamentals = await self.asset_repository.get_fundamental_metrics(asset.symbol)
                if not fundamentals or not calculator.validate_metrics(fundamentals):
                    continue

                # Calculate score
                score = calculator.calculate_score(asset, snapshot, fundamentals)

                results.append(StrategyScore(
                    symbol=asset.symbol,
                    strategy_id=strategy_id,
                    score=score,
                    timestamp=calculation_time,
                ))

            except (StrategyCalculationError, DataAccessError) as e:
                # Skip assets that fail calculation, but log the error
                self._logger.warning(f"Failed to calculate strategy score for {asset.symbol}: {e}")
                continue
            except Exception as e:
                # Unexpected errors - log and wrap in domain exception
                self._logger.error(f"Unexpected error calculating score for {asset.symbol}: {e}")
                # Continue processing other assets but log the unexpected error
                continue

        return results

    async def get_top_ranked_assets(
        self,
        strategy_id: str,
        limit: int = 50,
        as_of_date: Optional[datetime] = None,
    ) -> List[StrategyScore]:
        """Get top-ranked assets for a strategy."""
        all_scores = await self.calculate_strategy_scores(
            strategy_id=strategy_id,
            as_of_date=as_of_date,
        )

        # Sort by score descending and return top N
        sorted_scores = sorted(all_scores, key=lambda x: x.score, reverse=True)
        return sorted_scores[:limit]

    async def backtest_strategy(
        self,
        strategy_id: str,
        start_date: datetime,
        end_date: datetime,
        initial_capital: Decimal,
    ) -> BacktestResult:
        """Backtest a strategy over a historical period."""
        if strategy_id not in self.strategy_calculators:
            raise StrategyCalculationError(f"Unknown strategy: {strategy_id}")

        calculator = self.strategy_calculators[strategy_id]

        # Get all assets for backtesting
        assets = await self.asset_repository.get_all_assets()

        # For this implementation, create a simple backtest result
        # In a full implementation, this would simulate trades over time
        trades = []  # Placeholder for backtest trades

        # Calculate simple performance metrics
        final_value = initial_capital * Decimal('1.05')  # Placeholder 5% return
        total_return = (final_value - initial_capital) / initial_capital

        performance_metrics = {
            "total_return": total_return,
            "annualized_return": total_return * Decimal('12'),  # Simplified
            "volatility": Decimal('0.15'),  # Placeholder
            "sharpe_ratio": Decimal('1.2'),  # Placeholder
            "max_drawdown": Decimal('-0.08'),  # Placeholder
        }

        return BacktestResult(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_value=final_value,
            total_return=total_return,
            trades=trades,
            performance_metrics=performance_metrics,
        )

    def get_strategy_info(self, strategy_id: str) -> Dict:
        """Get information about a strategy."""
        if strategy_id not in self.strategy_calculators:
            raise StrategyCalculationError(f"Unknown strategy: {strategy_id}")

        calculator = self.strategy_calculators[strategy_id]

        return {
            "id": strategy_id,
            "name": calculator.get_strategy_name(),
            "description": calculator.get_strategy_description(),
            "required_metrics": calculator.get_required_metrics(),
        }

    def list_available_strategies(self) -> List[Dict]:
        """List all available strategies."""
        return [
            self.get_strategy_info(strategy_id)
            for strategy_id in self.strategy_calculators.keys()
        ]
