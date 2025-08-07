"""Application services implementing use cases."""

from .base_service import (
    BaseApplicationService,
    ResultBasedService,
    ExceptionBasedService,
    ServiceResult,
    ServiceErrorStrategy,
    ServiceMetrics,
    DependencyContainer,
)
from .portfolio_simulator import PortfolioSimulatorService, TradeResult, PortfolioMetrics
from .data_ingestion import DataIngestionService, IngestionResult
from .strategy_scorer import StrategyScoreService, StrategyScore, BacktestResult

__all__ = [
    # Base service classes
    "BaseApplicationService",
    "ResultBasedService",
    "ExceptionBasedService",
    "ServiceResult",
    "ServiceErrorStrategy",
    "ServiceMetrics",
    "DependencyContainer",
    # Concrete service classes
    "PortfolioSimulatorService",
    "DataIngestionService",
    "StrategyScoreService",
    # Result types
    "TradeResult",
    "PortfolioMetrics",
    "IngestionResult",
    "StrategyScore",
    "BacktestResult",
]