"""Application services implementing use cases."""

from .base_service import (
    BaseApplicationService,
    DependencyContainer,
    ExceptionBasedService,
    ResultBasedService,
    ServiceErrorStrategy,
    ServiceMetrics,
    ServiceResult,
)
from .data_ingestion import DataIngestionService, IngestionResult
from .portfolio_simulator import (
    PortfolioMetrics,
    PortfolioSimulatorService,
    TradeResult,
)
from .strategy_scorer import BacktestResult, StrategyScore, StrategyScoreService

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
