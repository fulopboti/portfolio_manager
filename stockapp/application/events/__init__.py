"""
Application layer event handlers.

This package contains event handlers that implement business logic
in response to domain events. These handlers coordinate between
domain services and infrastructure components.
"""

from .event_system import EventSystem
from .trade_handlers import TradeExecutedEventHandler, PortfolioMetricsEventHandler
from .price_handlers import AssetPriceUpdatedEventHandler, PortfolioRevaluationEventHandler
from .rebalancing_handlers import (
    PortfolioRebalancedEventHandler,
    RebalancingMetricsEventHandler,
    RebalancingNotificationEventHandler,
)
from .risk_handlers import RiskThresholdBreachedEventHandler, RiskMitigationEventHandler
from .market_data_handlers import (
    MarketDataReceivedEventHandler,
    MarketDataQualityEventHandler,
    MarketDataCachingEventHandler,
)

__all__ = [
    "EventSystem",
    # Trade handlers
    "TradeExecutedEventHandler",
    "PortfolioMetricsEventHandler",
    # Price handlers
    "AssetPriceUpdatedEventHandler",
    "PortfolioRevaluationEventHandler", 
    # Rebalancing handlers
    "PortfolioRebalancedEventHandler",
    "RebalancingMetricsEventHandler",
    "RebalancingNotificationEventHandler",
    # Risk handlers
    "RiskThresholdBreachedEventHandler",
    "RiskMitigationEventHandler",
    # Market data handlers
    "MarketDataReceivedEventHandler",
    "MarketDataQualityEventHandler",
    "MarketDataCachingEventHandler",
]