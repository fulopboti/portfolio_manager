"""
Event handlers for risk management and threshold monitoring.

This module contains handlers that process risk-related events and
coordinate risk mitigation actions.
"""

from decimal import Decimal
from typing import Any

from ...domain.events import RiskThresholdBreachedEvent
from .base_handler import BaseEventHandler, ErrorHandlingStrategy


class RiskThresholdBreachedEventHandler(BaseEventHandler):
    """Handler for risk threshold breach events."""

    def __init__(self, portfolio_repository, risk_service, alert_service):
        """
        Initialize the risk threshold breach handler.

        Args:
            portfolio_repository: Repository for portfolio data access
            risk_service: Service for risk calculations and management
            alert_service: Service for sending risk alerts
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.CRITICAL)
        self.portfolio_repository = portfolio_repository
        self.risk_service = risk_service
        self.alert_service = alert_service

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler can process the event."""
        return isinstance(event, RiskThresholdBreachedEvent)

    async def _handle_event(self, event: RiskThresholdBreachedEvent) -> None:
        """
        Handle a risk threshold breach event.

        This processes the breach by:
        1. Validating portfolio exists
        2. Escalating based on severity
        3. Triggering risk mitigation if needed
        4. Logging and alerting

        Args:
            event: The risk threshold breached event to process
        """
        # Validate portfolio exists
        portfolio = await self.portfolio_repository.get_portfolio(event.portfolio_id)
        if not portfolio:
            raise ValueError(f"Portfolio {event.portfolio_id} not found")

        # Handle breach based on severity
        if event.is_critical_breach():
            await self._handle_critical_breach(event, portfolio)
        else:
            await self._handle_standard_breach(event, portfolio)

        # Update portfolio risk status
        await self._update_portfolio_risk_status(event, portfolio)

        # Send risk alerts
        await self._send_risk_alerts(event)

    async def _handle_critical_breach(
        self, event: RiskThresholdBreachedEvent, portfolio
    ) -> None:
        """
        Handle critical risk threshold breaches.

        Args:
            event: The risk breach event
            portfolio: The affected portfolio
        """
        self._logger.critical(
            f"CRITICAL RISK BREACH for portfolio {event.portfolio_id}: "
            f"{event.threshold_type} breach of {event.breach_amount()}"
        )

        # For critical breaches, trigger immediate risk mitigation
        await self.risk_service.trigger_emergency_risk_mitigation(
            portfolio_id=event.portfolio_id,
            breach_type=event.threshold_type,
            severity=event.severity,
        )

        # Mark portfolio for urgent review
        portfolio.risk_status = "CRITICAL"
        portfolio.requires_review = True
        portfolio.last_updated = event.timestamp

        await self.portfolio_repository.save_portfolio(portfolio)

    async def _handle_standard_breach(
        self, event: RiskThresholdBreachedEvent, portfolio
    ) -> None:
        """
        Handle standard risk threshold breaches.

        Args:
            event: The risk breach event
            portfolio: The affected portfolio
        """
        # For standard breaches, schedule risk review
        await self.risk_service.schedule_risk_review(
            portfolio_id=event.portfolio_id,
            breach_type=event.threshold_type,
            severity=event.severity,
            review_priority=event.severity.upper(),
        )

        # Update portfolio risk status
        if event.severity.upper() in ["HIGH", "MEDIUM"]:
            portfolio.risk_status = event.severity.upper()
            portfolio.last_updated = event.timestamp
            await self.portfolio_repository.save_portfolio(portfolio)

    async def _update_portfolio_risk_status(
        self, event: RiskThresholdBreachedEvent, portfolio
    ) -> None:
        """
        Update the portfolio's risk tracking information.

        Args:
            event: The risk breach event
            portfolio: The affected portfolio
        """
        # Add breach to portfolio risk history
        risk_breach_record = {
            "timestamp": event.timestamp,
            "threshold_type": event.threshold_type,
            "threshold_value": event.threshold_value,
            "current_value": event.current_value,
            "severity": event.severity,
            "breach_amount": event.breach_amount(),
        }

        # Store breach history (would be implemented in portfolio entity)
        if not hasattr(portfolio, "risk_breaches"):
            portfolio.risk_breaches = []

        portfolio.risk_breaches.append(risk_breach_record)
        portfolio.last_risk_event = event.timestamp

        await self.portfolio_repository.save_portfolio(portfolio)

    async def _send_risk_alerts(self, event: RiskThresholdBreachedEvent) -> None:
        """
        Send appropriate risk alerts based on breach severity.

        Args:
            event: The risk breach event
        """
        try:
            await self.alert_service.send_risk_breach_alert(
                portfolio_id=event.portfolio_id,
                threshold_type=event.threshold_type,
                current_value=event.current_value,
                threshold_value=event.threshold_value,
                severity=event.severity,
                timestamp=event.timestamp,
            )

        except Exception as e:
            self._logger.warning(f"Failed to send risk alert: {e}")


class RiskMitigationEventHandler(BaseEventHandler):
    """Handler for coordinating risk mitigation actions."""

    def __init__(self, trading_service, position_service, notification_service):
        """
        Initialize the risk mitigation handler.

        Args:
            trading_service: Service for executing trades
            position_service: Service for position management
            notification_service: Service for notifications
        """
        super().__init__(error_strategy=ErrorHandlingStrategy.RESILIENT)
        self.trading_service = trading_service
        self.position_service = position_service
        self.notification_service = notification_service

    async def can_handle(self, event: Any) -> bool:
        """Check if this handler processes risk threshold events."""
        return isinstance(event, RiskThresholdBreachedEvent)

    async def _handle_event(self, event: RiskThresholdBreachedEvent) -> None:
        """
        Coordinate risk mitigation actions based on breach type and severity.

        Args:
            event: The risk threshold breached event
        """
        if not event.is_critical_breach():
            return  # Only handle critical breaches for automatic mitigation

        # Determine mitigation strategy based on breach type
        mitigation_actions = await self._determine_mitigation_actions(event)

        # Execute mitigation actions
        for action in mitigation_actions:
            await self._execute_mitigation_action(event.portfolio_id, action)

        # Notify stakeholders
        await self._notify_mitigation_actions(event, mitigation_actions)

    async def _determine_mitigation_actions(
        self, event: RiskThresholdBreachedEvent
    ) -> list:
        """
        Determine appropriate mitigation actions for the risk breach.

        Args:
            event: The risk breach event

        Returns:
            List of mitigation actions to execute
        """
        actions = []

        if event.threshold_type == "MAX_POSITION_SIZE":
            actions.append(
                {
                    "type": "REDUCE_POSITION",
                    "target_reduction": min(
                        event.breach_amount() * Decimal("1.1"),
                        event.current_value * Decimal("0.5"),
                    ),
                }
            )
        elif event.threshold_type == "MAX_PORTFOLIO_VOLATILITY":
            actions.append(
                {
                    "type": "HEDGE_PORTFOLIO",
                    "hedge_ratio": min(
                        Decimal("0.5"), event.breach_amount() / event.threshold_value
                    ),
                }
            )
        elif event.threshold_type == "MAX_DRAWDOWN":
            actions.append(
                {
                    "type": "STOP_LOSS_ACTIVATION",
                    "stop_level": event.current_value * Decimal("0.95"),
                }
            )

        return actions

    async def _execute_mitigation_action(self, portfolio_id: str, action: dict) -> None:
        """
        Execute a specific mitigation action.

        Args:
            portfolio_id: The portfolio ID
            action: The mitigation action to execute
        """
        action_type = action["type"]

        if action_type == "REDUCE_POSITION":
            await self.position_service.reduce_largest_positions(
                portfolio_id, action["target_reduction"]
            )
        elif action_type == "HEDGE_PORTFOLIO":
            await self.trading_service.create_hedge_positions(
                portfolio_id, action["hedge_ratio"]
            )
        elif action_type == "STOP_LOSS_ACTIVATION":
            await self.trading_service.activate_stop_losses(
                portfolio_id, action["stop_level"]
            )

    async def _notify_mitigation_actions(
        self, event: RiskThresholdBreachedEvent, actions: list
    ) -> None:
        """
        Notify stakeholders about executed mitigation actions.

        Args:
            event: The risk breach event
            actions: The executed mitigation actions
        """
        try:
            await self.notification_service.send_risk_mitigation_notification(
                portfolio_id=event.portfolio_id,
                breach_type=event.threshold_type,
                severity=event.severity,
                actions_taken=actions,
                timestamp=event.timestamp,
            )

        except Exception as e:
            self._logger.warning(f"Failed to send mitigation notification: {e}")
