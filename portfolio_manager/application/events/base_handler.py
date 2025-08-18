"""
Base event handler classes providing common functionality.

This module provides abstract base classes and utilities that eliminate
code duplication across event handlers and standardize error handling.
"""

import logging
from abc import abstractmethod
from enum import Enum
from typing import Any

from ...infrastructure.events.handlers import EventHandler


class ErrorHandlingStrategy(Enum):
    """Strategy for handling errors in event handlers."""

    CRITICAL = "critical"  # Re-raise all errors - failure breaks the system
    RESILIENT = "resilient"  # Log errors but don't re-raise - system continues
    SELECTIVE = "selective"  # Re-raise critical errors, suppress non-critical ones


class BaseEventHandler(EventHandler):
    """
    Base class for all domain event handlers.

    Provides common functionality for:
    - Structured logging with context
    - Standardized error handling strategies
    - Common initialization patterns
    - Metrics tracking (future enhancement)
    """

    def __init__(
        self,
        error_strategy: ErrorHandlingStrategy = ErrorHandlingStrategy.CRITICAL,
        logger_name: str | None = None,
    ):
        """
        Initialize base event handler.

        Args:
            error_strategy: How to handle errors during event processing
            logger_name: Custom logger name (defaults to class name)
        """
        self.error_strategy = error_strategy
        self._logger = logging.getLogger(logger_name or self.__class__.__name__)
        self._setup_logging_context()

    def _setup_logging_context(self) -> None:
        """Setup logging context for this handler."""
        self._log_prefix = f"[{self.__class__.__name__}]"

    @abstractmethod
    async def can_handle(self, event: Any) -> bool:
        """
        Check if this handler can process the given event.

        Args:
            event: The event to check

        Returns:
            True if this handler can process the event, False otherwise
        """
        pass

    @abstractmethod
    async def _handle_event(self, event: Any) -> None:
        """
        Handle the specific event logic.

        This method contains the core business logic for processing
        the event. It should be implemented by concrete handlers.

        Args:
            event: The event to process

        Raises:
            Exception: Any exception during event processing
        """
        pass

    async def handle(self, event: Any) -> None:
        """
        Handle an event with standardized error handling and logging.

        This method provides the common template for event handling:
        1. Log event start
        2. Execute handler-specific logic
        3. Handle errors according to strategy
        4. Log completion or failure

        Args:
            event: The event to process
        """
        event_info = self._get_event_info(event)

        try:
            self._logger.info(f"{self._log_prefix} Processing {event_info}")
            await self._handle_event(event)
            self._logger.info(f"{self._log_prefix} Successfully processed {event_info}")

        except Exception as e:
            await self._handle_error(event, e, event_info)

    async def _handle_error(
        self, event: Any, error: Exception, event_info: str
    ) -> None:
        """
        Handle errors according to the configured strategy.

        Args:
            event: The event that caused the error
            error: The exception that occurred
            event_info: String representation of event for logging

        Raises:
            Exception: Re-raises the error if strategy requires it
        """
        error_context = self._build_error_context(event, error)

        if self.error_strategy == ErrorHandlingStrategy.CRITICAL:
            self._logger.error(
                f"{self._log_prefix} Failed to process {event_info}: {error}",
                extra=error_context,
            )
            raise

        elif self.error_strategy == ErrorHandlingStrategy.RESILIENT:
            self._logger.warning(
                f"{self._log_prefix} Error processing {event_info} (continuing): {error}",
                extra=error_context,
            )
            # Don't re-raise - system continues

        elif self.error_strategy == ErrorHandlingStrategy.SELECTIVE:
            if self._is_critical_error(error):
                self._logger.error(
                    f"{self._log_prefix} Critical error processing {event_info}: {error}",
                    extra=error_context,
                )
                raise
            else:
                self._logger.warning(
                    f"{self._log_prefix} Non-critical error processing {event_info}: {error}",
                    extra=error_context,
                )

    def _get_event_info(self, event: Any) -> str:
        """
        Get a string representation of the event for logging.

        Args:
            event: The event to describe

        Returns:
            Human-readable event description
        """
        event_type = event.__class__.__name__

        # Try to get meaningful identifiers from common event attributes
        identifiers = []

        for attr in ["event_id", "portfolio_id", "symbol", "trade_id"]:
            if hasattr(event, attr):
                value = getattr(event, attr)
                identifiers.append(f"{attr}={str(value)}")

        if identifiers:
            return f"{event_type}({', '.join(identifiers)})"
        else:
            return event_type

    def _build_error_context(self, event: Any, error: Exception) -> dict:
        """
        Build error context for structured logging.

        Args:
            event: The event that caused the error
            error: The exception that occurred

        Returns:
            Dictionary with error context information
        """
        return {
            "event_type": event.__class__.__name__,
            "event_id": getattr(event, "event_id", None),
            "error_type": error.__class__.__name__,
            "handler_class": self.__class__.__name__,
        }

    def _is_critical_error(self, error: Exception) -> bool:
        """
        Determine if an error is critical and should break processing.

        Override this method in concrete handlers to define
        handler-specific critical error conditions.

        Args:
            error: The exception to evaluate

        Returns:
            True if the error is critical, False otherwise
        """
        # Default: consider validation and domain errors as critical
        critical_types = (
            "DomainValidationError",
            "ConfigurationError",
            "RepositoryError",
        )
        return error.__class__.__name__ in critical_types


class CriticalEventHandler(BaseEventHandler):
    """Event handler for critical operations that must not fail."""

    def __init__(self, logger_name: str | None = None):
        super().__init__(
            error_strategy=ErrorHandlingStrategy.CRITICAL, logger_name=logger_name
        )


class ResilientEventHandler(BaseEventHandler):
    """Event handler for operations that should continue despite errors."""

    def __init__(self, logger_name: str | None = None):
        super().__init__(
            error_strategy=ErrorHandlingStrategy.RESILIENT, logger_name=logger_name
        )


class SelectiveEventHandler(BaseEventHandler):
    """Event handler with custom critical error determination."""

    def __init__(self, logger_name: str | None = None):
        super().__init__(
            error_strategy=ErrorHandlingStrategy.SELECTIVE, logger_name=logger_name
        )

    @abstractmethod
    def _is_critical_error(self, error: Exception) -> bool:
        """
        Concrete handlers must define what constitutes a critical error.

        Args:
            error: The exception to evaluate

        Returns:
            True if the error should break processing, False otherwise
        """
        pass
