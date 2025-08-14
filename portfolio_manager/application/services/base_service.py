"""
Base service class providing common application service functionality.

This module provides abstract base classes and utilities that eliminate
code duplication across application service implementations.
"""

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar

from portfolio_manager.domain.exceptions import DomainError, DomainValidationError

T = TypeVar('T')


class ServiceErrorStrategy(Enum):
    """Strategy for handling errors in application services."""

    RAISE_EXCEPTIONS = "raise_exceptions"     # Re-raise all errors as exceptions
    RETURN_RESULTS = "return_results"        # Return result objects with success/error
    MIXED = "mixed"                          # Use both strategies based on operation type


@dataclass(frozen=True)
class ServiceResult(Generic[T]):
    """
    Generic result wrapper for service operations.

    Provides a consistent way to return success/failure information
    along with data or error details.
    """

    success: bool
    data: T | None = None
    error: Exception | None = None
    error_code: str | None = None
    metadata: dict[str, Any] | None = None

    @classmethod
    def success_result(cls, data: T, metadata: dict[str, Any] | None = None) -> 'ServiceResult[T]':
        """Create a successful result."""
        return cls(success=True, data=data, metadata=metadata)

    @classmethod
    def error_result(
        cls,
        error: Exception,
        error_code: str | None = None,
        metadata: dict[str, Any] | None = None
    ) -> 'ServiceResult[T]':
        """Create an error result."""
        return cls(
            success=False,
            error=error,
            error_code=error_code or error.__class__.__name__,
            metadata=metadata
        )

    def unwrap(self) -> T:
        """
        Unwrap the result data or raise the error.

        Returns:
            The result data if successful

        Raises:
            The contained exception if the result was an error
        """
        if self.success and self.data is not None:
            return self.data
        elif self.error:
            raise self.error
        else:
            raise ValueError("Result has no data or error")


class BaseApplicationService:
    """
    Base class for all application services.

    Provides common functionality for:
    - Structured logging with context
    - Standardized error handling strategies
    - Common validation patterns
    - Dependency management utilities
    - Performance monitoring hooks
    """

    def __init__(
        self,
        error_strategy: ServiceErrorStrategy = ServiceErrorStrategy.RAISE_EXCEPTIONS,
        logger_name: str | None = None
    ):
        """
        Initialize base application service.

        Args:
            error_strategy: How to handle errors during service operations
            logger_name: Custom logger name (defaults to class name)
        """
        self.error_strategy = error_strategy
        self._logger = logging.getLogger(logger_name or self.__class__.__name__)
        self._setup_logging_context()

    def _setup_logging_context(self) -> None:
        """Setup logging context for this service."""
        self._log_prefix = f"[{self.__class__.__name__}]"

    def _log_operation_start(self, operation: str, context: str = "") -> None:
        """Log the start of a service operation."""
        context_info = f" ({context})" if context else ""
        self._logger.debug(f"{self._log_prefix} Starting {operation}{context_info}")

    def _log_operation_success(self, operation: str, context: str = "", metrics: dict | None = None) -> None:
        """Log the successful completion of a service operation."""
        context_info = f" ({context})" if context else ""
        metrics_info = f" - {metrics}" if metrics else ""
        self._logger.debug(f"{self._log_prefix} Successfully completed {operation}{context_info}{metrics_info}")

    def _log_operation_error(self, operation: str, error: Exception, context: str = "") -> None:
        """Log an error during a service operation."""
        context_info = f" ({context})" if context else ""
        error_context = self._build_error_context(operation, error, context)
        self._logger.error(
            f"{self._log_prefix} Failed {operation}{context_info}: {error}",
            extra=error_context
        )

    def _build_error_context(self, operation: str, error: Exception, context: str) -> dict[str, Any]:
        """
        Build error context for structured logging.

        Args:
            operation: The operation that failed
            error: The exception that occurred
            context: Additional context information

        Returns:
            Dictionary with error context information
        """
        return {
            'operation': operation,
            'context': context,
            'error_type': error.__class__.__name__,
            'service_class': self.__class__.__name__,
        }

    async def _execute_operation(
        self,
        operation_name: str,
        operation_func: Any,  # Callable coroutine
        context: str = "",
        expected_exceptions: list[type[Exception]] | None = None
    ) -> Any:
        """
        Execute a service operation with standardized logging and error handling.

        Args:
            operation_name: Name of the operation for logging
            operation_func: Async function to execute
            context: Additional context for logging
            expected_exceptions: List of exceptions that are expected/handled

        Returns:
            Result of the operation

        Raises:
            Exception: Based on the configured error strategy
        """
        self._log_operation_start(operation_name, context)

        try:
            result = await operation_func()
            self._log_operation_success(operation_name, context)
            return result

        except Exception as e:
            self._log_operation_error(operation_name, e, context)

            # Handle expected exceptions differently
            if expected_exceptions and any(isinstance(e, exc_type) for exc_type in expected_exceptions):
                # Expected exceptions are logged but may not be re-raised based on strategy
                pass

            if self.error_strategy == ServiceErrorStrategy.RAISE_EXCEPTIONS:
                raise
            elif self.error_strategy == ServiceErrorStrategy.RETURN_RESULTS:
                # This should be handled by the caller wrapping in ServiceResult
                raise  # Let caller handle the ServiceResult wrapping
            else:  # MIXED strategy - decide based on exception type
                if isinstance(e, DomainValidationError | DomainError):
                    raise  # Always raise domain/validation errors
                # Other errors might be wrapped in results by caller
                raise

    def _validate_required_params(self, params: dict[str, Any]) -> None:
        """
        Validate that required parameters are present and not None.

        Args:
            params: Dictionary of parameter names to values

        Raises:
            ValidationError: If any required parameter is missing or None
        """
        missing_params = [name for name, value in params.items() if value is None]
        if missing_params:
            raise DomainValidationError(f"Required parameters missing: {', '.join(missing_params)}")

    def _validate_business_rules(self, rules: list[tuple[bool, str]]) -> None:
        """
        Validate business rules and raise ValidationError on first failure.

        Args:
            rules: List of (condition, error_message) tuples

        Raises:
            ValidationError: If any rule condition is False
        """
        for condition, error_message in rules:
            if not condition:
                raise DomainValidationError(error_message)

    @asynccontextmanager
    async def _performance_tracking(self, operation_name: str):
        """
        Context manager for tracking operation performance.

        Args:
            operation_name: Name of the operation being tracked

        Usage:
            async with self._performance_tracking("calculate_scores"):
                # operation code here
        """
        import time
        start_time = time.time()

        try:
            yield
        finally:
            duration = time.time() - start_time
            self._logger.debug(f"{self._log_prefix} {operation_name} completed in {duration:.3f}s")


class ResultBasedService(BaseApplicationService):
    """
    Base service class for services that return ServiceResult objects.

    This class is configured to use the RETURN_RESULTS error strategy
    and provides helper methods for creating consistent result objects.
    """

    def __init__(self, logger_name: str | None = None):
        """Initialize result-based service."""
        super().__init__(
            error_strategy=ServiceErrorStrategy.RETURN_RESULTS,
            logger_name=logger_name
        )

    async def _execute_with_result(
        self,
        operation_name: str,
        operation_func: Any,
        context: str = "",
        expected_exceptions: list[type[Exception]] | None = None
    ) -> ServiceResult[Any]:
        """
        Execute an operation and wrap the result in a ServiceResult.

        Args:
            operation_name: Name of the operation
            operation_func: Async function to execute
            context: Additional context
            expected_exceptions: Expected exception types

        Returns:
            ServiceResult with either success data or error information
        """
        try:
            result = await self._execute_operation(
                operation_name, operation_func, context, expected_exceptions
            )
            return ServiceResult.success_result(result)

        except Exception as e:
            return ServiceResult.error_result(e, context=context)


class ExceptionBasedService(BaseApplicationService):
    """
    Base service class for services that raise exceptions directly.

    This class is configured to use the RAISE_EXCEPTIONS error strategy
    and provides standard exception handling patterns.
    """

    def __init__(self, logger_name: str | None = None):
        """Initialize exception-based service."""
        super().__init__(
            error_strategy=ServiceErrorStrategy.RAISE_EXCEPTIONS,
            logger_name=logger_name
        )


class ServiceMetrics:
    """
    Utility class for collecting service operation metrics.

    Provides a simple way to track operation counts, durations,
    and success rates across service methods.
    """

    def __init__(self):
        """Initialize metrics collector."""
        self._metrics: dict[str, dict[str, Any]] = {}

    def record_operation(
        self,
        operation: str,
        duration: float,
        success: bool,
        context: str | None = None
    ) -> None:
        """
        Record metrics for a service operation.

        Args:
            operation: Name of the operation
            duration: Duration in seconds
            success: Whether the operation succeeded
            context: Optional context information
        """
        key = f"{operation}:{context}" if context else operation

        if key not in self._metrics:
            self._metrics[key] = {
                'count': 0,
                'success_count': 0,
                'total_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0
            }

        metrics = self._metrics[key]
        metrics['count'] += 1
        if success:
            metrics['success_count'] += 1

        metrics['total_duration'] += duration
        metrics['min_duration'] = min(metrics['min_duration'], duration)
        metrics['max_duration'] = max(metrics['max_duration'], duration)

    def get_operation_metrics(self, operation: str) -> dict[str, Any] | None:
        """Get metrics for a specific operation."""
        metrics = self._metrics.get(operation)
        if not metrics:
            return None

        avg_duration = metrics['total_duration'] / metrics['count']
        success_rate = metrics['success_count'] / metrics['count']

        return {
            **metrics,
            'avg_duration': avg_duration,
            'success_rate': success_rate
        }

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """Get all collected metrics."""
        return {
            operation: self.get_operation_metrics(operation)
            for operation in self._metrics.keys()
        }


class DependencyContainer:
    """
    Simple dependency injection container for service initialization.

    Provides a lightweight way to manage service dependencies
    and ensure consistent initialization patterns.
    """

    def __init__(self):
        """Initialize empty dependency container."""
        self._dependencies: dict[str, Any] = {}
        self._factories: dict[str, Any] = {}

    def register(self, name: str, instance: Any) -> None:
        """Register a dependency instance."""
        self._dependencies[name] = instance

    def register_factory(self, name: str, factory_func: Any) -> None:
        """Register a factory function for lazy initialization."""
        self._factories[name] = factory_func

    def get(self, name: str) -> Any:
        """Get a dependency by name."""
        if name in self._dependencies:
            return self._dependencies[name]

        if name in self._factories:
            instance = self._factories[name]()
            self._dependencies[name] = instance  # Cache the instance
            return instance

        raise KeyError(f"Dependency '{name}' not found")

    def has(self, name: str) -> bool:
        """Check if a dependency is registered."""
        return name in self._dependencies or name in self._factories
