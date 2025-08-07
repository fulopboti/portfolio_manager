"""Standardized exception handling for application services."""

import logging
from typing import Any, Dict, Optional, Type, List, Callable, Union
from enum import Enum
from functools import wraps

from stockapp.domain.exceptions import (
    DomainError,
    DataIngestionError, 
    StrategyCalculationError,
    InvalidTradeError,
    InsufficientFundsError,
    DomainValidationError
)
from stockapp.infrastructure.data_access.exceptions import (
    DataAccessError,
    NotFoundError
)


logger = logging.getLogger(__name__)


class ExceptionSeverity(Enum):
    """Exception severity levels for different handling strategies."""
    
    CRITICAL = "critical"      # System-level errors that require immediate attention
    HIGH = "high"             # Business-critical errors that block operations
    MEDIUM = "medium"         # Recoverable errors that may impact functionality
    LOW = "low"              # Minor errors or expected failures
    INFO = "info"            # Informational exceptions (e.g., validation failures)


class ExceptionCategory(Enum):
    """Categories of exceptions for consistent handling."""
    
    DOMAIN = "domain"                # Business logic errors
    DATA_ACCESS = "data_access"      # Database/repository errors
    VALIDATION = "validation"        # Input validation errors
    EXTERNAL = "external"           # Third-party service errors
    SYSTEM = "system"               # System-level errors
    CONFIGURATION = "configuration" # Configuration-related errors


class ServiceExceptionRegistry:
    """Registry for mapping exceptions to handling strategies."""
    
    def __init__(self):
        """Initialize the exception registry with default mappings."""
        self._mappings: Dict[Type[Exception], Dict[str, Any]] = {}
        self._setup_default_mappings()
    
    def _setup_default_mappings(self) -> None:
        """Setup default exception mappings."""
        # Domain exceptions
        self.register(
            DomainValidationError,
            category=ExceptionCategory.VALIDATION,
            severity=ExceptionSeverity.MEDIUM,
            retry_strategy="no_retry",
            log_stack_trace=False
        )
        
        self.register(
            InvalidTradeError,
            category=ExceptionCategory.DOMAIN,
            severity=ExceptionSeverity.HIGH,
            retry_strategy="no_retry",
            log_stack_trace=True
        )
        
        self.register(
            InsufficientFundsError,
            category=ExceptionCategory.DOMAIN,
            severity=ExceptionSeverity.MEDIUM,
            retry_strategy="no_retry",
            log_stack_trace=False
        )
        
        self.register(
            DataIngestionError,
            category=ExceptionCategory.EXTERNAL,
            severity=ExceptionSeverity.MEDIUM,
            retry_strategy="exponential_backoff",
            log_stack_trace=True
        )
        
        self.register(
            StrategyCalculationError,
            category=ExceptionCategory.DOMAIN,
            severity=ExceptionSeverity.MEDIUM,
            retry_strategy="no_retry",
            log_stack_trace=True
        )
        
        # Data access exceptions
        self.register(
            DataAccessError,
            category=ExceptionCategory.DATA_ACCESS,
            severity=ExceptionSeverity.HIGH,
            retry_strategy="exponential_backoff",
            log_stack_trace=True
        )
        
        self.register(
            NotFoundError,
            category=ExceptionCategory.DATA_ACCESS,
            severity=ExceptionSeverity.LOW,
            retry_strategy="no_retry",
            log_stack_trace=False
        )
        
        # System exceptions
        self.register(
            ConnectionError,
            category=ExceptionCategory.SYSTEM,
            severity=ExceptionSeverity.CRITICAL,
            retry_strategy="exponential_backoff",
            log_stack_trace=True
        )
        
        self.register(
            TimeoutError,
            category=ExceptionCategory.EXTERNAL,
            severity=ExceptionSeverity.MEDIUM,
            retry_strategy="exponential_backoff",
            log_stack_trace=False
        )
        
        # Generic exception (catch-all)
        self.register(
            Exception,
            category=ExceptionCategory.SYSTEM,
            severity=ExceptionSeverity.CRITICAL,
            retry_strategy="no_retry",
            log_stack_trace=True
        )
    
    def register(
        self,
        exception_type: Type[Exception],
        category: ExceptionCategory,
        severity: ExceptionSeverity,
        retry_strategy: str = "no_retry",
        log_stack_trace: bool = True,
        custom_handler: Optional[Callable] = None
    ) -> None:
        """
        Register an exception type with handling strategy.
        
        Args:
            exception_type: The exception class to register
            category: Exception category
            severity: Exception severity level
            retry_strategy: Retry strategy ("no_retry", "simple_retry", "exponential_backoff")
            log_stack_trace: Whether to log stack traces
            custom_handler: Optional custom handler function
        """
        self._mappings[exception_type] = {
            "category": category,
            "severity": severity,
            "retry_strategy": retry_strategy,
            "log_stack_trace": log_stack_trace,
            "custom_handler": custom_handler
        }
    
    def get_handling_strategy(self, exception: Exception) -> Dict[str, Any]:
        """
        Get handling strategy for an exception.
        
        Args:
            exception: The exception instance
            
        Returns:
            Dictionary with handling strategy information
        """
        # Look for exact type match first
        exception_type = type(exception)
        if exception_type in self._mappings:
            return self._mappings[exception_type]
        
        # Look for parent class matches
        for registered_type, strategy in self._mappings.items():
            if isinstance(exception, registered_type):
                return strategy
        
        # Fall back to generic Exception handling
        return self._mappings.get(Exception, {
            "category": ExceptionCategory.SYSTEM,
            "severity": ExceptionSeverity.CRITICAL,
            "retry_strategy": "no_retry",
            "log_stack_trace": True,
            "custom_handler": None
        })


class StandardExceptionHandler:
    """Standardized exception handler for application services."""
    
    def __init__(self, service_name: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the exception handler.
        
        Args:
            service_name: Name of the service using this handler
            logger: Optional logger instance
        """
        self.service_name = service_name
        self.logger = logger or logging.getLogger(f"{__name__}.{service_name}")
        self.registry = ServiceExceptionRegistry()
        self._operation_context: Dict[str, Any] = {}
    
    def set_operation_context(self, **context) -> None:
        """Set context information for current operation."""
        self._operation_context.update(context)
    
    def clear_operation_context(self) -> None:
        """Clear the current operation context."""
        self._operation_context.clear()
    
    def handle_exception(
        self,
        exception: Exception,
        operation_name: str,
        entity_context: Optional[str] = None,
        reraise: bool = True,
        return_default: Any = None
    ) -> Any:
        """
        Handle an exception according to registered strategy.
        
        Args:
            exception: The exception to handle
            operation_name: Name of the operation that failed
            entity_context: Optional context about the entity being processed
            reraise: Whether to re-raise the exception after handling
            return_default: Default value to return if not re-raising
            
        Returns:
            Default value if not re-raising, otherwise raises the exception
        """
        strategy = self.registry.get_handling_strategy(exception)
        
        # Build comprehensive error context
        error_context = {
            "service": self.service_name,
            "operation": operation_name,
            "entity_context": entity_context,
            "exception_type": exception.__class__.__name__,
            "exception_message": str(exception),
            "category": strategy["category"].value,
            "severity": strategy["severity"].value,
            **self._operation_context
        }
        
        # Log the exception according to severity and strategy
        self._log_exception(exception, strategy, error_context)
        
        # Execute custom handler if provided
        if strategy.get("custom_handler"):
            try:
                strategy["custom_handler"](exception, error_context)
            except Exception as handler_error:
                self.logger.error(f"Custom exception handler failed: {handler_error}")
        
        # Decide whether to re-raise or return default
        if reraise:
            # Optionally transform the exception
            transformed = self._transform_exception(exception, strategy, error_context)
            raise transformed
        else:
            return return_default
    
    def _log_exception(
        self,
        exception: Exception,
        strategy: Dict[str, Any],
        error_context: Dict[str, Any]
    ) -> None:
        """Log exception according to strategy."""
        severity = strategy["severity"]
        log_stack = strategy["log_stack_trace"]
        
        # Create base log message
        base_msg = f"[{self.service_name}] {error_context['operation']} failed: {exception}"
        if error_context.get("entity_context"):
            base_msg += f" (Entity: {error_context['entity_context']})"
        
        # Log according to severity
        if severity == ExceptionSeverity.CRITICAL:
            if log_stack:
                self.logger.critical(base_msg, exc_info=True, extra=error_context)
            else:
                self.logger.critical(base_msg, extra=error_context)
        elif severity == ExceptionSeverity.HIGH:
            if log_stack:
                self.logger.error(base_msg, exc_info=True, extra=error_context)
            else:
                self.logger.error(base_msg, extra=error_context)
        elif severity == ExceptionSeverity.MEDIUM:
            if log_stack:
                self.logger.warning(base_msg, exc_info=log_stack, extra=error_context)
            else:
                self.logger.warning(base_msg, extra=error_context)
        elif severity == ExceptionSeverity.LOW:
            self.logger.info(base_msg, extra=error_context)
        else:  # INFO
            self.logger.debug(base_msg, extra=error_context)
    
    def _transform_exception(
        self,
        exception: Exception,
        strategy: Dict[str, Any],
        error_context: Dict[str, Any]
    ) -> Exception:
        """Transform exception if needed before re-raising."""
        # For now, return the original exception
        # Could be extended to wrap exceptions or add context
        return exception


def service_exception_handler(
    operation_name: str,
    entity_context: Optional[str] = None,
    reraise: bool = True,
    return_default: Any = None,
    expected_exceptions: Optional[List[Type[Exception]]] = None
):
    """
    Decorator for standardized exception handling in service methods.
    
    Args:
        operation_name: Name of the operation
        entity_context: Optional entity context
        reraise: Whether to re-raise exceptions
        return_default: Default return value if not re-raising
        expected_exceptions: List of expected exception types
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            if not hasattr(self, '_exception_handler'):
                handler_name = getattr(self, '__class__', {}).get('__name__', 'UnknownService')
                self._exception_handler = StandardExceptionHandler(handler_name)
            
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                # Check if this is an expected exception
                if expected_exceptions and any(isinstance(e, exc_type) for exc_type in expected_exceptions):
                    # Handle expected exceptions with lower severity
                    return self._exception_handler.handle_exception(
                        e, operation_name, entity_context, reraise, return_default
                    )
                else:
                    # Handle unexpected exceptions
                    return self._exception_handler.handle_exception(
                        e, operation_name, entity_context, reraise, return_default
                    )
        
        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            if not hasattr(self, '_exception_handler'):
                handler_name = getattr(self, '__class__', {}).get('__name__', 'UnknownService')
                self._exception_handler = StandardExceptionHandler(handler_name)
            
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                return self._exception_handler.handle_exception(
                    e, operation_name, entity_context, reraise, return_default
                )
        
        # Return appropriate wrapper based on whether the function is async
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Singleton instance for global use
default_exception_registry = ServiceExceptionRegistry()