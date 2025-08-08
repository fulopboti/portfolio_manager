"""Audit logging and system tracking data access layer abstractions."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum
from uuid import UUID


class AuditEventType(Enum):
    """Types of audit events that can be logged."""
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout" 
    DATA_INGESTION = "data_ingestion"
    PORTFOLIO_CREATE = "portfolio_create"
    PORTFOLIO_UPDATE = "portfolio_update"
    PORTFOLIO_DELETE = "portfolio_delete"
    TRADE_EXECUTE = "trade_execute"
    TRADE_CANCEL = "trade_cancel"
    STRATEGY_CALCULATE = "strategy_calculate"
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    ERROR_OCCURRED = "error_occurred"
    DATA_EXPORT = "data_export"
    DATA_IMPORT = "data_import"
    CONFIGURATION_CHANGE = "configuration_change"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditDataAccess(ABC):
    """Abstract interface for audit logging and system tracking.

    Provides methods for recording and querying system events,
    user actions, and operational data for compliance and debugging.
    """

    # Event Logging
    @abstractmethod
    async def log_event(
        self,
        event_type: AuditEventType,
        severity: AuditSeverity,
        message: str,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Log an audit event.

        Args:
            event_type: Type of event being logged
            severity: Severity level of the event
            message: Human-readable description of the event
            entity_id: Related entity (portfolio ID, asset symbol, etc.)
            user_id: User who triggered the event (if applicable)
            session_id: Session identifier (if applicable)
            details: Additional structured data about the event

        Returns:
            UUID: Unique identifier for the logged event

        Raises:
            DataAccessError: If logging fails
        """
        pass

    @abstractmethod
    async def log_events_batch(
        self,
        events: List[Dict[str, Any]]
    ) -> List[UUID]:
        """Log multiple audit events in a single batch.

        Args:
            events: List of event dictionaries with required fields

        Returns:
            List of UUIDs for the logged events

        Raises:
            DataAccessError: If batch logging fails
        """
        pass

    # Event Retrieval
    @abstractmethod
    async def get_event(self, event_id: UUID) -> Optional[Dict[str, Any]]:
        """Retrieve a specific audit event by ID.

        Args:
            event_id: Unique event identifier

        Returns:
            Event dictionary if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_types: Optional[List[AuditEventType]] = None,
        severity_levels: Optional[List[AuditSeverity]] = None,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query audit events with various filters.

        Args:
            start_date: Filter events after this date
            end_date: Filter events before this date
            event_types: Filter by event types
            severity_levels: Filter by severity levels
            entity_id: Filter by related entity
            user_id: Filter by user
            limit: Maximum number of events to return
            offset: Number of events to skip

        Returns:
            List of event dictionaries ordered by timestamp (newest first)
        """
        pass

    @abstractmethod
    async def get_events_for_entity(
        self,
        entity_id: str,
        event_types: Optional[List[AuditEventType]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all audit events for a specific entity.

        Args:
            entity_id: Entity identifier (portfolio ID, asset symbol, etc.)
            event_types: Filter by event types
            limit: Maximum number of events to return

        Returns:
            List of event dictionaries ordered by timestamp (newest first)
        """
        pass

    @abstractmethod
    async def get_recent_events(
        self,
        hours: int = 24,
        severity_levels: Optional[List[AuditSeverity]] = None
    ) -> List[Dict[str, Any]]:
        """Get recent audit events within the specified time window.

        Args:
            hours: Number of hours back to look
            severity_levels: Filter by severity levels

        Returns:
            List of event dictionaries ordered by timestamp (newest first)
        """
        pass

    # Error and Issue Tracking
    @abstractmethod
    async def log_error(
        self,
        error_message: str,
        error_type: str,
        stack_trace: Optional[str] = None,
        entity_id: Optional[str] = None,
        user_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Log an error event with detailed information.

        Args:
            error_message: Error description
            error_type: Type/class of error
            stack_trace: Full stack trace (if available)
            entity_id: Related entity where error occurred
            user_id: User who encountered the error
            context: Additional context data

        Returns:
            UUID: Unique identifier for the error event
        """
        pass

    @abstractmethod
    async def get_error_summary(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get summary statistics for errors in a time period.

        Args:
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with error counts, types, trends, etc.
        """
        pass

    @abstractmethod
    async def get_error_patterns(
        self,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """Analyze error patterns to identify recurring issues.

        Args:
            days: Number of days to analyze

        Returns:
            List of dictionaries describing error patterns
        """
        pass

    # Performance Tracking
    @abstractmethod
    async def log_performance_metric(
        self,
        operation_name: str,
        duration_ms: float,
        success: bool,
        details: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Log a performance metric for an operation.

        Args:
            operation_name: Name of the operation measured
            duration_ms: Duration in milliseconds
            success: Whether the operation succeeded
            details: Additional performance details

        Returns:
            UUID: Unique identifier for the performance record
        """
        pass

    @abstractmethod
    async def get_performance_stats(
        self,
        operation_name: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get performance statistics for an operation.

        Args:
            operation_name: Name of operation to analyze
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dictionary with avg, min, max, percentiles, success rate
        """
        pass

    @abstractmethod
    async def get_slow_operations(
        self,
        threshold_ms: float,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get operations that exceeded performance thresholds.

        Args:
            threshold_ms: Duration threshold in milliseconds
            hours: Number of hours back to analyze

        Returns:
            List of slow operation records
        """
        pass

    # Session Tracking
    @abstractmethod
    async def start_session(
        self,
        user_id: Optional[str] = None,
        client_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start a new user/system session.

        Args:
            user_id: User identifier (if user session)
            client_info: Information about client (IP, user agent, etc.)

        Returns:
            Session identifier string
        """
        pass

    @abstractmethod
    async def end_session(self, session_id: str) -> None:
        """End an active session.

        Args:
            session_id: Session to end
        """
        pass

    @abstractmethod
    async def get_active_sessions(self) -> List[Dict[str, Any]]:
        """Get list of currently active sessions.

        Returns:
            List of active session dictionaries
        """
        pass

    @abstractmethod
    async def get_session_history(
        self,
        user_id: Optional[str] = None,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get session history for analysis.

        Args:
            user_id: Filter by specific user (None = all users)
            days: Number of days of history to retrieve

        Returns:
            List of session records
        """
        pass

    # Data Retention and Cleanup
    @abstractmethod
    async def cleanup_old_events(self, days_to_keep: int) -> int:
        """Remove audit events older than specified days.

        Args:
            days_to_keep: Number of days of events to retain

        Returns:
            Number of events deleted
        """
        pass

    @abstractmethod
    async def archive_events(
        self,
        before_date: datetime,
        archive_path: Optional[str] = None
    ) -> int:
        """Archive old events to external storage.

        Args:
            before_date: Archive events before this date
            archive_path: Optional path for archive storage

        Returns:
            Number of events archived
        """
        pass

    @abstractmethod
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get statistics about audit log storage usage.

        Returns:
            Dictionary with counts, sizes, oldest/newest dates
        """
        pass

    # Reporting and Analytics
    @abstractmethod
    async def generate_activity_report(
        self,
        start_date: datetime,
        end_date: datetime,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate activity report for a time period.

        Args:
            start_date: Start of report period
            end_date: End of report period
            user_id: Filter by specific user (None = all users)

        Returns:
            Dictionary containing activity summary and statistics
        """
        pass

    @abstractmethod
    async def get_usage_patterns(
        self,
        days: int = 30
    ) -> Dict[str, Any]:
        """Analyze usage patterns to identify trends.

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary with usage patterns, peak times, etc.
        """
        pass

    @abstractmethod
    async def get_security_events(
        self,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get security-related events for monitoring.

        Args:
            hours: Number of hours back to check

        Returns:
            List of security event dictionaries
        """
        pass
