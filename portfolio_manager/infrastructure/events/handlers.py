"""
Event handler base classes and interfaces.

This module provides the abstract base classes and interfaces that all
event handlers must implement to participate in the event-driven system.
"""

from abc import ABC, abstractmethod
from typing import Any


class EventHandler(ABC):
    """Abstract base class for all event handlers."""
    
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
    async def handle(self, event: Any) -> None:
        """
        Handle the given event.
        
        Args:
            event: The event to process
            
        Raises:
            Exception: If event processing fails
        """
        pass
