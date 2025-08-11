"""Domain models and services for currency-aware symbol mapping."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Set


class CurrencyCode(Enum):
    """Supported currency codes."""
    
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CAD = "CAD"
    AUD = "AUD"
    CHF = "CHF"
    CNY = "CNY"
    HKD = "HKD"
    SGD = "SGD"
    SEK = "SEK"
    NOK = "NOK"
    DKK = "DKK"
    PLN = "PLN"
    CZK = "CZK"
    HUF = "HUF"
    RON = "RON"
    BGN = "BGN"
    HRK = "HRK"
    RSD = "RSD"


@dataclass
class ExchangeInfo:
    """Information about a stock exchange and symbol listing."""
    
    symbol: str
    exchange: str
    country: str
    currency: CurrencyCode
    trading_hours: str = ""
    lot_size: int = 1
    tick_size: Decimal = field(default_factory=lambda: Decimal('0.01'))


@dataclass 
class ProviderInfo:
    """Information about a data provider's symbol mapping."""
    
    symbol: str
    provider: str
    supports_fundamentals: bool = True
    supports_realtime: bool = False


@dataclass
class SymbolMapping:
    """Complete symbol mapping with currency-aware exchange and provider information."""
    
    isin: str
    base_symbol: str
    base_exchange: str
    base_country: str
    base_currency: CurrencyCode
    company_name: str
    sector: Optional[str] = None
    market_cap_usd: Optional[Decimal] = None
    exchanges: Dict[str, ExchangeInfo] = field(default_factory=dict)
    providers: Dict[str, ProviderInfo] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize collections if not provided."""
        if not isinstance(self.exchanges, dict):
            self.exchanges = {}
        if not isinstance(self.providers, dict):
            self.providers = {}
    
    def get_currency_for_exchange(self, exchange: str) -> Optional[CurrencyCode]:
        """Get the currency used on a specific exchange."""
        if exchange == self.base_exchange:
            return self.base_currency
        
        exchange_info = self.exchanges.get(exchange)
        if exchange_info:
            return exchange_info.currency
        
        return None
    
    def get_all_currencies(self) -> Set[CurrencyCode]:
        """Get all currencies this symbol is traded in."""
        currencies = {self.base_currency}
        
        for exchange_info in self.exchanges.values():
            currencies.add(exchange_info.currency)
        
        return currencies


class SymbolMappingService(ABC):
    """Abstract service for symbol mapping operations."""
    
    @abstractmethod
    async def get_equivalent_symbols(self, symbol: str) -> List[SymbolMapping]:
        """Get equivalent symbols across different exchanges and currencies."""
        pass
    
    @abstractmethod
    async def get_provider_symbol(self, symbol: str, provider: str) -> Optional[str]:
        """Get the symbol used by a specific data provider."""
        pass
    
    @abstractmethod
    async def search_by_company(self, company_name: str) -> List[SymbolMapping]:
        """Search for symbols by company name."""
        pass