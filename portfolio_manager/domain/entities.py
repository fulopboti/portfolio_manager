"""Domain entities for the Stock Analysis Platform."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List
from uuid import UUID

from portfolio_manager.domain.exceptions import (
    DomainValidationError,
    InsufficientFundsError,
    InvalidPositionError,
    InvalidTradeError,
)


class AssetType(Enum):
    """Enumeration of supported asset types."""
    STOCK = "STOCK"
    ETF = "ETF"
    CRYPTO = "CRYPTO"
    COMMODITY = "COMMODITY"


class TradeSide(Enum):
    """Enumeration of trade sides."""
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class Asset:
    """Represents a financial asset that can be traded."""
    
    symbol: str
    exchange: str
    asset_type: AssetType
    name: str

    def __post_init__(self):
        """Validate asset data after initialization."""
        if not self.symbol or not self.symbol.strip():
            raise DomainValidationError("Symbol cannot be empty")
        
        if not self.exchange or not self.exchange.strip():
            raise DomainValidationError("Exchange cannot be empty")
        
        if not self.name or not self.name.strip():
            raise DomainValidationError("Name cannot be empty")
        
        # Normalize symbol and exchange to uppercase
        object.__setattr__(self, 'symbol', self.symbol.upper().strip())
        object.__setattr__(self, 'exchange', self.exchange.upper().strip())

    def __eq__(self, other) -> bool:
        """Assets are equal if they have the same symbol."""
        if not isinstance(other, Asset):
            return False
        return self.symbol == other.symbol

    def __hash__(self) -> int:
        """Hash based on symbol for use in sets and dictionaries."""
        return hash(self.symbol)

    def __repr__(self) -> str:
        """String representation of the asset."""
        return f"Asset(symbol='{self.symbol}', exchange='{self.exchange}', type={self.asset_type.value})"


@dataclass(frozen=True)
class AssetSnapshot:
    """Represents a point-in-time snapshot of asset price data (OHLCV)."""
    
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def __post_init__(self):
        """Validate snapshot data after initialization."""
        # Validate prices are positive
        if any(price <= 0 for price in [self.open, self.high, self.low, self.close]):
            raise DomainValidationError("Prices must be positive")
        
        # Validate OHLC relationships - high must be highest, low must be lowest
        if self.high < self.low:
            raise DomainValidationError("High price must be >= low price")
        
        # Check high is >= all other prices
        if self.high < self.open:
            raise DomainValidationError("High price must be >= low price")
        
        if self.high < self.close:
            raise DomainValidationError("High price must be >= low price")
        
        # Check low is <= all other prices
        if self.low > self.open:
            raise DomainValidationError("High price must be >= low price")
        
        if self.low > self.close:
            raise DomainValidationError("High price must be >= low price")
        
        # Validate volume is non-negative
        if self.volume < 0:
            raise DomainValidationError("Volume must be non-negative")

    def daily_return(self) -> Decimal:
        """Calculate the daily return percentage."""
        if self.open == 0:
            return Decimal('0')
        return (self.close - self.open) / self.open

    def is_green_day(self) -> bool:
        """Check if this was a positive day (close > open)."""
        return self.close > self.open

    def price_range(self) -> Decimal:
        """Calculate the price range (high - low)."""
        return self.high - self.low


@dataclass
class Portfolio:
    """Represents an investment portfolio."""
    
    portfolio_id: UUID
    name: str
    base_ccy: str
    cash_balance: Decimal
    created: datetime

    def __post_init__(self):
        """Validate portfolio data after initialization."""
        if not self.name or not self.name.strip():
            raise DomainValidationError("Portfolio name cannot be empty")
        
        # Validate currency code (simple validation for major currencies)
        valid_currencies = {"USD", "EUR", "RON", "GBP", "JPY", "CAD", "AUD", "CHF"}
        if self.base_ccy not in valid_currencies:
            raise DomainValidationError(f"Invalid currency code: {self.base_ccy}")
        
        if self.cash_balance < 0:
            raise DomainValidationError("Cash balance cannot be negative")

    def add_cash(self, amount: Decimal) -> None:
        """Add cash to the portfolio."""
        if amount <= 0:
            raise DomainValidationError("Cash amount must be positive")
        
        self.cash_balance += amount

    def deduct_cash(self, amount: Decimal) -> None:
        """Deduct cash from the portfolio."""
        if amount <= 0:
            raise DomainValidationError("Cash amount must be positive")
        
        if self.cash_balance < amount:
            raise InsufficientFundsError(
                f"Insufficient funds: available {self.cash_balance}, required {amount}"
            )
        
        self.cash_balance -= amount

    def has_sufficient_cash(self, amount: Decimal) -> bool:
        """Check if portfolio has sufficient cash for a transaction."""
        return self.cash_balance >= amount


@dataclass(frozen=True)
class Trade:
    """Represents a trade execution record."""
    
    trade_id: UUID
    portfolio_id: UUID
    symbol: str
    timestamp: datetime
    side: TradeSide
    qty: Decimal
    price: Decimal
    pip_pct: Decimal
    fee_flat: Decimal
    fee_pct: Decimal
    unit: str
    price_ccy: str
    comment: str

    def __post_init__(self):
        """Validate trade data after initialization."""
        if self.qty <= 0:
            raise InvalidTradeError("Trade quantity must be positive")
        
        if self.price <= 0:
            raise InvalidTradeError("Trade price must be positive")
        
        if self.pip_pct < 0 or self.fee_pct < 0:
            raise InvalidTradeError("Fee percentages must be non-negative")
        
        if self.fee_flat < 0:
            raise InvalidTradeError("Flat fees must be non-negative")

    def gross_amount(self) -> Decimal:
        """Calculate the gross trade amount (quantity * price)."""
        return self.qty * self.price

    def pip_cost(self) -> Decimal:
        """Calculate the pip/spread cost."""
        return self.gross_amount() * self.pip_pct

    def commission_cost(self) -> Decimal:
        """Calculate the commission cost."""
        return self.gross_amount() * self.fee_pct

    def total_fees(self) -> Decimal:
        """Calculate total fees (pip + flat fee + commission)."""
        return self.pip_cost() + self.fee_flat + self.commission_cost()

    def net_amount(self) -> Decimal:
        """Calculate the net trade amount including fees."""
        gross = self.gross_amount()
        fees = self.total_fees()
        
        if self.side == TradeSide.BUY:
            # For buy trades, we pay gross + fees
            return gross + fees
        else:
            # For sell trades, we receive gross - fees
            return gross - fees

    def is_buy(self) -> bool:
        """Check if this is a buy trade."""
        return self.side == TradeSide.BUY

    def is_sell(self) -> bool:
        """Check if this is a sell trade."""
        return self.side == TradeSide.SELL


@dataclass
class Position:
    """Represents a position in a portfolio."""
    
    portfolio_id: UUID
    symbol: str
    qty: Decimal
    avg_cost: Decimal
    unit: str
    price_ccy: str
    last_updated: datetime

    def __post_init__(self):
        """Validate position data after initialization."""
        if self.qty <= 0:
            raise InvalidPositionError("Position quantity must be positive")
        
        if self.avg_cost <= 0:
            raise InvalidPositionError("Average cost must be positive")

    def market_value(self, current_price: Decimal) -> Decimal:
        """Calculate current market value of the position."""
        return self.qty * current_price

    def cost_basis(self) -> Decimal:
        """Calculate the total cost basis of the position."""
        return self.qty * self.avg_cost

    def unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized profit/loss."""
        return self.market_value(current_price) - self.cost_basis()

    def unrealized_pnl_pct(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized profit/loss percentage."""
        if self.avg_cost == 0:
            return Decimal('0')
        return (current_price - self.avg_cost) / self.avg_cost

    def add_shares(self, additional_qty: Decimal, price: Decimal) -> None:
        """Add shares to the position and update average cost."""
        if additional_qty <= 0:
            raise InvalidPositionError("Additional quantity must be positive")
        
        if price <= 0:
            raise InvalidPositionError("Price must be positive")
        
        # Calculate new weighted average cost
        total_cost = (self.qty * self.avg_cost) + (additional_qty * price)
        new_qty = self.qty + additional_qty
        
        self.qty = new_qty
        self.avg_cost = total_cost / new_qty

    def reduce_shares(self, reduction_qty: Decimal) -> None:
        """Reduce shares in the position."""
        if reduction_qty <= 0:
            raise InvalidPositionError("Reduction quantity must be positive")
        
        if reduction_qty > self.qty:
            raise InvalidPositionError(
                f"Cannot reduce position by {reduction_qty}, only {self.qty} available"
            )
        
        self.qty -= reduction_qty
        # Average cost remains the same when reducing position


@dataclass(frozen=True)
class BrokerProfile:
    """Represents a broker's fee structure and capabilities."""
    
    broker_id: str
    name: str
    pip_pct: Decimal
    fee_flat: Decimal
    fee_pct: Decimal
    min_order_value: Decimal
    supported_currencies: List[str]
    supports_fractional: bool

    def __post_init__(self):
        """Validate broker profile data after initialization."""
        if not self.broker_id or not self.broker_id.strip():
            raise DomainValidationError("Broker ID cannot be empty")
        
        if not self.name or not self.name.strip():
            raise DomainValidationError("Broker name cannot be empty")
        
        if self.pip_pct < 0 or self.fee_pct < 0:
            raise DomainValidationError("Fee percentages must be non-negative")
        
        if self.fee_flat < 0:
            raise DomainValidationError("Flat fees must be non-negative")
        
        if self.min_order_value < 0:
            raise DomainValidationError("Minimum order value must be non-negative")
        
        if not self.supported_currencies:
            raise DomainValidationError("Supported currencies list cannot be empty")

    def calculate_total_cost(self, trade: Trade) -> Decimal:
        """Calculate total cost for a trade with this broker."""
        if trade.side == TradeSide.BUY:
            return trade.net_amount()
        else:
            # For sell, return the net proceeds (positive value)
            return abs(trade.net_amount())

    def supports_currency(self, currency: str) -> bool:
        """Check if broker supports the given currency."""
        return currency in self.supported_currencies

    def can_execute_order(self, quantity: Decimal, price: Decimal) -> bool:
        """Check if broker can execute an order with given parameters."""
        # Check fractional shares
        if not self.supports_fractional and quantity != quantity.to_integral_value():
            return False
        
        # Check minimum order value
        order_value = quantity * price
        if order_value < self.min_order_value:
            return False
        
        return True
