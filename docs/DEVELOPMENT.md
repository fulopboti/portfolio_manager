# Development Guidelines

This document provides comprehensive guidelines for developing and contributing to the Portfolio Manager platform.

## 🏗️ Architecture Principles

### Hexagonal Architecture (Ports and Adapters)
The codebase follows strict hexagonal architecture principles with clear separation of concerns:

```
┌─────────────────────────────────────────┐
│              Domain Layer               │
│    Business Logic, Entities, Rules     │
│     portfolio_manager/domain/           │
└─────────────┬───────────────────────────┘
              │ Domain Events & Rules
┌─────────────┴───────────────────────────┐
│           Application Layer             │
│   Use Cases, Ports, Services           │
│   portfolio_manager/application/        │
└─────────────┬───────────────────────────┘
              │ Repository & Service Ports
┌─────────────┴───────────────────────────┐
│         Infrastructure Layer            │
│  Database, APIs, External Systems      │
│  portfolio_manager/infrastructure/     │
└─────────────────────────────────────────┘
```

### Layer Responsibilities

#### Domain Layer (`portfolio_manager/domain/`)
- **Entities**: Core business objects (`Asset`, `Portfolio`, `Trade`, `Position`)
- **Value Objects**: Immutable objects (`Money`, `Price`, `Volume`)
- **Domain Events**: Business events (`TradeExecuted`, `PortfolioCreated`)
- **Business Rules**: Domain validation and constraints
- **Exceptions**: Domain-specific error types

#### Application Layer (`portfolio_manager/application/`)
- **Ports**: Abstract interfaces for external dependencies
- **Services**: Orchestration of business operations
- **Event Handlers**: Domain event processing
- **DTOs**: Data transfer objects for API boundaries

#### Infrastructure Layer (`portfolio_manager/infrastructure/`)
- **Repositories**: Data persistence implementations
- **External APIs**: Third-party service integrations
- **Database**: Schema and migration management
- **Events**: Event bus and messaging infrastructure

## 🔧 Development Standards

### Code Quality Requirements

#### Exception Handling
```python
# ✅ CORRECT - Domain-specific exceptions
try:
    result = await service.process_data()
except (DomainError, DataAccessError) as e:
    self._logger.warning(f"Expected error: {e}")
    return ErrorResult(error=e)
except Exception as e:
    self._logger.error(f"Unexpected error: {e}")
    return ErrorResult(error=DomainSpecificError(f"Process failed: {e}"))

# ❌ INCORRECT - Generic exception handling
try:
    result = await service.process_data()
except Exception as e:
    print(f"Error: {e}")  # No logging, no domain context
    return None
```

#### Financial Data Precision
```python
# ✅ CORRECT - String-based Decimal storage
def store_price(self, price: Decimal) -> None:
    parameters = [str(price)]  # Convert to string for database
    await self.query_executor.execute_command(sql, parameters)

# ✅ CORRECT - Trust automatic conversion from database
def get_price(self) -> Decimal:
    result = await self.query_executor.execute_query(sql)
    return result.rows[0]['price']  # Automatically converted to Decimal

# ❌ INCORRECT - Float conversion loses precision
def store_price(self, price: Decimal) -> None:
    parameters = [float(price)]  # NEVER do this for financial data
```

#### Logging Patterns
```python
# ✅ CORRECT - Structured logging with context
class DataIngestionService(ExceptionBasedService):
    async def ingest_symbol(self, symbol: str) -> IngestionResult:
        try:
            self._log_operation_start("ingest_symbol", f"symbol={symbol}")
            result = await self._process_symbol(symbol)
            self._log_operation_success("ingest_symbol", f"Processed {symbol}")
            return result
        except DataIngestionError as e:
            self._logger.warning(f"Ingestion failed for {symbol}: {e}")
            return IngestionResult(success=False, error=str(e))

# ❌ INCORRECT - Print statements and generic logging
def ingest_symbol(self, symbol):
    print(f"Processing {symbol}")  # Use proper logging
    try:
        # process
        print("Done")
    except:  # Too generic
        print("Failed")
```

### Testing Standards

#### Test Structure
```python
class TestDataIngestionService:
    """Test cases for DataIngestionService following AAA pattern."""
    
    @pytest.fixture
    def service(self, mock_data_provider, mock_asset_repository):
        return DataIngestionService(mock_data_provider, mock_asset_repository)
    
    @pytest.mark.asyncio
    async def test_ingest_symbol_success(self, service, mock_data_provider):
        """Test successful symbol ingestion with valid data."""
        # Arrange
        symbol = "AAPL"
        expected_snapshots = [create_test_snapshot()]
        mock_data_provider.get_ohlcv_data.return_value = expected_snapshots
        
        # Act  
        result = await service.ingest_symbol(symbol, AssetType.STOCK, "NASDAQ", "Apple Inc.")
        
        # Assert
        assert result.success is True
        assert result.snapshots_count == 1
        mock_data_provider.get_ohlcv_data.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ingest_symbol_provider_error(self, service, mock_data_provider):
        """Test handling of data provider errors during ingestion."""
        # Arrange
        symbol = "INVALID"
        mock_data_provider.get_ohlcv_data.side_effect = DataIngestionError("Provider error")
        
        # Act
        result = await service.ingest_symbol(symbol, AssetType.STOCK, "NASDAQ", "Invalid")
        
        # Assert
        assert result.success is False
        assert "Provider error" in result.error
```

#### Test Categories and Markers
```python
@pytest.mark.unit
def test_entity_validation():
    """Unit test for domain entity validation."""
    pass

@pytest.mark.integration
@pytest.mark.duckdb
async def test_repository_operations():
    """Integration test with real DuckDB database."""
    pass

@pytest.mark.slow
async def test_full_ingestion_workflow():
    """End-to-end test that may take longer to complete."""
    pass
```

### Code Style

#### Import Organization
```python
# Standard library imports
import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

# Third-party imports
import pytest
from fastapi import FastAPI

# Local application imports
from portfolio_manager.domain.entities import Asset, AssetType
from portfolio_manager.domain.exceptions import DomainError
from portfolio_manager.application.ports import AssetRepository
from portfolio_manager.infrastructure.duckdb.asset_repository import DuckDBAssetRepository
```

#### Type Hints and Documentation
```python
from typing import Optional, List, Dict, Any
from decimal import Decimal

class PortfolioService:
    """Service for portfolio management operations.
    
    This service orchestrates portfolio operations including creation,
    position management, and performance calculation.
    
    Attributes:
        portfolio_repository: Repository for portfolio data persistence
        asset_repository: Repository for asset data access
    """
    
    def __init__(
        self,
        portfolio_repository: PortfolioRepository,
        asset_repository: AssetRepository
    ) -> None:
        self.portfolio_repository = portfolio_repository
        self.asset_repository = asset_repository
    
    async def calculate_portfolio_value(
        self,
        portfolio_id: UUID,
        as_of_date: Optional[datetime] = None
    ) -> Decimal:
        """Calculate total portfolio value at specific point in time.
        
        Args:
            portfolio_id: Unique identifier for the portfolio
            as_of_date: Valuation date (defaults to current time)
            
        Returns:
            Total portfolio value in base currency
            
        Raises:
            PortfolioNotFoundError: If portfolio doesn't exist
            DataAccessError: If unable to retrieve required data
        """
        # Implementation
        pass
```

## 🗄️ Database Development

### Schema Guidelines
```sql
-- Use appropriate DECIMAL precision for financial data
CREATE TABLE asset_snapshots (
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(18,6) NOT NULL,      -- 6 decimal places for price precision
    high DECIMAL(18,6) NOT NULL,
    low DECIMAL(18,6) NOT NULL,
    close DECIMAL(18,6) NOT NULL,
    volume BIGINT NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

-- Use proper constraints and indexes
CREATE TABLE portfolios (
    portfolio_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    base_ccy VARCHAR(3) NOT NULL DEFAULT 'USD',
    cash_balance DECIMAL(18,2) NOT NULL CHECK (cash_balance >= 0),
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_portfolios_created ON portfolios(created);
```

### Migration Practices
```python
class Migration_001_CreateAssetTables(Migration):
    """Create initial asset-related tables with proper constraints."""
    
    migration_id = "001"
    description = "Create asset snapshots and metrics tables"
    
    def up(self) -> List[str]:
        return [
            """
            CREATE TABLE asset_snapshots (
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DECIMAL(18,6) NOT NULL CHECK (open > 0),
                high DECIMAL(18,6) NOT NULL CHECK (high > 0),
                low DECIMAL(18,6) NOT NULL CHECK (low > 0),
                close DECIMAL(18,6) NOT NULL CHECK (close > 0),
                volume BIGINT NOT NULL CHECK (volume >= 0),
                PRIMARY KEY (symbol, timestamp)
            );
            """,
            "CREATE INDEX idx_asset_snapshots_symbol ON asset_snapshots(symbol);",
            "CREATE INDEX idx_asset_snapshots_timestamp ON asset_snapshots(timestamp);"
        ]
    
    def down(self) -> List[str]:
        return ["DROP TABLE IF EXISTS asset_snapshots;"]
```

## 🧪 Testing Guidelines

### Test Organization
```
tests/
├── unit/                    # Fast, isolated tests
│   ├── domain/             # Domain entity and business logic tests  
│   ├── application/        # Service and use case tests
│   └── infrastructure/     # Repository and adapter tests
├── integration/            # Multi-component tests with real dependencies
│   ├── database/          # Database integration tests
│   └── api/               # API integration tests
└── end_to_end/            # Full application workflow tests
```

### Test Data Management
```python
# Use factories for consistent test data
@pytest.fixture
def sample_portfolio():
    return Portfolio(
        portfolio_id=uuid4(),
        name="Test Portfolio",
        base_ccy="USD",
        cash_balance=Decimal("100000.00"),
        created=datetime.now(timezone.utc)
    )

@pytest.fixture  
def sample_asset():
    return Asset(
        symbol="AAPL",
        exchange="NASDAQ", 
        asset_type=AssetType.STOCK,
        name="Apple Inc."
    )

# Use parametrization for multiple test cases
@pytest.mark.parametrize("side,quantity,expected_cash", [
    (TradeSide.BUY, Decimal("10"), Decimal("98500.00")),
    (TradeSide.SELL, Decimal("5"), Decimal("107500.00")),
])
async def test_trade_execution_cash_impact(side, quantity, expected_cash):
    # Test implementation
    pass
```

## 📊 Performance Guidelines

### Async/Await Best Practices
```python
# ✅ CORRECT - Proper async context management
async def process_multiple_symbols(self, symbols: List[str]) -> List[IngestionResult]:
    tasks = [self.ingest_symbol(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]

# ✅ CORRECT - Proper resource cleanup
async def with_database_transaction(self, operations: List[Operation]):
    async with self.transaction_manager.transaction():
        for operation in operations:
            await operation.execute()

# ❌ INCORRECT - Blocking calls in async context  
async def bad_async_method(self):
    time.sleep(1)  # Blocks the event loop
    result = requests.get("http://api.example.com")  # Blocking HTTP call
    return result.json()
```

### Database Query Optimization
```python
# ✅ CORRECT - Batch operations for efficiency
async def save_multiple_snapshots(self, snapshots: List[AssetSnapshot]) -> None:
    sql = """
    INSERT INTO asset_snapshots (symbol, timestamp, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    parameters_list = [
        [s.symbol, s.timestamp, str(s.open), str(s.high), str(s.low), str(s.close), s.volume]
        for s in snapshots
    ]
    await self.query_executor.execute_batch(sql, parameters_list)

# ❌ INCORRECT - Individual operations in loop
async def save_snapshots_inefficient(self, snapshots: List[AssetSnapshot]) -> None:
    for snapshot in snapshots:  # Each call is a separate database round-trip
        await self.save_snapshot(snapshot)
```

## 🔒 Security Guidelines

### Input Validation
```python
from portfolio_manager.domain.validation import validate_symbol, validate_currency

async def create_portfolio(
    self,
    name: str,
    base_currency: str,
    initial_cash: Decimal
) -> Portfolio:
    """Create a new portfolio with validated inputs."""
    # Validate inputs at the boundary
    if not name or len(name.strip()) == 0:
        raise DomainValidationError("Portfolio name cannot be empty")
    
    if not validate_currency(base_currency):
        raise DomainValidationError(f"Invalid currency code: {base_currency}")
    
    if initial_cash < Decimal('0'):
        raise DomainValidationError("Initial cash cannot be negative")
    
    # Business logic with validated inputs
    portfolio = Portfolio(
        portfolio_id=uuid4(),
        name=name.strip(),
        base_ccy=base_currency.upper(),
        cash_balance=initial_cash,
        created=datetime.now(timezone.utc)
    )
    
    await self.portfolio_repository.save_portfolio(portfolio)
    return portfolio
```

### SQL Injection Prevention
```python
# ✅ CORRECT - Parameterized queries
async def get_assets_by_exchange(self, exchange: str) -> List[Asset]:
    sql = "SELECT * FROM assets WHERE exchange = ?"
    parameters = [exchange]
    result = await self.query_executor.execute_query(sql, parameters)
    return [Asset.from_dict(row) for row in result.rows]

# ❌ INCORRECT - String interpolation (SQL injection risk)
async def get_assets_dangerous(self, exchange: str) -> List[Asset]:
    sql = f"SELECT * FROM assets WHERE exchange = '{exchange}'"  # NEVER do this
    result = await self.query_executor.execute_query(sql)
    return [Asset.from_dict(row) for row in result.rows]
```

## 📝 Documentation Standards

### Code Documentation
```python
class AssetRepository(ABC):
    """Abstract repository for asset data operations.
    
    This repository defines the interface for asset data persistence
    and retrieval operations. Implementations should handle the
    storage backend specifics while maintaining consistent behavior.
    
    Example:
        >>> repo = DuckDBAssetRepository(connection)
        >>> asset = await repo.get_asset("AAPL")
        >>> snapshots = await repo.get_snapshots("AAPL", start_date, end_date)
    """
    
    @abstractmethod
    async def get_asset(self, symbol: str) -> Optional[Asset]:
        """Retrieve asset by symbol.
        
        Args:
            symbol: Asset symbol (e.g., "AAPL", "GOOGL")
            
        Returns:
            Asset instance if found, None otherwise
            
        Raises:
            DataAccessError: If database operation fails
        """
        pass
```

### API Documentation
```python
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field

class PortfolioCreateRequest(BaseModel):
    """Request model for portfolio creation."""
    
    name: str = Field(..., min_length=1, max_length=255, description="Portfolio name")
    base_currency: str = Field("USD", regex="^[A-Z]{3}$", description="ISO 4217 currency code")
    initial_cash: Decimal = Field(..., ge=0, description="Initial cash balance")

@app.post("/api/portfolios", response_model=PortfolioResponse)
async def create_portfolio(
    request: PortfolioCreateRequest,
    service: PortfolioService = Depends(get_portfolio_service)
) -> PortfolioResponse:
    """Create a new investment portfolio.
    
    Creates a new portfolio with the specified parameters. The portfolio
    will be initialized with the provided cash balance and currency.
    
    Args:
        request: Portfolio creation parameters
        
    Returns:
        Created portfolio details including unique identifier
        
    Raises:
        HTTPException: 400 if validation fails, 500 for server errors
    """
    try:
        portfolio = await service.create_portfolio(
            name=request.name,
            base_currency=request.base_currency,
            initial_cash=request.initial_cash
        )
        return PortfolioResponse.from_portfolio(portfolio)
    except DomainValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Portfolio creation failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
```

## 🔄 Continuous Integration

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: ["--profile", "black"]

  - repo: https://github.com/charliermarsh/ruff-pre-commit
    rev: v0.0.270
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.3.0
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
```

### GitHub Actions Workflow
```yaml
name: CI/CD Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.11, 3.12]
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v3
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e ".[dev]"
    
    - name: Run linting
      run: |
        ruff check portfolio_manager/
        black --check portfolio_manager/
        isort --check-only portfolio_manager/
    
    - name: Run type checking
      run: mypy portfolio_manager/
    
    - name: Run tests with coverage
      run: pytest --cov=portfolio_manager --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

This comprehensive development guide ensures consistent, high-quality code that follows established architectural patterns and best practices.