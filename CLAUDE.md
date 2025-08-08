# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- Run all tests: `pytest` or `python tests/run_tests.py all`
- Run unit tests only: `python tests/run_tests.py unit` or `pytest tests/unit/`
- Run integration tests: `python tests/run_tests.py integration` or `pytest tests/integration/`
- Run tests with coverage: `python tests/run_tests.py coverage`
- Run specific test categories:
  - DuckDB tests: `python tests/run_tests.py duckdb`
  - Domain layer: `python tests/run_tests.py domain`
  - Application layer: `python tests/run_tests.py application`
  - Infrastructure layer: `python tests/run_tests.py infrastructure`
- Single test file: `pytest tests/unit/domain/test_entities.py -v`

### Code Quality
- Format code: `black portfolio_manager/`
- Sort imports: `isort portfolio_manager/`
- Lint code: `ruff check portfolio_manager/`
- Type checking: `mypy portfolio_manager/`
- Security scan: `bandit -r portfolio_manager/`

### Application Commands
- Install in development mode: `pip install -e ".[dev]"`
- Run development server: `portfolio-manager serve --reload --log-level DEBUG`
- Ingest market data: `portfolio-manager ingest --symbols AAPL MSFT GOOGL`
- Calculate strategy scores: `portfolio-manager score --strategy VAL --limit 50`
- Create portfolio: `portfolio-manager portfolio create "My Portfolio" --currency USD --cash 10000`

## Architecture Overview

This is a **Hexagonal Architecture** (Ports and Adapters) investment research platform with strict layer separation:

### Layer Structure
- **Domain Layer** (`portfolio_manager/domain/`): Core business logic, entities, and domain events
- **Application Layer** (`portfolio_manager/application/`): Use cases, ports (interfaces), and application services
- **Infrastructure Layer** (`portfolio_manager/infrastructure/`): Database adapters, external APIs, and concrete implementations
- **Configuration** (`portfolio_manager/config/`): YAML-based configuration with environment variable overrides

### Key Components

#### Domain Layer
- `entities.py`: Core business entities (Asset, Portfolio, Position, Trade)
- `events.py`: Domain events for the event-driven architecture
- `exceptions.py`: Domain-specific exceptions
- Entity types: `AssetType` (STOCK, ETF, CRYPTO, COMMODITY), `TradeSide` (BUY, SELL)

#### Application Layer
- `ports.py`: Abstract interfaces for repositories and external services
- `services/`: Application services (DataIngestion, PortfolioSimulator, StrategyScorer)
- `events/`: Event handlers and event system orchestration

#### Infrastructure Layer
- `duckdb/`: DuckDB-specific implementations for data storage
  - `connection.py`: Database connection management
  - `schema/`: Schema management and migrations
  - Repository pattern implementations for each domain aggregate
- `data_access/`: Data access layer with query executors and specialized DAOs
- `events/`: Event bus implementation for inter-service communication

#### Configuration System
- Environment-based configuration with YAML defaults
- Configurations in `config/defaults/`: `base.yaml`, `development.yaml`, `production.yaml`, `testing.yaml`
- Override via environment variables with `PORTFOLIO_MANAGER_` prefix

### Event System
The application uses an event-driven architecture with:
- Domain events for business state changes
- Event handlers for cross-cutting concerns (auditing, notifications)
- Event bus for decoupled communication between services

### Database Strategy
- **Primary Database**: DuckDB for analytical workloads and historical data
- **Schema Management**: Automated migrations and schema validation
- **Query Strategy**: Repository pattern with specialized query builders

### Strategy Engine
Three main investment strategies with configurable parameters:
- **Value Strategy**: Low P/E, high dividend yield, strong FCF, low leverage
- **Growth Strategy**: High revenue/FCF growth, favorable PEG ratios
- **Age-Optimized Blends**: Dynamic asset allocation based on investor age

### Testing Strategy
- **Unit Tests**: Domain logic and isolated component testing
- **Integration Tests**: Database operations and cross-service interactions
- **Test Categories**: Use pytest markers (`unit`, `integration`, `duckdb`, `slow`)
- **Coverage Target**: Minimum 95% as enforced in pytest.ini

## Configuration Management

Configuration is managed through:
1. YAML files in `config/defaults/` for base settings
2. Environment-specific overrides (development, production, testing)
3. Environment variables with `PORTFOLIO_MANAGER_` prefix
4. Pydantic schemas for validation in `config/schema.py`

## Important Development Notes

### Code Style Requirements
- **Formatting**: Black with 88-character line length
- **Import Sorting**: isort with black profile
- **Type Checking**: Full MyPy compliance with strict settings
- **Linting**: Ruff with comprehensive rule set (E, W, F, I, B, C4, UP)

### Repository Patterns
When working with data access, follow the established repository patterns:
- Use abstract ports from `application/ports.py`
- Implement concrete repositories in `infrastructure/duckdb/`
- Follow the existing naming conventions: `AssetRepository`, `PortfolioRepository`

### Error Handling
- Use domain-specific exceptions from `domain/exceptions.py`
- Maintain clear exception hierarchies with meaningful error messages
- Log errors using structured logging via `structlog`

### Event-Driven Development
When adding new features:
- Define domain events in `domain/events.py`
- Create event handlers in `application/events/`
- Register handlers with the event bus in `infrastructure/events/`

### Testing Guidelines
- Write tests for all three layers with appropriate isolation
- Use dependency injection for testing with mock implementations
- Integration tests should use real DuckDB instances with cleanup
- Mark tests appropriately (`@pytest.mark.unit`, `@pytest.mark.integration`, etc.)

## CLI Entry Point

The main CLI is accessible via `portfolio-manager` command after installation, implemented in the (currently missing) `cli/main.py` module.