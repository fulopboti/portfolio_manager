# Test Organization

This directory contains all tests for the StockApp application, organized by test type and architectural layer.

## Directory Structure

```
tests/
├── conftest.py                 # Shared fixtures and pytest configuration
├── fixtures/                   # Reusable test fixtures and data factories
│   └── __init__.py
├── unit/                       # Unit tests (fast, isolated)
│   ├── domain/                 # Domain entity tests
│   │   ├── __init__.py
│   │   └── test_entities.py
│   ├── application/            # Application service tests  
│   │   ├── __init__.py
│   │   └── test_services.py
│   └── infrastructure/         # Infrastructure layer tests
│       ├── __init__.py
│       ├── test_data_access_contracts.py
│       └── duckdb/            # DuckDB-specific tests
│           ├── __init__.py
│           ├── test_connection.py
│           ├── test_query_executor.py
│           ├── test_schema_components.py
│           └── test_migration_manager.py
├── integration/                # Integration tests (slower, with database)
│   ├── __init__.py
│   ├── application/            # End-to-end application tests
│   │   └── __init__.py
│   └── infrastructure/         # Infrastructure integration tests
│       ├── __init__.py
│       └── duckdb/            # DuckDB integration tests
│           ├── __init__.py
│           └── test_integration.py
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose**: Test individual components in isolation
- **Characteristics**: Fast execution, minimal dependencies, mocked external services
- **Markers**: `@pytest.mark.unit`
- **Run with**: `pytest tests/unit/`

### Integration Tests (`tests/integration/`)
- **Purpose**: Test component interactions and database operations
- **Characteristics**: Slower execution, real database connections, full stack testing
- **Markers**: `@pytest.mark.integration`
- **Run with**: `pytest tests/integration/`

## Test Markers

Use pytest markers to categorize and selectively run tests:

- `@pytest.mark.unit` - Unit tests (fast, isolated)
- `@pytest.mark.integration` - Integration tests (with database)
- `@pytest.mark.performance` - Performance benchmarking tests
- `@pytest.mark.slow` - Tests that take significant time
- `@pytest.mark.duckdb` - DuckDB-specific tests
- `@pytest.mark.external` - Tests requiring external services

## Running Tests

### All Tests
```bash
pytest
```

### By Category
```bash
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only
pytest -m "not slow"        # Skip slow tests
pytest -m duckdb           # DuckDB tests only
```

### By Directory
```bash
pytest tests/unit/                      # All unit tests
pytest tests/integration/              # All integration tests
pytest tests/unit/domain/              # Domain unit tests
pytest tests/unit/infrastructure/      # Infrastructure unit tests
```

### With Coverage
```bash
pytest --cov=stockapp --cov-report=term-missing
pytest --cov=stockapp --cov-report=html
```

## Test Fixtures

### Database Fixtures (in conftest.py)
- `temp_database`: Temporary database file path
- `duckdb_connection`: Connected DuckDB connection
- `query_executor`: DuckDB query executor
- `schema_manager`: DuckDB schema manager
- `initialized_database`: Database with schema created

### Domain Fixtures (in conftest.py)
- `sample_asset`: Sample Asset entity
- `sample_portfolio`: Sample Portfolio entity
- `sample_trade`: Sample Trade entity
- `sample_position`: Sample Position entity
- `test_factory`: Test data factory for flexible test data creation

## Best Practices

1. **Test Naming**: Use descriptive test names that explain what is being tested
2. **Test Organization**: Group related tests in classes
3. **Fixtures**: Use fixtures for common test data and setup
4. **Markers**: Always mark tests with appropriate categories
5. **Assertions**: Use descriptive assertion messages
6. **Cleanup**: Ensure tests clean up after themselves
7. **Independence**: Tests should not depend on each other

## Writing New Tests

1. **Unit Tests**: Place in `tests/unit/` matching the source code structure
2. **Integration Tests**: Place in `tests/integration/` for cross-component testing  
3. **Use Fixtures**: Leverage existing fixtures or create new ones in `conftest.py`
4. **Add Markers**: Always mark tests with appropriate categories
5. **Follow Patterns**: Use existing tests as templates for consistency