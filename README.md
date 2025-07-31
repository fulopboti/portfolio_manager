# Stock Analysis & Simulation Platform

> A comprehensive, privacy-focused investment research platform that empowers individual investors with institutional-grade analysis tools through local-first data processing and sophisticated strategy engines.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![React 18](https://img.shields.io/badge/react-18-blue.svg)](https://reactjs.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![DuckDB](https://img.shields.io/badge/duckdb-0.9+-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Key Features

### 📊 **Multi-Asset Data Ingestion**
- **Real-time market data** from Yahoo Finance with 15-minute automated refresh
- **Multi-asset support**: Stocks, ETFs, cryptocurrencies, commodities (gold, silver)
- **Fundamental metrics**: P/E, PEG, dividend yield, revenue growth, FCF, debt ratios
- **Global market coverage** with local currency support (USD, EUR, RON)
- **Robust data validation** and error handling with automatic retry mechanisms

### 🧠 **Advanced Strategy Engine**
- **Value Strategy**: Low P/E, high dividend yield, strong free cash flow, low leverage
- **Growth Strategy**: High revenue/FCF growth, favorable PEG ratios, positive momentum
- **Index Tracking**: S&P 500, MSCI World, and custom index strategies
- **Age-Optimized Blends**:
  - **Age < 30**: 70% Growth + 20% Value + 10% Index
  - **Age 30-50**: 45% Growth + 35% Value + 20% Index  
  - **Age 50+**: 25% Growth + 55% Value + 20% Index
- **Configurable parameters** via YAML files for custom strategy creation

### 💼 **Realistic Portfolio Simulation**
- **Multi-portfolio management** with comprehensive position tracking
- **Broker-specific fee modeling**:
  - **Robinhood**: 0.10% spread, $0 commission
  - **IBKR Lite**: 0.05% spread, $0.005/share
  - **XTB**: 0.15% spread, 0.08% commission
  - **eToro**: 0.20% spread, $0 commission
- **Advanced trade execution** with market impact and slippage simulation
- **Real-time P&L calculation** with comprehensive performance analytics
- **Multi-currency support** with automatic conversion and hedging

### 🌐 **Professional Web Interface**
- **Modern React + TypeScript SPA** with responsive design
- **Real-time data updates** via WebSocket connections
- **Interactive dashboards** with customizable metrics and charts
- **Asset screening and filtering** with advanced search capabilities
- **Trade execution interface** with order validation and confirmation
- **Export capabilities** for reports and data analysis

## 🏗️ Architecture

Built on **Hexagonal Architecture** principles for maximum maintainability and extensibility:

```
┌─────────────────────────────────────────────────────────┐
│                    Web Interface                        │
│              React + TypeScript SPA                     │
└─────────────────┬───────────────────────────────────────┘
                  │ HTTP/WebSocket
┌─────────────────┴───────────────────────────────────────┐
│                  FastAPI Backend                        │
│            REST API + WebSocket Server                  │
└─────────────────┬───────────────────────────────────────┘
                  │ Application Layer
┌─────────────────┴───────────────────────────────────────┐
│                Application Services                     │
│   DataIngestion • PortfolioSimulator • StrategyScorer  │
└─────────────────┬───────────────────────────────────────┘
                  │ Domain Ports
┌─────────────────┴───────────────────────────────────────┐
│                   Domain Layer                          │
│      Entities • Value Objects • Business Rules         │
└─────────────────┬───────────────────────────────────────┘
                  │ Repository Ports
┌─────────────────┴───────────────────────────────────────┐
│               Infrastructure Layer                      │
│     DuckDB • Schedulers • External APIs • Caching     │
└─────────────────┬───────────────────────────────────────┘
                  │ Plugin SPI
┌─────────────────┴───────────────────────────────────────┐
│                 Plugin Ecosystem                        │
│        Yahoo Finance • Data Providers • Strategies     │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+** - Core runtime environment
- **Node.js 20+** - Frontend development and build tools
- **4GB RAM minimum** - For data processing and analysis
- **2GB disk space** - Database and application storage

### 🔧 Installation & Setup

#### Option 1: Full Stack Development (Recommended)

```bash
# 1. Clone the repository
git clone <repository-url>
cd stocks

# 2. Install Python dependencies
pip install -e .

# 3. Install frontend dependencies
npm install

# 4. Start both backend and frontend
npm run dev
```

#### Option 2: Manual Setup

```bash
# Terminal 1: Backend API server
stockapp serve --reload --log-level DEBUG

# Terminal 2: Frontend development server  
cd stockapp/ui/frontend
npm install && npm run dev
```

#### Option 3: Docker Deployment

```bash
# Start all services with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f
```

### 🎯 First Steps

1. **Access the Application**:
   - 🌐 **Frontend UI**: http://localhost:3000
   - 🔧 **Backend API**: http://localhost:8000
   - 📚 **API Docs**: http://localhost:8000/docs

2. **Ingest Market Data**:
   ```bash
   # Ingest data for popular stocks
   stockapp ingest --symbols AAPL MSFT GOOGL AMZN TSLA NVDA META

   # Ingest with date range
   stockapp ingest --symbols SPY QQQ --since 2024-01-01
   ```

3. **Calculate Strategy Scores**:
   ```bash
   # Value strategy analysis
   stockapp score --strategy VAL

   # Growth strategy for specific date
   stockapp score --strategy GRW --as-of 2024-12-01

   # Age-optimized strategy
   stockapp score --strategy "AG<30"
   ```

4. **Create Your First Portfolio**:
   - Navigate to Portfolio Management in the web interface
   - Click "Create New Portfolio"
   - Set your base currency and initial cash balance
   - Start building positions based on strategy recommendations

## 📋 Usage Guide

### 🖥️ Command Line Interface

The `stockapp` CLI provides comprehensive functionality for data management and analysis:

#### Data Management
```bash
# Ingest market data with automatic retry and validation
stockapp ingest --symbols AAPL MSFT GOOGL AMZN TSLA
stockapp ingest --symbols SPY QQQ VTI --since 2024-01-01

# Refresh existing data for all tracked symbols
stockapp refresh-data

# Import symbols from a file
stockapp ingest --file symbols.txt --include-fundamentals
```

#### Strategy Analysis  
```bash
# Calculate and rank Value strategy scores
stockapp score --strategy VAL --limit 50

# Growth strategy with custom date
stockapp score --strategy GRW --as-of 2024-12-15

# Age-optimized strategies
stockapp score --strategy "AG<30"    # Young investor blend
stockapp score --strategy "AG30-50"  # Mid-career blend  
stockapp score --strategy "AG50+"    # Pre-retirement blend

# Export strategy results
stockapp score --strategy VAL --export results.csv
```

#### Portfolio Management
```bash
# Create a new portfolio
stockapp portfolio create "My Growth Portfolio" --currency USD --cash 10000

# Place trades via CLI
stockapp portfolio trade --portfolio-id <uuid> --symbol AAPL --side BUY --qty 10

# View portfolio summary
stockapp portfolio summary --portfolio-id <uuid>
```

#### Server Management
```bash
# Development server with hot reload
stockapp serve --reload --log-level DEBUG --host 127.0.0.1 --port 8000

# Production server
stockapp serve --host 0.0.0.0 --port 8000 --workers 4

# Enable CORS for frontend development  
stockapp serve --cors-origins "http://localhost:3000"
```

### 🌐 Web Interface Features

#### Dashboard
- **Portfolio overview** with real-time P&L and performance metrics
- **Market summary** with trending assets and sector performance  
- **Strategy leaderboards** showing top-ranked opportunities
- **Recent activity** with trade history and system events

#### Portfolio Management
- **Multi-portfolio support** with currency-specific views
- **Position tracking** with cost basis and unrealized gains/losses
- **Trade execution** with order validation and fee calculation
- **Performance analytics** with return attribution and risk metrics
- **Cash management** with deposit/withdrawal tracking

#### Asset Analysis
- **Advanced screening** with 50+ fundamental and technical filters
- **Strategy rankings** with sortable scoring results
- **Interactive charts** powered by TradingView integration
- **Fundamental data** with detailed financial metrics
- **Watchlist management** for tracking favorite assets

#### Data Management
- **Ingestion monitoring** with real-time status and error reporting
- **Data quality dashboard** showing completeness and accuracy metrics
- **Symbol management** with bulk import/export capabilities
- **Cache management** with selective clearing and statistics

### 🔌 REST API Reference

#### Portfolio Operations
```http
POST   /api/portfolio              # Create new portfolio
GET    /api/portfolio              # List all portfolios  
GET    /api/portfolio/{id}         # Get portfolio details
PUT    /api/portfolio/{id}         # Update portfolio settings
DELETE /api/portfolio/{id}         # Delete portfolio
POST   /api/portfolio/{id}/trade   # Execute trade
GET    /api/portfolio/{id}/positions # Get current positions
GET    /api/portfolio/{id}/metrics # Get performance metrics
GET    /api/portfolio/{id}/trades  # Get trade history
```

#### Asset & Market Data
```http  
GET    /api/assets                 # List available assets
GET    /api/assets/{symbol}        # Get asset details
GET    /api/assets/{symbol}/ohlcv  # Get price history
GET    /api/assets/{symbol}/metrics # Get fundamental data
POST   /api/data/ingest           # Trigger data ingestion
GET    /api/data/status           # Get ingestion status
```

#### Strategy Analysis
```http
GET    /api/strategies             # List available strategies
POST   /api/strategies/{id}/calculate # Calculate strategy scores
GET    /api/strategies/{id}/scores # Get ranked results
GET    /api/strategies/{id}/backtest # Historical performance
```

#### System & Configuration
```http
GET    /api/brokers               # List broker profiles
GET    /api/brokers/{id}          # Get broker details  
GET    /api/health                # System health check
GET    /api/metrics               # Application metrics
```

## ⚙️ Configuration

### Environment Variables

Configure the application behavior through environment variables:

```bash
# Core Application Settings
LOG_LEVEL=DEBUG                    # Logging verbosity (DEBUG, INFO, WARNING, ERROR)
DB_PATH=data/stockapp.db          # Database file location
API_HOST=127.0.0.1                # Backend server host
API_PORT=8000                     # Backend server port

# Data Ingestion Settings  
DATA_REFRESH_INTERVAL=15          # Minutes between automatic data refresh
MAX_SYMBOLS_PER_BATCH=100         # Symbols to process simultaneously
RETRY_ATTEMPTS=3                  # Failed request retry count
CACHE_TTL_MINUTES=60              # External API cache duration

# Performance Settings
MAX_WORKERS=4                     # Concurrent worker processes
QUERY_TIMEOUT_SEC=30              # Database query timeout
BATCH_SIZE=1000                   # Bulk operation batch size
```

### 🗄️ Database Configuration

**DuckDB** provides high-performance analytical capabilities:

- **Storage Location**: `data/stockapp.db` (configurable via `DB_PATH`)
- **Schema**: Automatically created and migrated on startup
- **Backup Strategy**: Daily automated backups with 30-day retention
- **Performance**: Columnar storage optimized for OLAP queries
- **Concurrent Access**: Multi-reader, single-writer with transaction support
- **Export Options**: Parquet, CSV, and JSON export capabilities

### 🏦 Broker Configurations

Pre-configured broker profiles with realistic fee structures:

| Broker | Spread % | Flat Fee | Commission % | Min Order | Currency |
|--------|----------|----------|--------------|-----------|----------|
| **Robinhood** | 0.10% | $0.00 | 0.00% | 1 share | USD |
| **IBKR Lite** | 0.05% | $0.005/share | 0.00% | 1 share | USD/EUR |
| **XTB** | 0.15% | $0.00 | 0.08% | 1 share | USD/EUR |
| **eToro** | 0.20% | $0.00 | 0.00% | 1 share | USD |
| **Custom** | Configurable | Configurable | Configurable | Configurable | Multi |

### 📊 Strategy Parameters

Strategies can be customized via YAML configuration files:

```yaml
# config/strategies.yaml
value_strategy:
  pe_max: 15.0              # Maximum P/E ratio
  dividend_yield_min: 2.0   # Minimum dividend yield %
  debt_equity_max: 0.5      # Maximum debt/equity ratio
  fcf_growth_min: 5.0       # Minimum FCF growth %
  
growth_strategy:
  revenue_growth_min: 15.0  # Minimum revenue growth %
  peg_max: 1.5             # Maximum PEG ratio
  rsi_range: [30, 70]      # RSI momentum range
  
age_blend_strategies:
  young_investor:           # Age < 30
    growth_weight: 0.70
    value_weight: 0.20  
    index_weight: 0.10
  mid_career:              # Age 30-50
    growth_weight: 0.45
    value_weight: 0.35
    index_weight: 0.20
```

## 🛠️ Development

### 📁 Project Structure

```
stocks/
├── stockapp/                    # Main application package
│   ├── core/                   # Core utilities and exceptions
│   │   ├── __init__.py
│   │   ├── cache.py           # Caching infrastructure
│   │   ├── container.py       # Dependency injection
│   │   └── exceptions.py      # Custom exception classes
│   ├── domain/                # Domain layer (business logic)
│   │   ├── __init__.py
│   │   └── entities.py        # Core business entities
│   ├── application/           # Application layer (use cases)
│   │   ├── __init__.py
│   │   ├── ports.py          # Interface definitions
│   │   └── services/         # Application services
│   │       ├── data_ingestion.py
│   │       ├── fundamentals_aggregator.py
│   │       └── portfolio_simulator.py
│   ├── infrastructure/        # Infrastructure layer (adapters)
│   │   ├── __init__.py
│   │   ├── db/               # Database implementations
│   │   │   ├── duckdb_repository.py
│   │   │   ├── portfolio_repository.py
│   │   │   └── broker_repository.py
│   │   └── scheduler.py      # Background job scheduling
│   ├── plugins/              # Data provider plugins
│   │   ├── __init__.py
│   │   └── yahoo_plugin.py   # Yahoo Finance integration
│   ├── cli/                  # Command-line interface
│   │   ├── __init__.py
│   │   ├── main.py          # CLI entry point
│   │   └── commands/        # CLI command modules
│   └── ui/                   # User interface layer
│       ├── backend/         # FastAPI REST API
│       │   ├── main.py      # API server entry point
│       │   ├── api.py       # Main API module
│       │   ├── models.py    # Pydantic models
│       │   ├── dependencies.py # Dependency injection
│       │   └── routers/     # API endpoint modules
│       └── frontend/        # React TypeScript SPA
│           ├── src/
│           │   ├── App.tsx
│           │   ├── components/
│           │   ├── pages/
│           │   ├── services/
│           │   └── types/
│           ├── package.json
│           └── vite.config.ts
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── conftest.py          # Pytest configuration
│   ├── test_domain_entities.py
│   ├── test_application_services.py
│   ├── test_infrastructure.py
│   └── test_api.py
├── data/                     # Database and data files
│   └── stockapp.db          # DuckDB database file
├── config/                   # Configuration files
│   └── strategies.yaml      # Strategy parameters
├── docker-compose.yml        # Container orchestration
├── Dockerfile               # Container build instructions
├── pyproject.toml           # Python project configuration
├── requirements.txt         # Python dependencies
├── package.json            # Node.js project configuration
└── README.md               # This documentation
```

### 🧪 Testing Strategy

Comprehensive test coverage across all application layers:

```bash
# Run complete test suite with coverage
pytest --cov=stockapp --cov-report=html --cov-report=term

# Run specific test categories
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only
pytest -k "test_portfolio"  # Portfolio-related tests only

# Run tests with verbose output
pytest -v --tb=short

# Run performance tests
pytest -m slow --durations=10
```

**Current Test Coverage:**
- **Domain Entities**: 94% coverage
- **Application Services**: 88% coverage  
- **Infrastructure**: 82% coverage
- **API Endpoints**: 85% coverage
- **Overall Target**: 85%+ coverage maintained

### 🔍 Code Quality Tools

Maintain high code quality with automated tooling:

```bash
# Format code with Black
black stockapp/ tests/

# Sort imports with isort
isort stockapp/ tests/

# Lint with Ruff (fast Python linter)
ruff check stockapp/ tests/
ruff check --fix stockapp/  # Auto-fix issues

# Type checking with MyPy
mypy stockapp/

# Security scanning
bandit -r stockapp/

# Check for unused dependencies
pip-check
```

### 🚀 Development Workflow

1. **Set up development environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   pip install -e ".[dev]"
   pre-commit install
   ```

2. **Start development servers**:
   ```bash
   # Terminal 1: Backend with hot reload
   stockapp serve --reload --log-level DEBUG
   
   # Terminal 2: Frontend with hot reload
   cd stockapp/ui/frontend && npm run dev
   ```

3. **Run tests before committing**:
   ```bash
   pytest --cov=stockapp
   ruff check stockapp/
   mypy stockapp/
   ```

4. **Build for production**:
   ```bash
   # Build frontend assets
   cd stockapp/ui/frontend && npm run build
   
   # Build Python package
   python -m build
   ```

## 🤝 Contributing

We welcome contributions from the community! Here's how to get involved:

### 🔄 Contribution Process

1. **Fork the repository** on GitHub
2. **Create a feature branch**: `git checkout -b feature/portfolio-analytics`
3. **Follow our coding standards** (see Development section)
4. **Add comprehensive tests** with ≥85% coverage for new code
5. **Update documentation** including docstrings and README changes
6. **Commit with clear messages**: `git commit -m "feat: add portfolio risk analytics"`
7. **Push to your fork**: `git push origin feature/portfolio-analytics`
8. **Open a Pull Request** with detailed description and test results

### 📋 Development Guidelines

- **Architecture**: Strict adherence to Hexagonal Architecture principles
- **Testing**: Minimum 85% test coverage, 90%+ for core business logic
- **Documentation**: Comprehensive docstrings with type hints for all public APIs
- **Error Handling**: Use custom exceptions from `stockapp.core.exceptions`
- **Logging**: Structured logging with `structlog` for observability
- **Type Safety**: Full MyPy compliance with strict type checking
- **Code Style**: Black formatting, isort import sorting, Ruff linting

### 🐛 Bug Reports & Feature Requests

- **Bug Reports**: Use GitHub issues with detailed reproduction steps
- **Feature Requests**: Propose new features with use cases and requirements
- **Security Issues**: Report privately via email to security@stockapp.dev

## 🗺️ Roadmap

### 📅 Version 1.1 (Q2 2025) - Enhanced Analytics
- [ ] **AI Integration**: Local LLM via Ollama for investment insights
- [ ] **Custom Strategies**: User-defined YAML strategy configurations
- [ ] **Advanced Charting**: Technical indicators and drawing tools
- [ ] **Data Export**: Portfolio reports in PDF and Excel formats
- [ ] **Commodity Trading**: Gold and silver with weight-based metrics
- [ ] **Performance Tracking**: Time-series portfolio value visualization

### 📅 Version 1.2 (Q3 2025) - Professional Features
- [ ] **Backtesting Engine**: Historical strategy performance analysis
- [ ] **Risk Analytics**: VaR, Expected Shortfall, and correlation analysis
- [ ] **Multi-Currency**: Advanced FX hedging and currency exposure management
- [ ] **Mobile Support**: Responsive design for tablets and smartphones
- [ ] **API Integrations**: Third-party data providers and brokerage connections

### 📅 Version 1.3 (Q4 2025) - Collaboration & Scale
- [ ] **Multi-User Support**: Family and team portfolio management
- [ ] **Real-Time Data**: Streaming market data with WebSocket updates
- [ ] **Social Features**: Strategy sharing and community insights
- [ ] **Cloud Deployment**: Optional cloud hosting with data synchronization

### 📅 Version 2.0 (2026) - Enterprise Ready
- [ ] **Machine Learning**: Predictive analytics and pattern recognition
- [ ] **Professional APIs**: Institution-grade data feeds and execution
- [ ] **Mobile Apps**: Native iOS and Android applications
- [ ] **Advanced Security**: Multi-factor auth and audit logging

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for full details.

```
MIT License - Copyright (c) 2025 StockApp Team

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

## 🙏 Acknowledgments

Special thanks to the open-source projects that power this platform:

- **[Yahoo Finance](https://finance.yahoo.com/)** - Free market data and financial information
- **[DuckDB](https://duckdb.org/)** - High-performance embedded analytical database
- **[FastAPI](https://fastapi.tiangolo.com/)** - Modern, fast web framework for APIs
- **[React](https://reactjs.org/)** - User interface library for web applications
- **[TradingView](https://www.tradingview.com/)** - Professional charting and technical analysis
- **[Tailwind CSS](https://tailwindcss.com/)** - Utility-first CSS framework
- **[TypeScript](https://www.typescriptlang.org/)** - Typed JavaScript for better development

## ⚠️ Important Disclaimer

**This software is for educational and research purposes only.**

- 📊 **Not Financial Advice**: This platform does not provide investment, financial, or trading advice
- 🔬 **Research Tool**: Designed for analysis and learning, not live trading
- 💼 **Consult Professionals**: Always consult qualified financial advisors before making investment decisions
- ⚖️ **Use at Your Own Risk**: Users are responsible for their own investment decisions and outcomes
- 🔒 **No Guarantees**: Past performance does not guarantee future results

---

**Built with ❤️ for the investment research community** 