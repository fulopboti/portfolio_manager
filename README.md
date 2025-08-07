# Portfolio Manager

> A comprehensive, privacy-focused investment research platform that empowers individual investors with institutional-grade analysis tools through local-first data processing and sophisticated strategy engines.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![React 18](https://img.shields.io/badge/react-18-blue.svg)](https://reactjs.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![DuckDB](https://img.shields.io/badge/duckdb-0.9+-yellow.svg)](https://duckdb.org/)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL%203.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

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
cd portfolio-manager

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
portfolio-manager serve --reload --log-level DEBUG

# Terminal 2: Frontend development server  
cd portfolio_manager/ui/frontend
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
   portfolio-manager ingest --symbols AAPL MSFT GOOGL AMZN TSLA NVDA META

   # Ingest with date range
   portfolio-manager ingest --symbols SPY QQQ --since 2024-01-01
   ```

3. **Calculate Strategy Scores**:
   ```bash
   # Value strategy analysis
   portfolio-manager score --strategy VAL

   # Growth strategy for specific date
   portfolio-manager score --strategy GRW --as-of 2024-12-01

   # Age-optimized strategy
   portfolio-manager score --strategy "AG<30"
   ```

4. **Create Your First Portfolio**:
   - Navigate to Portfolio Management in the web interface
   - Click "Create New Portfolio"
   - Set your base currency and initial cash balance
   - Start building positions based on strategy recommendations

## 📋 Usage Guide

### 🖥️ Command Line Interface

The `portfolio-manager` CLI provides comprehensive functionality for data management and analysis:

#### Data Management
```bash
# Ingest market data with automatic retry and validation
portfolio-manager ingest --symbols AAPL MSFT GOOGL AMZN TSLA
portfolio-manager ingest --symbols SPY QQQ VTI --since 2024-01-01

# Refresh existing data for all tracked symbols
portfolio-manager refresh-data

# Import symbols from a file
portfolio-manager ingest --file symbols.txt --include-fundamentals
```

#### Strategy Analysis  
```bash
# Calculate and rank Value strategy scores
portfolio-manager score --strategy VAL --limit 50

# Growth strategy with custom date
portfolio-manager score --strategy GRW --as-of 2024-12-15

# Age-optimized strategies
portfolio-manager score --strategy "AG<30"    # Young investor blend
portfolio-manager score --strategy "AG30-50"  # Mid-career blend  
portfolio-manager score --strategy "AG50+"    # Pre-retirement blend

# Export strategy results
portfolio-manager score --strategy VAL --export results.csv
```

#### Portfolio Management
```bash
# Create a new portfolio
portfolio-manager portfolio create "My Growth Portfolio" --currency USD --cash 10000

# Place trades via CLI
portfolio-manager portfolio trade --portfolio-id <uuid> --symbol AAPL --side BUY --qty 10

# View portfolio summary
portfolio-manager portfolio summary --portfolio-id <uuid>
```

#### Server Management
```bash
# Development server with hot reload
portfolio-manager serve --reload --log-level DEBUG --host 127.0.0.1 --port 8000

# Production server
portfolio-manager serve --host 0.0.0.0 --port 8000 --workers 4

# Enable CORS for frontend development  
portfolio-manager serve --cors-origins "http://localhost:3000"
```

### 🌐 Web Interface Features

#### Dashboard
- **Portfolio overview** with real-time P&L and performance metrics
- **Market summary** with key indices and sector performance
- **Recent activity** showing latest trades and data updates
- **Quick actions** for common tasks like data refresh and portfolio creation

#### Portfolio Management
- **Multi-portfolio support** with separate tracking and analysis
- **Position details** with cost basis, unrealized P&L, and performance history
- **Trade execution** with order validation and fee calculation
- **Performance analytics** with charts and risk metrics
- **Rebalancing tools** with strategy-based recommendations

#### Asset Analysis
- **Asset screener** with customizable filters and criteria
- **Strategy rankings** showing top-scoring assets for each strategy
- **Technical charts** with TradingView integration
- **Fundamental data** with key ratios and growth metrics
- **Comparison tools** for side-by-side asset analysis

#### Data Management
- **Data ingestion status** with progress tracking and error reporting
- **Symbol management** for adding/removing tracked assets
- **Data quality monitoring** with validation and integrity checks
- **System health** with performance metrics and resource usage

## 📁 Project Structure

```
portfolio-manager/
├── portfolio_manager/           # Main application package
│   ├── domain/                 # Domain layer (business logic)
│   │   ├── __init__.py
│   │   ├── entities.py         # Core business entities
│   │   ├── events.py           # Domain events
│   │   ├── exceptions.py       # Domain exceptions
│   │   └── validation.py       # Business rule validation
│   ├── application/            # Application layer (use cases)
│   │   ├── __init__.py
│   │   ├── ports.py           # Interface definitions
│   │   └── services/          # Application services
│   │       ├── data_ingestion.py
│   │       ├── portfolio_simulator.py
│   │       └── strategy_scorer.py
│   ├── infrastructure/         # Infrastructure layer (adapters)
│   │   ├── __init__.py
│   │   ├── duckdb/            # Database implementations
│   │   │   ├── connection.py
│   │   │   ├── repository_factory.py
│   │   │   └── schema/
│   │   └── events/            # Event system
│   ├── config/                # Configuration management
│   │   ├── __init__.py
│   │   ├── settings.py        # Configuration loading
│   │   ├── schema.py          # Pydantic schemas
│   │   └── defaults/          # Default configuration files
│   └── cli/                   # Command-line interface
│       ├── __init__.py
│       └── main.py           # CLI entry point
├── tests/                     # Test suite
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── conftest.py          # Test configuration
├── docs/                     # Documentation
├── scripts/                  # Utility scripts
├── pyproject.toml           # Project configuration
├── README.md                # This file
└── LICENSE                  # MIT License
```

## 🛠️ Development

### 📋 Prerequisites

- **Python 3.11+** with pip and virtualenv
- **Node.js 20+** with npm
- **Git** for version control
- **Docker** (optional) for containerized development

### 🔧 Development Setup

```bash
# 1. Clone and setup
git clone <repository-url>
cd portfolio-manager
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# 2. Install dependencies
pip install -e ".[dev]"
npm install

# 3. Setup pre-commit hooks
pre-commit install

# 4. Run initial tests
pytest
```

### 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=portfolio_manager --cov-report=html

# Run specific test categories
pytest tests/unit/           # Unit tests only
pytest tests/integration/    # Integration tests only
pytest -m "not slow"        # Exclude slow tests

# Run linting and type checking
ruff check portfolio_manager/
mypy portfolio_manager/

# Security scanning
bandit -r portfolio_manager/

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
   portfolio-manager serve --reload --log-level DEBUG
   
   # Terminal 2: Frontend with hot reload
   cd portfolio_manager/ui/frontend && npm run dev
   ```

3. **Run tests before committing**:
   ```bash
   pytest --cov=portfolio_manager
   ruff check portfolio_manager/
   mypy portfolio_manager/
   ```

4. **Build for production**:
   ```bash
   # Build frontend assets
   cd portfolio_manager/ui/frontend && npm run build
   
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
- **Error Handling**: Use custom exceptions from `portfolio_manager.core.exceptions`
- **Logging**: Structured logging with `structlog` for observability
- **Type Safety**: Full MyPy compliance with strict type checking
- **Code Style**: Black formatting, isort import sorting, Ruff linting

### 🐛 Bug Reports & Feature Requests

- **Bug Reports**: Use GitHub issues with detailed reproduction steps
- **Feature Requests**: Propose new features with use cases and requirements
- **Security Issues**: Report privately via email to security@portfolio-manager.dev

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

This project is licensed under the **GNU General Public License v3.0** - see the [LICENSE](LICENSE) file for full details.


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