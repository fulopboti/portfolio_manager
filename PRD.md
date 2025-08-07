# Product Requirements Document (PRD)
## Portfolio Manager

**Version:** 1.0  
**Date:** July 2025  
**Document Owner:** Fulop Botond  

---

## 1. Executive Summary

### 1.1 Product Vision
A single-user, local-first research application that empowers individual investors with institutional-grade analysis tools by ingesting multi-market asset data, implementing sophisticated strategy engines, and providing realistic portfolio simulation capabilities.

### 1.2 Product Mission
To democratize quantitative investment research by providing a comprehensive, privacy-focused platform that combines real-time market data ingestion, advanced strategy scoring, and realistic portfolio simulation in a single, locally-deployed application.

### 1.3 Key Value Propositions
- **Local-First Architecture**: Complete data privacy with no external dependencies for analysis
- **Multi-Asset Coverage**: Stocks, ETFs, crypto, commodities (gold, silver) from global markets
- **Strategy Engine**: Multiple pre-built strategies (Value, Growth, Index, Age-Blend)
- **Realistic Simulation**: Broker-specific fees, spreads, and trading constraints
- **Real-Time Data**: 15-minute automated data refresh from free public sources
- **Professional UI**: Modern React-based interface with TradingView integration

---

## 2. Product Overview

### 2.1 Product Type
Desktop application with web-based UI, designed for single-user deployment on personal computers or local servers.

### 2.2 Target Market
- **Primary**: Individual retail investors seeking advanced analysis tools
- **Secondary**: Financial advisors managing small client portfolios
- **Tertiary**: Finance students and researchers

### 2.3 Key Differentiators
1. **Hexagonal Architecture**: Clean separation of concerns enabling easy extensibility
2. **Plugin Ecosystem**: Modular data providers and strategy implementations
3. **Realistic Trading**: Broker-specific fee structures and market constraints
4. **Multi-Currency Support**: USD, EUR, RON base currencies
5. **Commodity Integration**: Gold and silver with weight-based metrics

---

## 3. User Personas

### 3.1 Primary Persona: Alex - The Analytical Investor
- **Demographics**: Age 28-45, tech-savvy, portfolio value $50K-500K
- **Goals**: Systematic investment approach, data-driven decisions, performance tracking
- **Pain Points**: Expensive professional tools, scattered data sources, manual analysis
- **Use Cases**: Daily portfolio monitoring, strategy backtesting, rebalancing decisions

### 3.2 Secondary Persona: Sarah - The DIY Researcher
- **Demographics**: Age 35-55, self-directed investor, values privacy
- **Goals**: Independent research, avoiding Wall Street bias, long-term wealth building
- **Pain Points**: Information overload, vendor lock-in, subscription fatigue
- **Use Cases**: Fundamental analysis, trend identification, portfolio construction

---

## 4. Functional Requirements

### 4.1 Core Features

#### 4.1.1 Data Ingestion System
**Priority: P0 (Critical)**

**Requirements:**
- **REQ-DI-001**: Ingest OHLCV data from Yahoo Finance for 15+ minute intervals
- **REQ-DI-002**: Fetch fundamental metrics (P/E, PEG, Dividend Yield, Revenue Growth, FCF Growth, Debt/Equity)
- **REQ-DI-003**: Support multiple data providers through plugin architecture
- **REQ-DI-004**: Automated 15-minute refresh scheduling
- **REQ-DI-005**: Manual data refresh on-demand
- **REQ-DI-006**: Data validation and error handling for invalid/missing data
- **REQ-DI-007**: Support for 500+ symbols simultaneously
- **REQ-DI-008**: Historical data backfill capabilities

**Acceptance Criteria:**
- Data ingestion completes within 2 minutes for 100 symbols
- 99.5% uptime for scheduled data refresh
- Graceful handling of API rate limits and network failures
- Data integrity validation with automatic error reporting

#### 4.1.2 Strategy Engine
**Priority: P0 (Critical)**

**Requirements:**
- **REQ-SE-001**: Value Strategy implementation (Low P/E, High Dividend Yield, Strong FCF, Low Leverage)
- **REQ-SE-002**: Growth Strategy implementation (High Revenue Growth, High FCF Growth, High PEG, Positive Momentum)
- **REQ-SE-003**: Index Strategy implementation (Track major indices: S&P 500, MSCI World)
- **REQ-SE-004**: Age-Blend Strategies:
  - Age < 30: 70% Growth + 20% Value + 10% Index
  - Age 30-50: 45% Growth + 35% Value + 20% Index  
  - Age 50+: 25% Growth + 55% Value + 20% Index
- **REQ-SE-005**: Strategy scoring with 0-100 scale normalization
- **REQ-SE-006**: Configurable strategy parameters via YAML files
- **REQ-SE-007**: Multi-factor ranking with weighted scoring
- **REQ-SE-008**: Historical strategy performance tracking

**Acceptance Criteria:**
- Strategy calculations complete within 30 seconds for 1000+ assets
- Scoring results are deterministic and reproducible
- Strategy parameters are user-configurable without code changes
- Performance metrics include Sharpe ratio, maximum drawdown, and alpha

#### 4.1.3 Portfolio Simulation Engine
**Priority: P0 (Critical)**

**Requirements:**
- **REQ-PS-001**: Virtual portfolio creation with multi-currency support (USD, EUR, RON)
- **REQ-PS-002**: Realistic order execution with market impact simulation
- **REQ-PS-003**: Broker-specific fee structures:
  - Robinhood: 0.10% pip, $0 flat fee, 0% commission
  - IBKR Lite: 0.05% pip, $0.005/share flat fee, 0% commission
  - XTB: 0.15% pip, $0 flat fee, 0.08% commission
  - eToro: 0.20% pip, $0 flat fee, 0% commission
- **REQ-PS-004**: Position tracking with average cost basis calculation
- **REQ-PS-005**: Cash management with deposit/withdrawal functionality
- **REQ-PS-006**: Real-time P&L calculation and performance metrics
- **REQ-PS-007**: Trade history and audit trail
- **REQ-PS-008**: Portfolio rebalancing recommendations

**Acceptance Criteria:**
- Order execution simulation includes realistic slippage (0.05-0.20%)
- Performance calculations update within 1 second of price changes
- Portfolio metrics include total return, volatility, and risk-adjusted returns
- Trade validation prevents overselling and insufficient cash scenarios

#### 4.1.4 Web User Interface
**Priority: P0 (Critical)**

**Requirements:**
- **REQ-UI-001**: Modern React-based single-page application
- **REQ-UI-002**: Responsive design supporting desktop and tablet viewports
- **REQ-UI-003**: Real-time data updates via WebSocket connections
- **REQ-UI-004**: TradingView chart integration for technical analysis
- **REQ-UI-005**: Portfolio dashboard with key metrics and performance charts
- **REQ-UI-006**: Asset screening and filtering capabilities
- **REQ-UI-007**: Trade execution interface with order validation
- **REQ-UI-008**: Dark/light theme support
- **REQ-UI-009**: Export functionality (CSV, PDF reports)

**Acceptance Criteria:**
- Page load times under 2 seconds on modern hardware
- Real-time updates with <500ms latency
- Responsive design works on screens 1024px and larger
- Accessibility compliance (WCAG 2.1 AA)

### 4.2 Advanced Features

#### 4.2.1 Asset Class Extensions
**Priority: P1 (High)**

**Requirements:**
- **REQ-AC-001**: Gold and silver asset support with weight-based metrics (grams, troy oz)
- **REQ-AC-002**: Cryptocurrency integration with major coins (BTC, ETH, etc.)
- **REQ-AC-003**: Forex pair support for currency hedging
- **REQ-AC-004**: Bond and fixed-income instrument support
- **REQ-AC-005**: Custom asset class creation with user-defined metrics

#### 4.2.2 Advanced Analytics
**Priority: P1 (High)**

**Requirements:**
- **REQ-AA-001**: Risk analytics (VaR, Expected Shortfall, Beta calculation)
- **REQ-AA-002**: Correlation analysis between assets and portfolios
- **REQ-AA-003**: Monte Carlo simulation for portfolio projections
- **REQ-AA-004**: Backtesting engine with walk-forward analysis
- **REQ-AA-005**: Sector and geographic allocation analysis

#### 4.2.3 AI Integration
**Priority: P2 (Medium)**

**Requirements:**
- **REQ-AI-001**: Local LLM integration via Ollama for investment insights
- **REQ-AI-002**: Natural language portfolio queries
- **REQ-AI-003**: Automated research report generation
- **REQ-AI-004**: Anomaly detection in market data
- **REQ-AI-005**: Sentiment analysis from news and social media

---

## 5. Non-Functional Requirements

### 5.1 Performance Requirements
- **REQ-PERF-001**: Application startup time < 10 seconds
- **REQ-PERF-002**: Data ingestion throughput: 100 symbols/minute
- **REQ-PERF-003**: Strategy calculation time: < 30 seconds for 1000 assets
- **REQ-PERF-004**: Database query response time: < 100ms for 95th percentile
- **REQ-PERF-005**: Memory usage: < 4GB during normal operation
- **REQ-PERF-006**: Disk storage: < 10GB for 5 years of daily data

### 5.2 Reliability Requirements
- **REQ-REL-001**: Application uptime: 99.9% during market hours
- **REQ-REL-002**: Data integrity: Zero data corruption incidents
- **REQ-REL-003**: Automated backup and recovery procedures
- **REQ-REL-004**: Graceful degradation during external API failures
- **REQ-REL-005**: Error logging and monitoring with structured logs

### 5.3 Security Requirements
- **REQ-SEC-001**: Local data storage with no external data transmission
- **REQ-SEC-002**: Secure API key management for data providers
- **REQ-SEC-003**: Input validation and sanitization for all user inputs
- **REQ-SEC-004**: Protection against SQL injection and XSS attacks
- **REQ-SEC-005**: Regular security dependency updates

### 5.4 Usability Requirements
- **REQ-USE-001**: Intuitive navigation with < 3 clicks to any feature
- **REQ-USE-002**: Comprehensive help documentation and tooltips
- **REQ-USE-003**: Keyboard shortcuts for power users
- **REQ-USE-004**: Consistent UI/UX patterns throughout application
- **REQ-USE-005**: Error messages with clear resolution guidance

### 5.5 Compatibility Requirements
- **REQ-COMP-001**: Cross-platform support (Windows, macOS, Linux)
- **REQ-COMP-002**: Python 3.11+ runtime requirement
- **REQ-COMP-003**: Modern browser support (Chrome 90+, Firefox 88+, Safari 14+)
- **REQ-COMP-004**: Docker containerization support
- **REQ-COMP-005**: Node.js 20+ for frontend development

---

## 6. Technical Architecture

### 6.1 Architecture Pattern
**Hexagonal Architecture (Ports & Adapters)**

### 6.2 Core Components

#### 6.2.1 Domain Layer
- **Entities**: Asset, Portfolio, Trade, Position, BrokerProfile
- **Value Objects**: Money, Percentage, AssetMetrics
- **Domain Services**: Strategy calculation, risk metrics
- **Business Rules**: Trade validation, position management

#### 6.2.2 Application Layer
- **Use Cases**: IngestData, CalculateStrategy, PlaceOrder
- **Ports**: Repository interfaces, external service contracts
- **Services**: DataIngestionService, PortfolioSimulator, StrategyScorer

#### 6.2.3 Infrastructure Layer
- **Repositories**: DuckDB implementations for data persistence
- **External Services**: Yahoo Finance API, data provider adapters
- **Schedulers**: APScheduler for automated data refresh
- **Caching**: In-memory and persistent caching layers

#### 6.2.4 UI Layer
- **Backend**: FastAPI REST API with WebSocket support
- **Frontend**: React + TypeScript SPA
- **State Management**: TanStack Query for server state
- **Styling**: Tailwind CSS with component library

### 6.3 Data Storage
- **Primary Database**: DuckDB (embedded analytical database)
- **Schema**: Star schema optimized for OLAP queries
- **Partitioning**: Time-based partitioning for historical data
- **Indexing**: B-tree indexes on symbol and timestamp columns

### 6.4 Integration Architecture
- **Plugin System**: Abstract base classes for data providers
- **Event System**: Domain events for loose coupling
- **API Gateway**: Single entry point for external integrations
- **Message Queue**: In-memory queue for background processing

---

## 7. Data Model

### 7.1 Core Entities

#### 7.1.1 Asset
```python
@dataclass(frozen=True)
class Asset:
    symbol: str              # Primary key
    exchange: str           # NYSE, NASDAQ, etc.
    asset_type: AssetType   # STOCK, ETF, CRYPTO, COMMODITY
    name: str               # Display name
```

#### 7.1.2 AssetSnapshot
```python
@dataclass(frozen=True) 
class AssetSnapshot:
    symbol: str             # Foreign key to Asset
    timestamp: datetime     # Price timestamp
    open: Decimal          # Opening price
    high: Decimal          # High price
    low: Decimal           # Low price
    close: Decimal         # Closing price
    volume: int            # Trading volume
```

#### 7.1.3 Portfolio
```python
@dataclass(frozen=True)
class Portfolio:
    portfolio_id: UUID      # Primary key
    name: str              # User-defined name
    base_ccy: str          # USD, EUR, RON
    cash_balance: Decimal  # Available cash
    created: datetime      # Creation timestamp
```

#### 7.1.4 Trade
```python
@dataclass(frozen=True)
class Trade:
    trade_id: UUID         # Primary key
    portfolio_id: UUID     # Foreign key to Portfolio
    symbol: str           # Asset symbol
    timestamp: datetime   # Execution timestamp
    side: TradeSide      # BUY, SELL
    qty: Decimal         # Quantity traded
    price: Decimal       # Execution price
    pip_pct: Decimal     # Spread percentage
    fee_flat: Decimal    # Flat fee amount
    fee_pct: Decimal     # Percentage fee
    unit: str            # share, gram, troy_oz
    price_ccy: str       # Price currency
    comment: str         # Optional comment
```

### 7.2 Database Schema
- **Assets Table**: Master asset registry
- **Snapshots Table**: Time-series OHLCV data
- **Metrics Table**: Fundamental and technical indicators
- **Portfolios Table**: Portfolio definitions
- **Trades Table**: Trade execution log
- **Positions Table**: Current position aggregates
- **Scores Table**: Strategy scoring results

---

## 8. User Interface Design

### 8.1 Navigation Structure
```
Portfolio Manager
├── Dashboard (Default)
│   ├── Portfolio Summary
│   ├── Market Overview
│   ├── Strategy Performance
│   └── Recent Activity
├── Portfolio Management
│   ├── Portfolio List
│   ├── Position Details
│   ├── Trade History
│   └── Performance Analytics
├── Asset Analysis
│   ├── Asset Screener
│   ├── Strategy Rankings
│   ├── Technical Charts
│   └── Fundamental Data
├── Data Management
│   ├── Data Ingestion
│   ├── Symbol Management
│   ├── Data Quality
│   └── System Status
└── Settings
    ├── Broker Configuration
    ├── Strategy Parameters
    ├── Display Preferences
    └── Data Sources
```

### 8.2 Key User Flows

#### 8.2.1 Portfolio Creation Flow
1. Navigate to Portfolio Management
2. Click "Create New Portfolio"
3. Enter portfolio name and base currency
4. Set initial cash balance
5. Configure broker settings
6. Save and redirect to portfolio details

#### 8.2.2 Trade Execution Flow
1. Navigate to portfolio details
2. Click "Place Trade" or search for asset
3. Select asset from search results
4. Enter trade details (side, quantity, order type)
5. Review order summary with fees
6. Confirm trade execution
7. View updated positions and cash balance

#### 8.2.3 Strategy Analysis Flow
1. Navigate to Asset Analysis
2. Select strategy from dropdown
3. Apply filters (market cap, sector, etc.)
4. Review ranked results in data table
5. Click asset to view detailed analysis
6. Add to watchlist or execute trade

### 8.3 Component Library
- **Data Tables**: Sortable, filterable asset and portfolio tables
- **Charts**: TradingView integration for price and performance charts
- **Forms**: Trade entry, portfolio creation, settings management
- **Navigation**: Responsive sidebar with collapsible sections
- **Modals**: Confirmation dialogs, trade details, error messages
- **Cards**: Portfolio summaries, metric displays, status indicators

---

## 9. API Specification

### 9.1 REST Endpoints

#### 9.1.1 Portfolio Management
```
POST /api/portfolio
GET /api/portfolio
GET /api/portfolio/{id}
PUT /api/portfolio/{id}
DELETE /api/portfolio/{id}
POST /api/portfolio/{id}/trade
GET /api/portfolio/{id}/positions
GET /api/portfolio/{id}/metrics
GET /api/portfolio/{id}/trades
```

#### 9.1.2 Asset Management
```
GET /api/asset
GET /api/asset/{symbol}
GET /api/asset/{symbol}/ohlcv
GET /api/asset/{symbol}/metrics
GET /api/asset/{symbol}/chart
POST /api/asset/ingest
```

#### 9.1.3 Strategy Engine
```
GET /api/strategy
POST /api/strategy/{id}/calculate
GET /api/strategy/{id}/scores
GET /api/strategy/{id}/backtest
```

#### 9.1.4 Data Management
```
POST /api/data/ingest
GET /api/data/status
GET /api/data/quality
DELETE /api/data/cache
```

### 9.2 WebSocket Events
```
// Real-time price updates
price_update: { symbol: string, price: number, timestamp: string }

// Portfolio value changes  
portfolio_update: { portfolio_id: string, market_value: number }

// Trade confirmations
trade_executed: { trade_id: string, status: string, details: object }

// System notifications
system_notification: { type: string, message: string, level: string }
```

### 9.3 Error Handling
- **4xx Client Errors**: Validation failures, authorization issues
- **5xx Server Errors**: Database failures, external API issues
- **Custom Error Codes**: Domain-specific error conditions
- **Error Response Format**: Consistent JSON structure with error details

---

## 10. Security & Privacy

### 10.1 Data Privacy
- **Local Storage**: All user data stored locally, no cloud transmission
- **API Keys**: Encrypted storage of external service credentials
- **Personal Data**: No collection of personally identifiable information
- **Analytics**: Optional, anonymized usage statistics only

### 10.2 Security Measures
- **Input Validation**: Server-side validation for all user inputs
- **SQL Injection**: Parameterized queries and ORM usage
- **XSS Protection**: Content Security Policy and input sanitization  
- **CSRF Protection**: Token-based request validation
- **Dependency Security**: Regular vulnerability scanning and updates

### 10.3 Access Control
- **Single User**: No multi-user authentication required
- **Local Access**: Application bound to localhost by default
- **Network Security**: Optional HTTPS for remote access
- **Session Management**: Stateless JWT tokens for API access

---

## 11. Testing Strategy

### 11.1 Test Pyramid
- **Unit Tests (70%)**: Domain logic, calculations, validations
- **Integration Tests (20%)**: Repository operations, external APIs
- **End-to-End Tests (10%)**: Complete user workflows

### 11.2 Test Coverage Requirements
- **Domain Entities**: 95% coverage minimum
- **Application Services**: 90% coverage minimum  
- **Infrastructure**: 80% coverage minimum
- **API Endpoints**: 85% coverage minimum
- **Overall Target**: 85% coverage minimum

### 11.3 Test Categories
- **Functional Tests**: Feature behavior validation
- **Performance Tests**: Load testing and benchmarking
- **Security Tests**: Vulnerability and penetration testing
- **Usability Tests**: User experience validation
- **Compatibility Tests**: Cross-platform and browser testing

### 11.4 Test Automation
- **Continuous Integration**: Automated test execution on code changes
- **Regression Testing**: Full test suite execution before releases
- **Performance Monitoring**: Automated performance regression detection
- **Security Scanning**: Dependency vulnerability monitoring

---

## 12. Deployment & Operations

### 12.1 Deployment Options

#### 12.1.1 Local Development
```bash
# Python backend
pip install -e .
portfolio-manager serve --reload

# React frontend  
cd portfolio_manager/ui/frontend
npm install && npm run dev
```

#### 12.1.2 Docker Deployment
```bash
docker-compose up -d
# Includes backend, frontend, and database
```

#### 12.1.3 Production Deployment
```bash
# Build optimized frontend
npm run build

# Start production server
portfolio-manager serve --host 0.0.0.0 --port 8000
```

### 12.2 Configuration Management
- **Environment Variables**: Runtime configuration
- **YAML Files**: Strategy parameters and data source settings
- **Database Migrations**: Automated schema updates
- **Feature Flags**: Toggle features without deployment

### 12.3 Monitoring & Observability
- **Structured Logging**: JSON logs with correlation IDs
- **Health Checks**: Application and dependency status endpoints
- **Metrics Collection**: Performance and usage metrics
- **Error Tracking**: Automated error detection and alerting

### 12.4 Backup & Recovery
- **Database Backup**: Automated daily backups of DuckDB files
- **Configuration Backup**: Settings and strategy parameter backup
- **Recovery Procedures**: Documented recovery processes
- **Data Export**: Portfolio and trade data export capabilities

---

## 13. Success Metrics

### 13.1 Key Performance Indicators (KPIs)

#### 13.1.1 User Engagement
- **Daily Active Usage**: Application launch frequency
- **Feature Adoption**: Usage of core features (portfolio, strategies, trades)
- **Session Duration**: Average time spent per session
- **User Retention**: Weekly and monthly active usage

#### 13.1.2 Technical Performance
- **System Uptime**: 99.9% availability target
- **Response Times**: <2s page loads, <100ms API responses
- **Data Accuracy**: <0.1% data ingestion error rate
- **System Reliability**: <5 crashes per month

#### 13.1.3 Business Value
- **Portfolio Performance**: Tracking user portfolio returns
- **Strategy Effectiveness**: Comparing strategy performance to benchmarks
- **User Satisfaction**: Feedback and feature request tracking
- **Cost Efficiency**: Reduced dependence on paid financial tools

### 13.2 Success Criteria
- **Phase 1**: Basic portfolio simulation and data ingestion (Q1 2025)
- **Phase 2**: Advanced strategies and analytics (Q2 2025)
- **Phase 3**: AI integration and automation (Q3 2025)
- **Phase 4**: Mobile support and advanced features (Q4 2025)

---

## 14. Risk Assessment

### 14.1 Technical Risks

#### 14.1.1 Data Provider Reliability
- **Risk**: Yahoo Finance API changes or restrictions
- **Mitigation**: Multiple data provider plugins, graceful degradation
- **Contingency**: Backup data sources (Alpha Vantage, IEX Cloud)

#### 14.1.2 Performance Scalability
- **Risk**: Slow performance with large datasets
- **Mitigation**: Database optimization, efficient algorithms
- **Contingency**: Data archiving, query optimization

#### 14.1.3 Security Vulnerabilities
- **Risk**: Security exploits in dependencies
- **Mitigation**: Regular security updates, vulnerability scanning
- **Contingency**: Incident response plan, security patches

### 14.2 Business Risks

#### 14.2.1 Market Data Costs
- **Risk**: Free data sources become paid or restricted
- **Mitigation**: Multiple free sources, rate limiting
- **Contingency**: Premium data source integration options

#### 14.2.2 Regulatory Changes
- **Risk**: Financial data regulations affect individual use
- **Mitigation**: Privacy-first design, no data sharing
- **Contingency**: Compliance documentation and disclaimers

### 14.3 User Experience Risks

#### 14.3.1 Complexity Overload
- **Risk**: Too many features confuse users
- **Mitigation**: Progressive disclosure, intuitive defaults
- **Contingency**: Simplified mode, guided tutorials

#### 14.3.2 Data Accuracy Concerns
- **Risk**: Users lose confidence due to data errors
- **Mitigation**: Multiple data validation layers, error reporting
- **Contingency**: Manual data correction tools, transparency reports

---

## 15. Future Roadmap

### 15.1 Version 1.1 (Q2 2025)
- **User-Defined Strategies**: YAML-based strategy configuration
- **Advanced Charting**: Technical indicators and drawing tools
- **Export/Import**: Portfolio data portability
- **Performance Optimization**: Caching and query improvements

### 15.2 Version 1.2 (Q3 2025)
- **AI Insights**: Local LLM integration via Ollama
- **Backtesting Engine**: Historical strategy performance testing
- **Mobile Responsive**: Tablet and mobile browser support
- **Advanced Analytics**: Risk metrics and correlation analysis

### 15.3 Version 1.3 (Q4 2025)
- **Multi-User Support**: Family/team portfolio management
- **Real-Time Data**: Streaming quotes and live updates
- **Social Features**: Strategy sharing and community insights
- **API Integration**: Third-party service connections

### 15.4 Version 2.0 (2026)
- **Cloud Deployment**: Optional cloud hosting
- **Machine Learning**: Predictive analytics and pattern recognition
- **Professional Features**: Institution-grade risk management
- **Mobile App**: Native iOS and Android applications

---

## 16. Appendices

### 16.1 Glossary
- **OHLCV**: Open, High, Low, Close, Volume price data
- **P/E Ratio**: Price-to-Earnings ratio
- **PEG Ratio**: Price/Earnings to Growth ratio
- **FCF**: Free Cash Flow
- **Pip**: Percentage in Point (smallest price move)
- **DuckDB**: In-process analytical database
- **Hexagonal Architecture**: Ports and Adapters architectural pattern

### 16.2 References
- **Technical Documentation**: `architecture.md`, `api_spec.md`
- **Test Documentation**: `test_summary.md`
- **Setup Documentation**: `README.md`, `INSTALL.md`
- **Code Standards**: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`

### 16.3 Compliance Requirements
- **Financial Disclaimers**: Investment advice disclaimers
- **Data Usage**: Third-party data usage terms
- **Open Source**: MIT license compliance
- **Privacy Policy**: Data handling and privacy practices

---

**Document History:**
- v1.0 - January 2025 - Initial PRD creation
- Future versions will track major requirement changes and feature additions

**Approval:**
- [ ] Product Owner
- [ ] Technical Lead  
- [ ] Security Review
- [ ] Legal Review

---

*This document serves as the single source of truth for the Portfolio Manager product requirements. All development work should align with the specifications outlined in this document.*