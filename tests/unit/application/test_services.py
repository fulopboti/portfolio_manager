"""Unit tests for application services."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest

from stockapp.application.ports import (
    AssetRepository,
    DataProvider,
    PortfolioRepository,
    StrategyCalculator,
)
from stockapp.application.services.data_ingestion import DataIngestionService
from stockapp.application.services.portfolio_simulator import PortfolioSimulatorService
from stockapp.application.services.strategy_scorer import StrategyScoreService
from stockapp.domain.entities import (
    Asset,
    AssetSnapshot,
    AssetType,
    BrokerProfile,
    Portfolio,
    Position,
    Trade,
    TradeSide,
)
from stockapp.domain.exceptions import (
    DataIngestionError,
    InsufficientFundsError,
    InvalidTradeError,
    StrategyCalculationError,
)


class TestDataIngestionService:
    """Test cases for DataIngestionService."""

    @pytest.fixture
    def mock_data_provider(self):
        """Mock data provider."""
        provider = Mock(spec=DataProvider)
        provider.get_ohlcv_data = AsyncMock()
        provider.get_fundamental_data = AsyncMock()
        provider.supports_symbol = Mock(return_value=True)
        return provider

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.save_asset = AsyncMock()
        repository.save_snapshot = AsyncMock()
        repository.get_asset = AsyncMock()
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def data_ingestion_service(self, mock_data_provider, mock_asset_repository):
        """Create DataIngestionService with mocked dependencies."""
        return DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=10,
            retry_attempts=3
        )

    @pytest.mark.asyncio


    async def test_ingest_symbol_success(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository, sample_asset
    ):
        """Test successful symbol ingestion."""
        # Setup mocks
        sample_snapshot = AssetSnapshot(
            symbol=sample_asset.symbol,
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        mock_data_provider.get_ohlcv_data.return_value = [sample_snapshot]
        mock_data_provider.get_fundamental_data.return_value = {
            "pe_ratio": Decimal("25.5"),
            "dividend_yield": Decimal("0.015")
        }
        mock_asset_repository.get_asset.return_value = None  # Asset doesn't exist yet

        # Execute
        result = await data_ingestion_service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc."
        )

        # Verify
        assert result.success is True
        assert result.symbol == "AAPL"
        assert result.snapshots_count == 1
        assert result.error is None

        mock_asset_repository.save_asset.assert_called_once()
        mock_asset_repository.save_snapshot.assert_called_once()

    @pytest.mark.asyncio


    async def test_ingest_symbol_provider_failure(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository
    ):
        """Test symbol ingestion with provider failure."""
        # Setup mocks
        mock_data_provider.get_ohlcv_data.side_effect = Exception("API Error")
        mock_asset_repository.get_asset.return_value = None

        # Execute
        result = await data_ingestion_service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc."
        )

        # Verify
        assert result.success is False
        assert result.symbol == "AAPL"
        assert result.snapshots_count == 0
        assert "API Error" in result.error

        mock_asset_repository.save_asset.assert_not_called()
        mock_asset_repository.save_snapshot.assert_not_called()

    @pytest.mark.asyncio


    async def test_ingest_multiple_symbols_batch(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository
    ):
        """Test batch ingestion of multiple symbols."""
        symbols = ["AAPL", "MSFT", "GOOGL"]
        
        # Setup mocks
        mock_data_provider.get_ohlcv_data.return_value = [
            AssetSnapshot(
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("99.00"),
                close=Decimal("102.00"),
                volume=1000000
            )
        ]
        mock_data_provider.get_fundamental_data.return_value = {}
        mock_asset_repository.get_asset.return_value = None

        # Execute
        results = await data_ingestion_service.ingest_multiple_symbols(
            symbols=symbols,
            asset_type=AssetType.STOCK,
            exchange="NASDAQ"
        )

        # Verify
        assert len(results) == 3
        assert all(result.success for result in results)
        assert mock_asset_repository.save_asset.call_count == 3
        assert mock_asset_repository.save_snapshot.call_count == 3

    @pytest.mark.asyncio


    async def test_ingest_with_validation_error(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository
    ):
        """Test ingestion with data validation error."""
        # Setup mocks to return valid snapshot but throw error during save
        valid_snapshot = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("95.00"), 
            close=Decimal("98.00"),
            volume=1000000
        )
        
        mock_data_provider.get_ohlcv_data.return_value = [valid_snapshot]
        mock_asset_repository.get_asset.return_value = None
        # Make save_snapshot raise a validation error
        mock_asset_repository.save_snapshot.side_effect = Exception("Snapshot validation failed")

        # Execute
        result = await data_ingestion_service.ingest_symbol(
            symbol="TEST",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Test"
        )

        # Verify
        assert result.success is False
        assert "validation" in result.error.lower()


class TestPortfolioSimulatorService:
    """Test cases for PortfolioSimulatorService."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Mock portfolio repository."""
        repository = Mock(spec=PortfolioRepository)
        repository.get_portfolio = AsyncMock()
        repository.save_portfolio = AsyncMock()
        repository.save_trade = AsyncMock()
        repository.get_position = AsyncMock()
        repository.save_position = AsyncMock()
        repository.delete_position = AsyncMock()
        return repository

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def portfolio_simulator(self, mock_portfolio_repository, mock_asset_repository):
        """Create PortfolioSimulatorService with mocked dependencies."""
        return PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repository,
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio


    async def test_execute_buy_trade_success(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile
    ):
        """Test successful buy trade execution."""
        # Setup mocks
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = None  # No existing position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        # Execute
        trade_result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),
            broker_profile=sample_broker_profile,
            comment="Test buy trade"
        )

        # Verify
        assert trade_result.success is True
        assert trade_result.trade.side == TradeSide.BUY
        assert trade_result.trade.qty == Decimal("100")
        assert trade_result.error is None

        mock_portfolio_repository.save_trade.assert_called_once()
        mock_portfolio_repository.save_position.assert_called_once()
        mock_portfolio_repository.save_portfolio.assert_called_once()

    @pytest.mark.asyncio


    async def test_execute_buy_trade_insufficient_funds(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile
    ):
        """Test buy trade with insufficient funds."""
        # Setup portfolio with low cash balance
        low_cash_portfolio = Portfolio(
            portfolio_id=sample_portfolio.portfolio_id,
            name=sample_portfolio.name,
            base_ccy=sample_portfolio.base_ccy,
            cash_balance=Decimal("1000.00"),  # Low balance
            created=sample_portfolio.created
        )
        
        mock_portfolio_repository.get_portfolio.return_value = low_cash_portfolio
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        # Execute
        trade_result = await portfolio_simulator.execute_trade(
            portfolio_id=low_cash_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),  # $15,000+ required
            broker_profile=sample_broker_profile,
            comment="Test buy trade"
        )

        # Verify
        assert trade_result.success is False
        assert isinstance(trade_result.error, InsufficientFundsError)

        mock_portfolio_repository.save_trade.assert_not_called()
        mock_portfolio_repository.save_position.assert_not_called()
        mock_portfolio_repository.save_portfolio.assert_not_called()

    @pytest.mark.asyncio


    async def test_execute_sell_trade_success(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile, sample_position
    ):
        """Test successful sell trade execution."""
        # Setup mocks
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = sample_position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("160.00"),
            high=Decimal("162.00"),
            low=Decimal("158.00"),
            close=Decimal("161.00"),
            volume=45000000
        )

        # Execute partial sell
        trade_result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("50"),  # Sell 50 out of 100 shares
            broker_profile=sample_broker_profile,
            comment="Test sell trade"
        )

        # Verify
        assert trade_result.success is True
        assert trade_result.trade.side == TradeSide.SELL
        assert trade_result.trade.qty == Decimal("50")
        assert trade_result.error is None

        mock_portfolio_repository.save_trade.assert_called_once()
        mock_portfolio_repository.save_position.assert_called_once()  # Updated position
        mock_portfolio_repository.save_portfolio.assert_called_once()

    @pytest.mark.asyncio


    async def test_execute_sell_trade_insufficient_position(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile, sample_position
    ):
        """Test sell trade with insufficient position."""
        # Setup mocks
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = sample_position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("160.00"),
            high=Decimal("162.00"),
            low=Decimal("158.00"),
            close=Decimal("161.00"),
            volume=45000000
        )

        # Execute oversell
        trade_result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("150"),  # Sell 150 but only have 100
            broker_profile=sample_broker_profile,
            comment="Test oversell"
        )

        # Verify
        assert trade_result.success is False
        assert "insufficient position" in trade_result.error.lower()

        mock_portfolio_repository.save_trade.assert_not_called()

    @pytest.mark.asyncio


    async def test_calculate_portfolio_metrics(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio
    ):
        """Test portfolio metrics calculation."""
        # Setup positions
        positions = [
            Position(
                portfolio_id=sample_portfolio.portfolio_id,
                symbol="AAPL",
                qty=Decimal("100"),
                avg_cost=Decimal("150.00"),
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            ),
            Position(
                portfolio_id=sample_portfolio.portfolio_id,
                symbol="MSFT",
                qty=Decimal("50"),
                avg_cost=Decimal("300.00"),
                unit="share",
                price_ccy="USD",
                last_updated=datetime.now(timezone.utc)
            )
        ]
        
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_positions_for_portfolio.return_value = positions
        mock_asset_repository.get_latest_snapshot.side_effect = [
            AssetSnapshot(
                symbol="AAPL",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("160.00"),
                high=Decimal("165.00"),
                low=Decimal("158.00"),
                close=Decimal("162.00"),  # +$12 per share
                volume=50000000
            ),
            AssetSnapshot(
                symbol="MSFT",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("320.00"),
                high=Decimal("325.00"),
                low=Decimal("305.00"),  # Fixed: low <= close
                close=Decimal("310.00"),  # -$10 per share
                volume=30000000
            )
        ]

        # Execute
        metrics = await portfolio_simulator.calculate_portfolio_metrics(
            sample_portfolio.portfolio_id
        )

        # Verify
        assert metrics.total_market_value == Decimal("31700.00")  # (100*162 + 50*310)
        assert metrics.total_cost_basis == Decimal("30000.00")    # (100*150 + 50*300)
        assert metrics.total_unrealized_pnl == Decimal("1700.00") # (100*12 + 50*-10)
        assert metrics.cash_balance == sample_portfolio.cash_balance


class TestPortfolioSimulatorExceptionCoverage:
    """Tests to cover exception handling and edge cases in PortfolioSimulatorService."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Mock portfolio repository that can throw exceptions."""
        repository = Mock(spec=PortfolioRepository)
        repository.get_portfolio = AsyncMock()
        repository.save_portfolio = AsyncMock()
        repository.save_trade = AsyncMock()
        repository.get_position = AsyncMock()
        repository.save_position = AsyncMock()
        repository.delete_position = AsyncMock()
        repository.get_positions_for_portfolio = AsyncMock()
        return repository

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository that can throw exceptions."""
        repository = Mock(spec=AssetRepository)
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def portfolio_simulator(self, mock_portfolio_repository, mock_asset_repository):
        """Create PortfolioSimulatorService with mocked dependencies."""
        return PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repository,
            asset_repository=mock_asset_repository
        )

    @pytest.fixture
    def sample_portfolio(self):
        """Create a sample portfolio."""
        return Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

    @pytest.fixture
    def sample_broker_profile(self):
        """Create a sample broker profile."""
        return BrokerProfile(
            broker_id="TEST_BROKER",
            name="Test Broker",
            pip_pct=Decimal("0.0001"),
            fee_flat=Decimal("1.00"),
            fee_pct=Decimal("0.001"),
            min_order_value=Decimal("1.00"),
            supported_currencies=["USD"],
            supports_fractional=True
        )

    @pytest.mark.asyncio
    async def test_execute_buy_trade_with_existing_position(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile
    ):
        """Test buy trade when position already exists (covers lines 139-141)."""
        # Setup existing position
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("50"),
            avg_cost=Decimal("140.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = existing_position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        # Execute buy trade
        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("25"),
            broker_profile=sample_broker_profile,
            comment="Add to existing position"
        )

        # Verify success
        assert result.success is True
        assert result.trade is not None
        
        # Verify existing position was updated (lines 139-141)
        mock_portfolio_repository.save_position.assert_called_once_with(existing_position)
        
        # Verify position quantities were updated via add_shares
        assert existing_position.qty == Decimal("75")  # 50 + 25

    @pytest.mark.asyncio
    async def test_execute_buy_trade_exception_handling(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile
    ):
        """Test exception handling in _execute_buy_trade (covers lines 160-161)."""
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = None
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        # Make save_position throw an exception
        mock_portfolio_repository.save_position.side_effect = RuntimeError("Database error")

        # Execute buy trade
        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10"),
            broker_profile=sample_broker_profile
        )

        # Verify exception handling (lines 160-161)
        assert result.success is False
        assert isinstance(result.error, RuntimeError)
        assert str(result.error) == "Database error"

    @pytest.mark.asyncio
    async def test_execute_sell_trade_exception_handling(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository,
        sample_portfolio, sample_broker_profile
    ):
        """Test exception handling in _execute_sell_trade (covers lines 208-209)."""
        existing_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )
        
        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = existing_position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        # Make save_position throw an exception
        mock_portfolio_repository.save_position.side_effect = RuntimeError("Database error")

        # Execute sell trade
        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("50"),
            broker_profile=sample_broker_profile
        )

        # Verify exception handling (lines 208-209)
        assert result.success is False
        assert isinstance(result.error, RuntimeError)
        assert str(result.error) == "Database error"


class TestStrategyScoreService:
    """Test cases for StrategyScoreService."""

    @pytest.fixture
    def mock_strategy_calculator(self):
        """Mock strategy calculator."""
        calculator = Mock(spec=StrategyCalculator)
        calculator.calculate_score = Mock()
        calculator.get_strategy_name = Mock(return_value="Value Strategy")
        return calculator

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.get_all_assets = AsyncMock()
        repository.get_fundamental_metrics = AsyncMock()
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def strategy_score_service(self, mock_strategy_calculator, mock_asset_repository):
        """Create StrategyScoreService with mocked dependencies."""
        return StrategyScoreService(
            strategy_calculators={"VALUE": mock_strategy_calculator},
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio


    async def test_calculate_strategy_scores_success(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository,
        multiple_assets
    ):
        """Test successful strategy score calculation."""
        # Setup mocks
        mock_asset_repository.get_all_assets.return_value = multiple_assets
        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("15.5"),
            "dividend_yield": Decimal("0.025"),
            "debt_equity": Decimal("0.3"),
            "fcf_growth": Decimal("0.12")
        }
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        mock_strategy_calculator.calculate_score.return_value = Decimal("85.5")

        # Execute
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            symbols=["AAPL", "MSFT"],
            as_of_date=datetime.now(timezone.utc)
        )

        # Verify
        assert len(results) == 2
        assert all(result.score == Decimal("85.5") for result in results)
        assert all(result.strategy_id == "VALUE" for result in results)
        assert mock_strategy_calculator.calculate_score.call_count == 2

    @pytest.mark.asyncio


    async def test_calculate_strategy_scores_missing_data(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository,
        multiple_assets
    ):
        """Test strategy calculation with missing fundamental data."""
        # Setup mocks
        mock_asset_repository.get_all_assets.return_value = multiple_assets
        mock_asset_repository.get_fundamental_metrics.return_value = None  # Missing data
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        # Execute
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            symbols=["AAPL"],
            as_of_date=datetime.now(timezone.utc)
        )

        # Verify - should skip assets with missing data
        assert len(results) == 0
        mock_strategy_calculator.calculate_score.assert_not_called()

    @pytest.mark.asyncio


    async def test_calculate_strategy_scores_invalid_strategy(
        self, strategy_score_service, mock_asset_repository
    ):
        """Test strategy calculation with invalid strategy ID."""
        with pytest.raises(StrategyCalculationError, match="Unknown strategy"):
            await strategy_score_service.calculate_strategy_scores(
                strategy_id="INVALID",
                symbols=["AAPL"],
                as_of_date=datetime.now(timezone.utc)
            )

    @pytest.mark.asyncio


    async def test_get_top_ranked_assets(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository,
        multiple_assets
    ):
        """Test getting top-ranked assets."""
        # Setup mocks with different scores
        mock_asset_repository.get_all_assets.return_value = multiple_assets
        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("15.5"),
            "dividend_yield": Decimal("0.025")
        }
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("99.00"),
            close=Decimal("102.00"),
            volume=1000000
        )
        
        # Return different scores for different symbols
        scores = [Decimal("95.0"), Decimal("85.0"), Decimal("75.0"), Decimal("65.0")]
        mock_strategy_calculator.calculate_score.side_effect = scores

        # Execute
        results = await strategy_score_service.get_top_ranked_assets(
            strategy_id="VALUE",
            limit=2,
            as_of_date=datetime.now(timezone.utc)
        )

        # Verify - should return top 2 scores in descending order
        assert len(results) == 2
        assert results[0].score == Decimal("95.0")
        assert results[1].score == Decimal("85.0")

    @pytest.mark.asyncio


    async def test_backtest_strategy_performance(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository,
        multiple_assets
    ):
        """Test strategy backtesting performance."""
        # Setup mocks
        mock_asset_repository.get_all_assets.return_value = multiple_assets[:1]  # Single asset
        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("15.5"),
            "dividend_yield": Decimal("0.025")
        }
        mock_asset_repository.get_historical_snapshots.return_value = [
            AssetSnapshot(
                symbol="AAPL",
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                open=Decimal("180.00"),
                high=Decimal("185.00"),
                low=Decimal("178.00"),
                close=Decimal("182.00"),
                volume=50000000
            ),
            AssetSnapshot(
                symbol="AAPL",
                timestamp=datetime(2024, 1, 31, tzinfo=timezone.utc),
                open=Decimal("182.00"),
                high=Decimal("190.00"),
                low=Decimal("180.00"),
                close=Decimal("188.00"),
                volume=55000000
            )
        ]
        mock_strategy_calculator.calculate_score.return_value = Decimal("90.0")

        # Execute
        backtest_result = await strategy_score_service.backtest_strategy(
            strategy_id="VALUE",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
            initial_capital=Decimal("100000.00")
        )

        # Verify
        assert backtest_result.strategy_id == "VALUE"
        assert backtest_result.initial_capital == Decimal("100000.00")
        assert backtest_result.final_value > Decimal("0")
        assert backtest_result.total_return != Decimal("0")
        assert len(backtest_result.trades) >= 0


# Additional test cases for improved coverage
class TestDataIngestionServiceAdditionalCoverage:
    """Additional tests for DataIngestionService to improve coverage."""

    @pytest.fixture
    def mock_data_provider(self):
        """Mock data provider."""
        provider = Mock(spec=DataProvider)
        provider.get_ohlcv_data = AsyncMock()
        provider.get_fundamental_data = AsyncMock()
        provider.supports_symbol = Mock(return_value=True)
        return provider

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.save_asset = AsyncMock()
        repository.save_snapshot = AsyncMock()
        repository.get_asset = AsyncMock()
        repository.get_latest_snapshot = AsyncMock()
        repository.get_all_assets = AsyncMock()
        repository.save_fundamental_metrics = AsyncMock()
        return repository

    @pytest.fixture
    def data_ingestion_service(self, mock_data_provider, mock_asset_repository):
        """Create DataIngestionService with mocked dependencies."""
        return DataIngestionService(
            data_provider=mock_data_provider,
            asset_repository=mock_asset_repository,
            batch_size=10,
            retry_attempts=3
        )

    @pytest.mark.asyncio
    async def test_ingest_symbol_fundamentals_exception(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository
    ):
        """Test symbol ingestion when fundamentals fetching fails."""
        # Setup mocks
        sample_snapshot = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        mock_data_provider.get_ohlcv_data.return_value = [sample_snapshot]
        mock_data_provider.get_fundamental_data.side_effect = Exception("Fundamentals API Error")
        mock_asset_repository.get_asset.return_value = None

        # Execute
        result = await data_ingestion_service.ingest_symbol(
            symbol="AAPL",
            asset_type=AssetType.STOCK,
            exchange="NASDAQ",
            name="Apple Inc."
        )

        # Verify - should still succeed even if fundamentals fail
        assert result.success is True
        assert result.snapshots_count == 1
        mock_asset_repository.save_fundamental_metrics.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_all_assets(
        self, data_ingestion_service, mock_data_provider, mock_asset_repository
    ):
        """Test refreshing all assets."""
        # Setup existing assets
        existing_assets = [
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple"),
            Asset(symbol="MSFT", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Microsoft"),
        ]
        
        mock_asset_repository.get_all_assets.return_value = existing_assets
        mock_data_provider.get_ohlcv_data.return_value = [
            AssetSnapshot(
                symbol="TEST",
                timestamp=datetime.now(timezone.utc),
                open=Decimal("100.00"),
                high=Decimal("105.00"),
                low=Decimal("95.00"),
                close=Decimal("102.00"),
                volume=1000000
            )
        ]
        mock_data_provider.get_fundamental_data.return_value = {}

        # Execute
        results = await data_ingestion_service.refresh_all_assets()

        # Verify
        assert len(results) == 2
        assert all(result.success for result in results)


class TestPortfolioSimulatorServiceAdditionalCoverage:
    """Additional tests for PortfolioSimulatorService to improve coverage."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Mock portfolio repository."""
        repository = Mock(spec=PortfolioRepository)
        repository.get_portfolio = AsyncMock()
        repository.save_portfolio = AsyncMock()
        repository.save_trade = AsyncMock()
        repository.get_position = AsyncMock()
        repository.save_position = AsyncMock()
        repository.delete_position = AsyncMock()
        repository.get_positions_for_portfolio = AsyncMock()
        return repository

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def portfolio_simulator(self, mock_portfolio_repository, mock_asset_repository):
        """Create PortfolioSimulatorService with mocked dependencies."""
        return PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repository,
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio
    async def test_execute_trade_portfolio_not_found(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test trade execution when portfolio is not found."""
        mock_portfolio_repository.get_portfolio.return_value = None

        result = await portfolio_simulator.execute_trade(
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            ),
            comment="Test"
        )

        assert result.success is False
        assert "not found" in str(result.error)

    @pytest.mark.asyncio
    async def test_execute_trade_no_market_data(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test trade execution when no market data is available."""
        sample_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_asset_repository.get_latest_snapshot.return_value = None  # No market data

        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            ),
            comment="Test"
        )

        assert result.success is False
        assert "No market data available" in str(result.error)

    @pytest.mark.asyncio
    async def test_execute_trade_broker_cannot_execute(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test trade execution when broker cannot execute the order."""
        sample_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        # Broker that doesn't support fractional shares
        restrictive_broker = BrokerProfile(
            broker_id="RESTRICTIVE",
            name="Restrictive Broker",
            pip_pct=Decimal("0.001"),
            fee_flat=Decimal("0.00"),
            fee_pct=Decimal("0.000"),
            min_order_value=Decimal("1.00"),
            supported_currencies=["USD"],
            supports_fractional=False
        )

        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("10.5"),  # Fractional quantity
            broker_profile=restrictive_broker,
            comment="Test"
        )

        assert result.success is False
        assert "cannot execute" in str(result.error)

    @pytest.mark.asyncio
    async def test_execute_sell_trade_no_position(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test sell trade when no position exists."""
        sample_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = None  # No position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("100"),
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            ),
            comment="Test"
        )

        assert result.success is False
        assert "No position found" in str(result.error)

    @pytest.mark.asyncio
    async def test_execute_sell_trade_complete_position_closure(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test sell trade that completely closes a position."""
        sample_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("10000.00"),
            created=datetime.now(timezone.utc)
        )

        sample_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_position.return_value = sample_position
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("160.00"),
            high=Decimal("162.00"),
            low=Decimal("158.00"),
            close=Decimal("161.00"),
            volume=45000000
        )

        # Sell entire position
        result = await portfolio_simulator.execute_trade(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            side=TradeSide.SELL,
            quantity=Decimal("100"),  # Sell entire position
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            ),
            comment="Close position"
        )

        assert result.success is True
        # Should delete position when quantity becomes zero
        mock_portfolio_repository.delete_position.assert_called_once_with(
            sample_portfolio.portfolio_id, "AAPL"
        )

    @pytest.mark.asyncio
    async def test_calculate_portfolio_metrics_portfolio_not_found(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test portfolio metrics calculation when portfolio is not found."""
        mock_portfolio_repository.get_portfolio.return_value = None

        with pytest.raises(InvalidTradeError, match="not found"):
            await portfolio_simulator.calculate_portfolio_metrics(uuid4())

    @pytest.mark.asyncio
    async def test_calculate_portfolio_metrics_no_market_data(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test portfolio metrics when no market data is available."""
        sample_portfolio = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("5000.00"),
            created=datetime.now(timezone.utc)
        )

        sample_position = Position(
            portfolio_id=sample_portfolio.portfolio_id,
            symbol="AAPL",
            qty=Decimal("100"),
            avg_cost=Decimal("150.00"),
            unit="share",
            price_ccy="USD",
            last_updated=datetime.now(timezone.utc)
        )

        mock_portfolio_repository.get_portfolio.return_value = sample_portfolio
        mock_portfolio_repository.get_positions_for_portfolio.return_value = [sample_position]
        mock_asset_repository.get_latest_snapshot.return_value = None  # No market data

        metrics = await portfolio_simulator.calculate_portfolio_metrics(
            sample_portfolio.portfolio_id
        )

        # Should handle missing market data gracefully
        assert metrics.cash_balance == sample_portfolio.cash_balance
        assert metrics.total_cost_basis == Decimal("15000.00")  # 100 * 150
        assert metrics.number_of_positions == 1


class TestStrategyScoreServiceAdditionalCoverage:
    """Additional tests for StrategyScoreService to improve coverage."""

    @pytest.fixture
    def mock_strategy_calculator(self):
        """Mock strategy calculator."""
        calculator = Mock(spec=StrategyCalculator)
        calculator.calculate_score = Mock()
        calculator.get_strategy_name = Mock(return_value="Value Investor Strategy")
        calculator.get_strategy_description = Mock(return_value="A value-based investment strategy")
        calculator.get_required_metrics = Mock(return_value=["pe_ratio", "dividend_yield"])
        calculator.validate_metrics = Mock(return_value=True)
        return calculator

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.get_all_assets = AsyncMock()
        repository.get_asset = AsyncMock()
        repository.get_fundamental_metrics = AsyncMock()
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def strategy_score_service(self, mock_strategy_calculator, mock_asset_repository):
        """Create StrategyScoreService with mocked dependencies."""
        return StrategyScoreService(
            strategy_calculators={"VALUE": mock_strategy_calculator},
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio
    async def test_calculate_strategy_scores_with_specific_symbols(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test strategy calculation with specific symbols where some don't exist."""
        # Mock repository to return only one asset
        mock_asset_repository.get_asset.side_effect = lambda symbol: (
            Asset(symbol=symbol, exchange="NASDAQ", asset_type=AssetType.STOCK, name=symbol)
            if symbol == "AAPL" else None
        )

        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="AAPL",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )

        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("25.5"),
            "dividend_yield": Decimal("0.015")
        }

        mock_strategy_calculator.calculate_score.return_value = Decimal("85.0")

        # Test with symbols where only one exists
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            symbols=["AAPL", "NONEXISTENT"],  # Only AAPL exists
            as_of_date=datetime.now(timezone.utc)
        )

        # Should only return results for existing assets
        assert len(results) == 1
        assert results[0].symbol == "AAPL"

    def test_get_strategy_info(self, strategy_score_service, mock_strategy_calculator):
        """Test getting strategy information."""
        info = strategy_score_service.get_strategy_info("VALUE")
        
        assert info["id"] == "VALUE"
        assert info["name"] == "Value Investor Strategy"
        assert info["description"] == "A value-based investment strategy"
        assert "pe_ratio" in info["required_metrics"]

    def test_list_available_strategies(self, strategy_score_service):
        """Test listing all available strategies."""
        strategies = strategy_score_service.list_available_strategies()
        
        assert len(strategies) == 1
        assert strategies[0]["id"] == "VALUE"


# Tests for uncovered exception handling and edge cases
class TestPortfolioSimulatorServiceExceptionCoverage:
    """Tests to cover exception handling paths in PortfolioSimulatorService."""

    @pytest.fixture
    def mock_portfolio_repository(self):
        """Mock portfolio repository that can throw exceptions."""
        repository = Mock(spec=PortfolioRepository)
        repository.get_portfolio = AsyncMock()
        repository.save_portfolio = AsyncMock()
        repository.save_trade = AsyncMock()
        repository.get_position = AsyncMock()
        repository.save_position = AsyncMock()
        repository.delete_position = AsyncMock()
        repository.get_positions_for_portfolio = AsyncMock()
        return repository

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository."""
        repository = Mock(spec=AssetRepository)
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def portfolio_simulator(self, mock_portfolio_repository, mock_asset_repository):
        """Create PortfolioSimulatorService with mocked dependencies."""
        return PortfolioSimulatorService(
            portfolio_repository=mock_portfolio_repository,
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio
    async def test_execute_trade_unexpected_exception(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test execute_trade when an unexpected exception occurs."""
        # Make get_portfolio raise an unexpected exception
        mock_portfolio_repository.get_portfolio.side_effect = RuntimeError("Unexpected database error")

        result = await portfolio_simulator.execute_trade(
            portfolio_id=uuid4(),
            symbol="AAPL",
            side=TradeSide.BUY,
            quantity=Decimal("100"),
            broker_profile=BrokerProfile(
                broker_id="TEST",
                name="Test Broker",
                pip_pct=Decimal("0.001"),
                fee_flat=Decimal("0.00"),
                fee_pct=Decimal("0.000"),
                min_order_value=Decimal("1.00"),
                supported_currencies=["USD"],
                supports_fractional=True
            ),
            comment="Test"
        )

        assert result.success is False
        assert isinstance(result.error, RuntimeError)
        assert "Unexpected database error" in str(result.error)


    @pytest.mark.asyncio
    async def test_calculate_portfolio_metrics_repository_exception(
        self, portfolio_simulator, mock_portfolio_repository, mock_asset_repository
    ):
        """Test portfolio metrics calculation when repository operations fail."""
        # Make get_positions_for_portfolio fail
        mock_portfolio_repository.get_portfolio.return_value = Portfolio(
            portfolio_id=uuid4(),
            name="Test Portfolio",
            base_ccy="USD",
            cash_balance=Decimal("5000.00"),
            created=datetime.now(timezone.utc)
        )
        mock_portfolio_repository.get_positions_for_portfolio.side_effect = Exception("Repository error")

        with pytest.raises(Exception, match="Repository error"):
            await portfolio_simulator.calculate_portfolio_metrics(uuid4())


class TestStrategyScoreServiceExceptionCoverage:
    """Tests to cover exception handling and edge cases in StrategyScoreService."""

    @pytest.fixture
    def mock_strategy_calculator(self):
        """Mock strategy calculator that can throw exceptions."""
        calculator = Mock(spec=StrategyCalculator)
        calculator.calculate_score = Mock()
        calculator.get_strategy_name = Mock(return_value="Value Investor Strategy")
        calculator.get_strategy_description = Mock(return_value="A value-based investment strategy")
        calculator.get_required_metrics = Mock(return_value=["pe_ratio", "dividend_yield"])
        calculator.validate_metrics = Mock(return_value=True)
        return calculator

    @pytest.fixture
    def mock_asset_repository(self):
        """Mock asset repository that can throw exceptions."""
        repository = Mock(spec=AssetRepository)
        repository.get_all_assets = AsyncMock()
        repository.get_asset = AsyncMock()
        repository.get_fundamental_metrics = AsyncMock()
        repository.get_latest_snapshot = AsyncMock()
        return repository

    @pytest.fixture
    def strategy_score_service(self, mock_strategy_calculator, mock_asset_repository):
        """Create StrategyScoreService with mocked dependencies."""
        return StrategyScoreService(
            strategy_calculators={"VALUE": mock_strategy_calculator},
            asset_repository=mock_asset_repository
        )

    @pytest.mark.asyncio
    async def test_calculate_strategy_scores_missing_snapshot(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test strategy calculation when snapshot is missing."""
        mock_asset_repository.get_all_assets.return_value = [
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple"),
            Asset(symbol="MISSING", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Missing Data")
        ]
        
        # First asset has snapshot, second doesn't
        def snapshot_side_effect(symbol):
            if symbol == "AAPL":
                return AssetSnapshot(
                    symbol="AAPL",
                    timestamp=datetime.now(timezone.utc),
                    open=Decimal("150.00"),
                    high=Decimal("155.00"),
                    low=Decimal("149.50"),
                    close=Decimal("152.75"),
                    volume=50000000
                )
            return None  # Missing snapshot for other symbols
        
        mock_asset_repository.get_latest_snapshot.side_effect = snapshot_side_effect
        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("25.5"),
            "dividend_yield": Decimal("0.015")
        }
        mock_strategy_calculator.calculate_score.return_value = Decimal("85.0")

        # Should only return results for assets with snapshots
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            as_of_date=datetime.now(timezone.utc)
        )

        assert len(results) == 1
        assert results[0].symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_calculate_strategy_scores_invalid_fundamentals(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test strategy calculation when fundamentals are invalid."""
        mock_asset_repository.get_all_assets.return_value = [
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple"),
            Asset(symbol="INVALID", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Invalid Data")
        ]
        
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        # First asset has valid fundamentals, second doesn't
        def fundamentals_side_effect(symbol):
            if symbol == "AAPL":
                return {"pe_ratio": Decimal("25.5"), "dividend_yield": Decimal("0.015")}
            return None  # Missing fundamentals for other symbols
        
        def validate_side_effect(fundamentals):
            if fundamentals is None:
                return False
            return True
        
        mock_asset_repository.get_fundamental_metrics.side_effect = fundamentals_side_effect
        mock_strategy_calculator.validate_metrics.side_effect = validate_side_effect
        mock_strategy_calculator.calculate_score.return_value = Decimal("85.0")

        # Should only return results for assets with valid fundamentals
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            as_of_date=datetime.now(timezone.utc)
        )

        assert len(results) == 1
        assert results[0].symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_calculate_strategy_scores_calculation_exception(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test strategy calculation when score calculation throws exception (covers lines 95-97)."""
        mock_asset_repository.get_all_assets.return_value = [
            Asset(symbol="AAPL", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Apple"),
            Asset(symbol="ERROR", exchange="NASDAQ", asset_type=AssetType.STOCK, name="Error Asset")
        ]
        
        mock_asset_repository.get_latest_snapshot.return_value = AssetSnapshot(
            symbol="TEST",
            timestamp=datetime.now(timezone.utc),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.50"),
            close=Decimal("152.75"),
            volume=50000000
        )
        
        mock_asset_repository.get_fundamental_metrics.return_value = {
            "pe_ratio": Decimal("25.5"),
            "dividend_yield": Decimal("0.015")
        }
        
        # First asset calculates successfully, second throws exception
        def score_side_effect(asset, snapshot, fundamentals):
            if asset.symbol == "AAPL":
                return Decimal("85.0")
            raise RuntimeError("Calculation failed")
        
        mock_strategy_calculator.calculate_score.side_effect = score_side_effect

        # Should only return results for assets that don't throw exceptions
        results = await strategy_score_service.calculate_strategy_scores(
            strategy_id="VALUE",
            as_of_date=datetime.now(timezone.utc)
        )

        assert len(results) == 1
        assert results[0].symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_backtest_strategy_unknown_strategy(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test backtest_strategy with unknown strategy (covers line 126)."""
        with pytest.raises(StrategyCalculationError, match="Unknown strategy: UNKNOWN"):
            await strategy_score_service.backtest_strategy(
                strategy_id="UNKNOWN",
                start_date=datetime(2023, 1, 1),
                end_date=datetime(2023, 12, 31),
                initial_capital=Decimal("10000.00")
            )

    def test_get_strategy_info_unknown_strategy(
        self, strategy_score_service, mock_strategy_calculator, mock_asset_repository
    ):
        """Test get_strategy_info with unknown strategy (covers line 163)."""
        with pytest.raises(StrategyCalculationError, match="Unknown strategy: UNKNOWN"):
            strategy_score_service.get_strategy_info("UNKNOWN")

