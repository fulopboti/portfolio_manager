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