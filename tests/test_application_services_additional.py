"""Additional unit tests for application services to improve coverage."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from stockapp.application.ports import AssetRepository, DataProvider, PortfolioRepository
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
from stockapp.domain.exceptions import InvalidTradeError


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
        from stockapp.application.ports import StrategyCalculator
        calculator = Mock(spec=StrategyCalculator)
        calculator.calculate_score = Mock()
        calculator.get_strategy_name = Mock(return_value="Test Strategy")
        calculator.get_strategy_description = Mock(return_value="A test strategy")
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
            strategy_calculators={"TEST": mock_strategy_calculator},
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
            strategy_id="TEST",
            symbols=["AAPL", "NONEXISTENT"],  # Only AAPL exists
            as_of_date=datetime.now(timezone.utc)
        )

        # Should only return results for existing assets
        assert len(results) == 1
        assert results[0].symbol == "AAPL"

    def test_get_strategy_info(self, strategy_score_service, mock_strategy_calculator):
        """Test getting strategy information."""
        info = strategy_score_service.get_strategy_info("TEST")
        
        assert info["id"] == "TEST"
        assert info["name"] == "Test Strategy"
        assert info["description"] == "A test strategy"
        assert info["required_metrics"] == ["pe_ratio", "dividend_yield"]

    def test_list_available_strategies(self, strategy_score_service):
        """Test listing all available strategies."""
        strategies = strategy_score_service.list_available_strategies()
        
        assert len(strategies) == 1
        assert strategies[0]["id"] == "TEST"