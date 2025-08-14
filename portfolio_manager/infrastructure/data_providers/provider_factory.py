"""Factory for creating data providers based on configuration."""

import logging

from portfolio_manager.application.ports import DataProvider
from portfolio_manager.config.schema import PortfolioManagerConfig

from .mock_provider import MockDataProvider
from .yfinance_provider import YFinanceProvider


class DataProviderFactory:
    """Factory for creating data provider instances."""

    def __init__(self, config: PortfolioManagerConfig):
        """Initialize factory with configuration."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._providers: dict[str, DataProvider] = {}

    def get_provider(self, provider_name: str | None = None) -> DataProvider:
        """Get a data provider instance."""
        if provider_name is None:
            provider_name = self.config.data_providers.market_data.primary

        # Return cached provider if available
        if provider_name in self._providers:
            return self._providers[provider_name]

        # Create new provider instance
        provider = self._create_provider(provider_name)
        self._providers[provider_name] = provider

        self.logger.info(f"Created data provider: {provider_name}")
        return provider

    def get_primary_provider(self) -> DataProvider:
        """Get the primary data provider."""
        return self.get_provider(self.config.data_providers.market_data.primary)

    def get_fallback_providers(self) -> list[DataProvider]:
        """Get fallback data providers."""
        fallback_names = self.config.data_providers.market_data.fallback
        return [self.get_provider(name) for name in fallback_names]

    def _create_provider(self, provider_name: str) -> DataProvider:
        """Create a specific provider instance."""
        provider_creators = {
            'yfinance': self._create_yfinance_provider,
            'yahoo_finance': self._create_yfinance_provider,  # Alias
            'mock': self._create_mock_provider,
            'test': self._create_mock_provider,  # Alias for testing
        }

        creator = provider_creators.get(provider_name.lower())
        if creator is None:
            available_providers = list(provider_creators.keys())
            raise ValueError(
                f"Unknown data provider '{provider_name}'. "
                f"Available providers: {available_providers}"
            )

        return creator()

    def _create_yfinance_provider(self) -> YFinanceProvider:
        """Create Yahoo Finance provider instance."""
        yf_config = self.config.data_providers.market_data.yfinance

        return YFinanceProvider(
            request_delay=yf_config.request_delay,
            max_retries=yf_config.max_retries
        )

    def _create_mock_provider(self) -> MockDataProvider:
        """Create mock provider instance for testing."""
        return MockDataProvider()

    def list_available_providers(self) -> list[str]:
        """List all available provider names."""
        return ['yfinance', 'yahoo_finance', 'mock', 'test']

    def clear_cache(self) -> None:
        """Clear cached provider instances."""
        self._providers.clear()
        self.logger.info("Cleared data provider cache")


def create_data_provider_factory(config: PortfolioManagerConfig) -> DataProviderFactory:
    """Create and return a DataProviderFactory instance."""
    return DataProviderFactory(config)
