"""Tests for repository factory configuration integration."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

from portfolio_manager.infrastructure.duckdb.repository_factory import DuckDBRepositoryFactory
from portfolio_manager.infrastructure.duckdb.connection import DuckDBConnection, DuckDBConfig
from portfolio_manager.config.schema import DatabaseConfig, DatabaseConnectionConfig, DatabasePoolConfig
from portfolio_manager.infrastructure.data_access.exceptions import ConnectionError


class TestDuckDBRepositoryFactoryConfigurationIntegration:
    """Test DuckDB repository factory with configuration objects."""

    def test_factory_initialization_without_config(self):
        """Test factory initialization without config object."""
        factory = DuckDBRepositoryFactory(
            database_path="./test.db",
            auto_initialize=False
        )

        assert factory.database_path == "./test.db"
        assert factory.auto_initialize is False
        assert factory.config is None

    def test_factory_initialization_with_config(self):
        """Test factory initialization with config object."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./configured.db",
                memory=False,
                read_only=True,
                pragmas={"threads": 8, "memory_limit": "4GB"}
            ),
            pool=DatabasePoolConfig(
                max_connections=20,
                connection_timeout=60
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./test.db",  # This should be overridden by config
            auto_initialize=True,
            config=db_config
        )

        assert factory.database_path == "./test.db"  # Constructor parameter takes precedence
        assert factory.auto_initialize is True
        assert factory.config is db_config
        assert factory.config.connection.read_only is True
        assert factory.config.connection.pragmas["threads"] == 8

    @pytest.mark.asyncio
    async def test_factory_initialization_with_memory_config(self):
        """Test factory initialization creates DuckDBConfig from DatabaseConfig."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path=":memory:",
                memory=True,
                read_only=False,
                pragmas={"threads": 2, "temp_directory": "/tmp/duck"}
            ),
            pool=DatabasePoolConfig(
                max_connections=5,
                connection_timeout=30
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path=":memory:",
            auto_initialize=False,
            config=db_config
        )

        # Mock the dependencies to test initialization logic
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor') as mock_executor, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            await factory.initialize()

            # Verify DuckDBConnection was created with proper DuckDBConfig
            mock_conn.assert_called_once()
            call_args = mock_conn.call_args
            assert call_args[0][0] == ":memory:"

            # Verify DuckDBConfig was created from DatabaseConfig
            duckdb_config = call_args[0][1]
            assert isinstance(duckdb_config, DuckDBConfig)
            assert duckdb_config.read_only is False
            assert duckdb_config.pragmas == {"threads": 2, "temp_directory": "/tmp/duck"}

    @pytest.mark.asyncio 
    async def test_factory_initialization_without_config_object(self):
        """Test factory initialization without config creates connection without DuckDBConfig."""
        factory = DuckDBRepositoryFactory(
            database_path="./test.db",
            auto_initialize=False,
            config=None
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            # Setup schema manager mock
            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()

            await factory.initialize()

            # Verify DuckDBConnection was created without DuckDBConfig
            mock_conn.assert_called_once_with("./test.db", None)

    @pytest.mark.asyncio
    async def test_factory_initialization_logging_with_config(self):
        """Test factory initialization logs configuration details."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./logged.db",
                memory=False,
                read_only=True,
                pragmas={"threads": 4, "memory_limit": "2GB"}
            ),
            pool=DatabasePoolConfig(
                max_connections=15,
                connection_timeout=45
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./logged.db",
            auto_initialize=True,
            config=db_config
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema, \
             patch.object(factory, '_logger') as mock_logger:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()

            await factory.initialize()

            # Verify configuration logging
            mock_logger.info.assert_any_call(
                "Using configuration: read_only=True, pragmas={'threads': 4, 'memory_limit': '2GB'}"
            )
            mock_logger.info.assert_any_call(
                "Database schema initialized successfully"
            )
            mock_logger.info.assert_any_call(
                "Repository factory initialized with database: ./logged.db"
            )

    @pytest.mark.asyncio
    async def test_factory_initialization_error_handling_with_config(self):
        """Test factory initialization error handling with config."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./error.db",
                memory=False,
                read_only=False,
                pragmas={}
            ),
            pool=DatabasePoolConfig(
                max_connections=10,
                connection_timeout=30
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./error.db",
            auto_initialize=False,
            config=db_config
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn:
            mock_conn.side_effect = Exception("Connection failed")

            with pytest.raises(ConnectionError, match="Failed to initialize repository factory"):
                await factory.initialize()

    def test_factory_config_property_access(self):
        """Test accessing config properties through factory."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./prop_test.db",
                memory=False,
                read_only=True,
                pragmas={"enable_progress_bar": True}
            ),
            pool=DatabasePoolConfig(
                max_connections=7,
                connection_timeout=25
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./prop_test.db",
            config=db_config
        )

        # Test direct config access
        assert factory.config.connection.read_only is True
        assert factory.config.connection.pragmas["enable_progress_bar"] == True
        assert factory.config.pool.max_connections == 7
        assert factory.config.pool.connection_timeout == 25

    @pytest.mark.asyncio
    async def test_factory_health_check_with_config(self):
        """Test factory health check includes config information."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./health_test.db",
                memory=False,
                read_only=False,
                pragmas={"threads": 6}
            ),
            pool=DatabasePoolConfig(
                max_connections=12,
                connection_timeout=40
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./health_test.db",
            auto_initialize=False,
            config=db_config
        )

        # Initialize with mocked dependencies
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            # Setup schema manager mock
            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()
            mock_connection_instance.ping = AsyncMock(return_value=True)
            mock_connection_instance.get_connection_info = AsyncMock(return_value={
                "database_path": "./health_test.db",
                "version": "0.9.0"
            })

            await factory.initialize()

            # Test health check
            health_result = await factory.health_check()

            assert health_result["status"] == "healthy"
            assert health_result["database_path"] == "./health_test.db"
            assert "connection_info" in health_result
            assert health_result["connection_info"]["database_path"] == "./health_test.db"


class TestRepositoryFactoryConfigurationEdgeCases:
    """Test edge cases and error scenarios for repository factory configuration."""

    def test_factory_with_conflicting_database_paths(self):
        """Test factory behavior when constructor path differs from config path."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./config_path.db",
                memory=False,
                read_only=False,
                pragmas={}
            ),
            pool=DatabasePoolConfig(
                max_connections=10,
                connection_timeout=30
            )
        )

        # Constructor path takes precedence over config path
        factory = DuckDBRepositoryFactory(
            database_path="./constructor_path.db",
            config=db_config
        )

        assert factory.database_path == "./constructor_path.db"
        assert factory.config.connection.database_path == "./config_path.db"

    def test_factory_with_empty_pragmas(self):
        """Test factory with empty pragmas in config."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path=":memory:",
                memory=True,
                read_only=False,
                pragmas={}
            ),
            pool=DatabasePoolConfig(
                max_connections=1,
                connection_timeout=5
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path=":memory:",
            config=db_config
        )

        assert factory.config.connection.pragmas == {}

    @pytest.mark.asyncio
    async def test_factory_initialization_with_complex_pragmas(self):
        """Test factory initialization with complex pragma configuration."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./complex.db",
                memory=False,
                read_only=False,
                pragmas={
                    "threads": 16,
                    "memory_limit": "8GB",
                    "temp_directory": "/var/tmp/duckdb",
                    "enable_progress_bar": True,
                    "enable_profiling": "json",
                    "profile_output": "/tmp/profile.json"
                }
            ),
            pool=DatabasePoolConfig(
                max_connections=50,
                connection_timeout=120
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./complex.db",
            auto_initialize=False,
            config=db_config
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            # Setup schema manager mock
            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()

            await factory.initialize()

            # Verify DuckDBConfig contains all pragmas
            call_args = mock_conn.call_args
            duckdb_config = call_args[0][1]
            assert len(duckdb_config.pragmas) == 6
            assert duckdb_config.pragmas["threads"] == 16
            assert duckdb_config.pragmas["memory_limit"] == "8GB"
            assert duckdb_config.pragmas["enable_progress_bar"] == True
            assert duckdb_config.pragmas["enable_profiling"] == "json"

    @pytest.mark.asyncio
    async def test_factory_reset_database_with_config(self):
        """Test database reset functionality with config."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./reset_test.db",
                memory=False,
                read_only=False,
                pragmas={"threads": 2}
            ),
            pool=DatabasePoolConfig(
                max_connections=5,
                connection_timeout=30
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./reset_test.db",
            auto_initialize=True,
            config=db_config
        )

        # Initialize factory
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()
            mock_schema_instance.drop_schema = AsyncMock()

            await factory.initialize()

            # Test database reset
            await factory.reset_database()

            # Verify schema operations
            mock_schema_instance.drop_schema.assert_called_once()
            assert mock_schema_instance.create_schema.call_count == 2  # Once during init, once during reset

    @pytest.mark.asyncio
    async def test_factory_convenience_function_compatibility(self):
        """Test convenience function create_repository_factory still works."""
        from portfolio_manager.infrastructure.duckdb.repository_factory import create_repository_factory

        # This should work without config
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBRepositoryFactory') as mock_factory_class:
            mock_instance = Mock()
            mock_instance.initialize = AsyncMock()
            mock_factory_class.return_value = mock_instance

            # Test async function
            result = await create_repository_factory("./convenience.db", auto_initialize=False)

            # Verify factory was created correctly
            mock_factory_class.assert_called_with("./convenience.db", False)

            # Verify the result is the mock instance
            assert result is mock_instance


class TestRepositoryFactoryConfigurationValidation:
    """Test configuration validation and error handling in repository factory."""

    def test_factory_with_invalid_config_type(self):
        """Test factory with invalid config type."""
        # This should not raise an error in constructor, but might in initialization
        factory = DuckDBRepositoryFactory(
            database_path="./test.db",
            config="invalid_config_type"  # Not a DatabaseConfig object
        )

        # The factory accepts any config type, validation happens during use
        assert factory.config == "invalid_config_type"

    @pytest.mark.asyncio
    async def test_factory_initialization_with_none_config_values(self):
        """Test factory initialization handles None values in config gracefully."""
        # Create config with some None values
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./none_test.db",
                memory=False,
                read_only=False,
                pragmas={}  # Empty dict, not None
            ),
            pool=DatabasePoolConfig(
                max_connections=10,
                connection_timeout=30
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./none_test.db",
            config=db_config
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            # Setup schema manager mock
            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()

            # Should not raise error
            await factory.initialize()

            # Verify connection created properly
            mock_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_factory_with_read_only_configuration(self):
        """Test factory respects read-only configuration."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./readonly.db",
                memory=False,
                read_only=True,  # Read-only mode
                pragmas={"threads": 1}
            ),
            pool=DatabasePoolConfig(
                max_connections=3,
                connection_timeout=15
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./readonly.db",
            auto_initialize=False,  # Don't auto-initialize for read-only
            config=db_config
        )

        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager') as mock_schema:

            mock_connection_instance = Mock()
            mock_conn.return_value = mock_connection_instance
            mock_connection_instance.connect = AsyncMock()

            # Setup schema manager mock
            mock_schema_instance = Mock()
            mock_schema.return_value = mock_schema_instance
            mock_schema_instance.create_schema = AsyncMock()

            await factory.initialize()

            # Verify DuckDBConfig was created with read_only=True
            call_args = mock_conn.call_args
            duckdb_config = call_args[0][1]
            assert duckdb_config.read_only is True

    def test_factory_config_attribute_preservation(self):
        """Test that all config attributes are preserved in factory."""
        original_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./preserve_test.db",
                memory=True,
                read_only=False,
                pragmas={"preserve_insertion_order": True}
            ),
            pool=DatabasePoolConfig(
                max_connections=25,
                connection_timeout=90
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./preserve_test.db",
            config=original_config
        )

        # Verify all attributes are preserved
        assert factory.config is original_config
        assert factory.config.type == "duckdb"
        assert factory.config.connection.database_path == "./preserve_test.db"
        assert factory.config.connection.memory is True
        assert factory.config.connection.read_only is False
        assert factory.config.connection.pragmas["preserve_insertion_order"] == True
        assert factory.config.pool.max_connections == 25
        assert factory.config.pool.connection_timeout == 90


class TestRepositoryCreationWithConfiguration:
    """Test repository creation uses configuration properly."""

    @pytest.mark.asyncio
    async def test_create_asset_repository_with_config(self):
        """Test asset repository creation with configuration."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path=":memory:",
                memory=True,
                read_only=False,
                pragmas={"threads": 4}
            ),
            pool=DatabasePoolConfig(
                max_connections=10,
                connection_timeout=30
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path=":memory:",
            auto_initialize=False,
            config=db_config
        )

        # Mock the dependencies
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor') as mock_executor, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBAssetRepository') as mock_asset_repo, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.AssetRepositoryAdapter') as mock_adapter:

            mock_connection_instance = Mock()
            mock_executor_instance = Mock()
            mock_asset_repo_instance = Mock()
            mock_adapter_instance = Mock()

            mock_conn.return_value = mock_connection_instance
            mock_executor.return_value = mock_executor_instance
            mock_asset_repo.return_value = mock_asset_repo_instance
            mock_adapter.return_value = mock_adapter_instance

            mock_connection_instance.connect = AsyncMock()

            await factory.initialize()
            asset_repo = factory.create_asset_repository()

            # Verify repository creation
            mock_asset_repo.assert_called_once_with(mock_connection_instance, mock_executor_instance)
            mock_adapter.assert_called_once_with(mock_asset_repo_instance)
            assert asset_repo is mock_adapter_instance

    @pytest.mark.asyncio
    async def test_create_portfolio_repository_with_config(self):
        """Test portfolio repository creation with configuration."""
        db_config = DatabaseConfig(
            type="duckdb",
            connection=DatabaseConnectionConfig(
                database_path="./portfolio_test.db",
                memory=False,
                read_only=True,
                pragmas={"enable_object_cache": True}
            ),
            pool=DatabasePoolConfig(
                max_connections=15,
                connection_timeout=45
            )
        )

        factory = DuckDBRepositoryFactory(
            database_path="./portfolio_test.db",
            auto_initialize=False,
            config=db_config
        )

        # Mock the dependencies
        with patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBConnection') as mock_conn, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBQueryExecutor') as mock_executor, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBSchemaManager'), \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.DuckDBPortfolioRepository') as mock_portfolio_repo, \
             patch('portfolio_manager.infrastructure.duckdb.repository_factory.PortfolioRepositoryAdapter') as mock_adapter:

            mock_connection_instance = Mock()
            mock_executor_instance = Mock()
            mock_portfolio_repo_instance = Mock()
            mock_adapter_instance = Mock()

            mock_conn.return_value = mock_connection_instance
            mock_executor.return_value = mock_executor_instance
            mock_portfolio_repo.return_value = mock_portfolio_repo_instance
            mock_adapter.return_value = mock_adapter_instance

            mock_connection_instance.connect = AsyncMock()

            await factory.initialize()
            portfolio_repo = factory.create_portfolio_repository()

            # Verify repository creation
            mock_portfolio_repo.assert_called_once_with(mock_connection_instance, mock_executor_instance)
            mock_adapter.assert_called_once_with(mock_portfolio_repo_instance)
            assert portfolio_repo is mock_adapter_instance
