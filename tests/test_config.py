"""Tests for configuration module."""

import os
from unittest.mock import patch

from reddit_agent.config import (
    Config,
    ContextualConfig,
    RedditConfig,
    ScraperConfig,
    SupabaseConfig,
    load_config,
)


class TestRedditConfig:
    """Tests for Reddit configuration."""

    def test_reddit_config_from_env(self):
        """Test Reddit config loads from environment."""
        with patch.dict(
            os.environ,
            {
                "REDDIT_CLIENT_ID": "test_id",
                "REDDIT_CLIENT_SECRET": "test_secret",
                "REDDIT_USER_AGENT": "test_agent/1.0",
            },
        ):
            config = RedditConfig()
            assert config.client_id == "test_id"
            assert config.client_secret == "test_secret"
            assert config.user_agent == "test_agent/1.0"

    def test_reddit_config_default_user_agent(self):
        """Test Reddit config uses default user agent."""
        with patch.dict(
            os.environ,
            {
                "REDDIT_CLIENT_ID": "test_id",
                "REDDIT_CLIENT_SECRET": "test_secret",
            },
            clear=False,
        ):
            # Remove USER_AGENT if present
            env = os.environ.copy()
            env.pop("REDDIT_USER_AGENT", None)
            with patch.dict(os.environ, env, clear=True):
                # Re-set required vars
                os.environ["REDDIT_CLIENT_ID"] = "test_id"
                os.environ["REDDIT_CLIENT_SECRET"] = "test_secret"
                config = RedditConfig()
                assert "reddit-contextual-agent" in config.user_agent


class TestContextualConfig:
    """Tests for Contextual AI configuration."""

    def test_contextual_config_from_env(self):
        """Test Contextual config loads from environment."""
        with patch.dict(
            os.environ,
            {
                "CONTEXTUAL_API_KEY": "test_key",
                "CONTEXTUAL_DATASTORE_ID": "test_datastore",
                "CONTEXTUAL_AGENT_ID": "test_agent",
            },
        ):
            config = ContextualConfig()
            assert config.api_key == "test_key"
            assert config.datastore_id == "test_datastore"
            assert config.agent_id == "test_agent"

    def test_contextual_config_optional_agent_id(self):
        """Test Contextual config with optional agent ID."""
        with patch.dict(
            os.environ,
            {
                "CONTEXTUAL_API_KEY": "test_key",
                "CONTEXTUAL_DATASTORE_ID": "test_datastore",
            },
            clear=False,
        ):
            env = os.environ.copy()
            env.pop("CONTEXTUAL_AGENT_ID", None)
            with patch.dict(os.environ, env, clear=True):
                os.environ["CONTEXTUAL_API_KEY"] = "test_key"
                os.environ["CONTEXTUAL_DATASTORE_ID"] = "test_datastore"
                config = ContextualConfig()
                assert config.agent_id == ""

    def test_contextual_config_default_base_url(self):
        """Test Contextual config uses default base URL."""
        config = ContextualConfig()
        assert config.base_url == "https://api.contextual.ai"


class TestScraperConfig:
    """Tests for scraper configuration."""

    def test_scraper_config_defaults(self):
        """Test scraper config default values."""
        config = ScraperConfig()
        assert config.time_window_hours == 26
        assert config.max_comments == 100
        assert config.requests_per_minute == 30
        assert config.min_request_delay == 2.0
        assert config.include_comments is True

    def test_scraper_config_subreddits_from_env(self):
        """Test subreddits parsed from comma-separated env var."""
        with patch.dict(
            os.environ,
            {
                "SUBREDDITS": "sub1,sub2,sub3",
            },
        ):
            config = ScraperConfig()
            assert config.subreddits == ["sub1", "sub2", "sub3"]

    def test_scraper_config_subreddits_strips_whitespace(self):
        """Test subreddits strips whitespace."""
        with patch.dict(
            os.environ,
            {
                "SUBREDDITS": "sub1 , sub2 , sub3 ",
            },
        ):
            config = ScraperConfig()
            assert config.subreddits == ["sub1", "sub2", "sub3"]

    def test_scraper_config_refresh_settings(self):
        """Test refresh/freeze count settings."""
        with patch.dict(
            os.environ,
            {
                "REFRESH_AT_COUNT": "2",
                "FREEZE_AT_COUNT": "4",
                "ALWAYS_REINGEST_ON_REFRESH": "true",
            },
        ):
            config = ScraperConfig()
            assert config.refresh_at_count == 2
            assert config.freeze_at_count == 4
            assert config.always_reingest_on_refresh is True

    def test_scraper_config_cleanup_days(self):
        """Test cleanup days setting."""
        with patch.dict(
            os.environ,
            {
                "CLEANUP_AFTER_DAYS": "60",
            },
        ):
            config = ScraperConfig()
            assert config.cleanup_after_days == 60


class TestSupabaseConfig:
    """Tests for Supabase configuration."""

    def test_supabase_config_from_env(self):
        """Test Supabase config loads from environment."""
        with patch.dict(
            os.environ,
            {
                "SUPABASE_CONNECTION_STRING": "postgresql://user:pass@host:5432/db",
            },
        ):
            config = SupabaseConfig()
            assert config.connection_string == "postgresql://user:pass@host:5432/db"


class TestConfig:
    """Tests for main Config container."""

    def test_config_contains_all_sub_configs(self):
        """Test Config contains all sub-configurations."""
        config = Config()
        assert isinstance(config.reddit, RedditConfig)
        assert isinstance(config.contextual, ContextualConfig)
        assert isinstance(config.scraper, ScraperConfig)
        assert isinstance(config.supabase, SupabaseConfig)

    def test_load_config_function(self):
        """Test load_config convenience function."""
        config = load_config()
        assert isinstance(config, Config)
