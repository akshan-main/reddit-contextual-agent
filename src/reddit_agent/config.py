"""Configuration management for the Reddit agent with SQLite persistence."""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class RedditConfig:
    """Reddit API configuration."""

    client_id: str = field(default_factory=lambda: os.environ["REDDIT_CLIENT_ID"])
    client_secret: str = field(default_factory=lambda: os.environ["REDDIT_CLIENT_SECRET"])
    user_agent: str = field(default_factory=lambda: os.getenv("REDDIT_USER_AGENT", "reddit-contextual-agent/0.2.0"))


@dataclass
class ContextualConfig:
    """Contextual AI configuration."""

    api_key: str = field(default_factory=lambda: os.environ["CONTEXTUAL_API_KEY"])
    datastore_id: str = field(default_factory=lambda: os.environ["CONTEXTUAL_DATASTORE_ID"])
    # Optional since we haven't used agents thus far
    agent_id: str = field(default_factory=lambda: os.getenv("CONTEXTUAL_AGENT_ID", ""))
    # API base URL
    base_url: str = field(default_factory=lambda: os.getenv("CONTEXTUAL_API_URL", "https://api.contextual.ai"))


@dataclass
class ScraperConfig:
    """Scraper behavior configuration."""

    subreddits: list[str] = field(
        default_factory=lambda: [
            s.strip()
            for s in os.getenv("SUBREDDITS", "contextengineering,RAG,LocalLLaMA,AgentsOfAI,AI_Agents").split(",")
            if s.strip()
        ]
    )
    # Time window in hours - scrapes ALL posts within this window
    # Default 26 hours for daily runs (2 hour overlap for safety)
    time_window_hours: int = field(default_factory=lambda: int(os.getenv("TIME_WINDOW_HOURS", "26")))
    # Update window in days (posts tracked and updated for this period, then frozen)
    update_window_days: int = field(default_factory=lambda: int(os.getenv("UPDATE_WINDOW_DAYS", "2")))
    # Max comments per post
    max_comments: int = field(default_factory=lambda: int(os.getenv("MAX_COMMENTS", "100")))
    # Rate limiting (Reddit allows upto 60 per minute)
    requests_per_minute: int = field(default_factory=lambda: int(os.getenv("REQUESTS_PER_MINUTE", "30")))
    min_request_delay: float = field(default_factory=lambda: float(os.getenv("MIN_REQUEST_DELAY", "2.0")))
    # Include comments in posts
    include_comments: bool = field(default_factory=lambda: os.getenv("INCLUDE_COMMENTS", "true").lower() == "true")

    # REFRESH SETTINGS
    # At which update_count to start refreshing (default: 0 means Day 3)
    # Count progression: -1 (Day 1, scrape) -> 0 (Day 2, skip) -> 1 (Day 3, refresh) -> 2 (Day 4, freeze)
    refresh_at_count: int = field(default_factory=lambda: int(os.getenv("REFRESH_AT_COUNT", "0")))
    # When to freeze (default: 2 = freeze on Day 4)
    freeze_at_count: int = field(default_factory=lambda: int(os.getenv("FREEZE_AT_COUNT", "2")))
    # Whether to always re-ingest on refresh, or only if content changed
    # True = always re-ingest at refresh_at_count
    # False = check for changes first, skip if unchanged
    always_reingest_on_refresh: bool = field(
        default_factory=lambda: os.getenv("ALWAYS_REINGEST_ON_REFRESH", "false").lower() == "true"
    )
    # Days to keep posts in database before cleanup (default: 30)
    # Set to 0 to disable cleanup
    cleanup_after_days: int = field(default_factory=lambda: int(os.getenv("CLEANUP_AFTER_DAYS", "30")))


@dataclass
class SupabaseConfig:
    """Supabase database configuration."""

    connection_string: str = field(default_factory=lambda: os.environ["SUPABASE_CONNECTION_STRING"])


@dataclass
class Config:
    """Main configuration container with Supabase database."""

    reddit: RedditConfig = field(default_factory=RedditConfig)
    contextual: ContextualConfig = field(default_factory=ContextualConfig)
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)


def load_config() -> Config:
    """Load configuration from environment variables."""
    return Config()
