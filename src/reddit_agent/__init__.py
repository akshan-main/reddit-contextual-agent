"""Reddit Agent for Contextual AI - Scrapes and syncs Reddit posts to Contextual AI datastore."""

__version__ = "0.2.0"

from .config import Config, load_config
from .contextual_client import ContextualClient
from .db import SupabaseDatabase
from .models import PostStatus, RedditComment, RedditPost, TrackedPost
from .pipeline import Pipeline, PipelineStats, run_pipeline
from .scraper import RedditScraper

__all__ = [
    "Config",
    "load_config",
    "SupabaseDatabase",
    "PostStatus",
    "RedditComment",
    "RedditPost",
    "TrackedPost",
    "Pipeline",
    "PipelineStats",
    "run_pipeline",
    "RedditScraper",
    "ContextualClient",
]
