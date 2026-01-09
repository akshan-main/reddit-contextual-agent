"""Tests for pipeline module."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from reddit_agent.config import Config
from reddit_agent.models import PostStatus
from reddit_agent.pipeline import Pipeline, PipelineStats


def _utc_now() -> datetime:
    """Helper to get current UTC time for tests."""
    return datetime.now(timezone.utc)


class TestPipelineStats:
    """Tests for PipelineStats dataclass."""

    def test_default_stats(self):
        """Test default stats values."""
        stats = PipelineStats()
        assert stats.posts_scraped == 0
        assert stats.new_posts == 0
        assert stats.documents_ingested == 0
        assert stats.completed_at is None
        assert stats.started_at is not None

    def test_stats_to_dict(self):
        """Test conversion to dictionary."""
        stats = PipelineStats()
        stats.posts_scraped = 10
        stats.new_posts = 5
        stats.completed_at = _utc_now()

        result = stats.to_dict()

        assert result["posts_scraped"] == 10
        assert result["new_posts"] == 5
        assert "started_at" in result
        assert "completed_at" in result
        assert "duration_seconds" in result

    def test_stats_to_dict_no_completion(self):
        """Test to_dict when not completed."""
        stats = PipelineStats()
        result = stats.to_dict()

        assert result["completed_at"] is None
        assert result["duration_seconds"] is None


class TestPipeline:
    """Tests for Pipeline class."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return Config()

    @pytest.fixture
    def pipeline(self, config, mock_db, mock_contextual_client):
        """Create pipeline with mocked dependencies."""
        with (
            patch("reddit_agent.pipeline.SupabaseDatabase") as mock_db_cls,
            patch("reddit_agent.pipeline.ContextualClient") as mock_ctx_cls,
            patch("reddit_agent.pipeline.RedditScraper") as mock_scraper_cls,
        ):
            mock_db_cls.return_value = mock_db
            mock_ctx_cls.return_value = mock_contextual_client
            mock_scraper_cls.return_value = AsyncMock()
            mock_scraper_cls.return_value.scrape_all_subreddits = AsyncMock(return_value=([], []))
            mock_scraper_cls.return_value.refresh_post = AsyncMock(return_value=None)

            pipeline = Pipeline(config)
            pipeline.db = mock_db
            pipeline.contextual = mock_contextual_client
            return pipeline

    def test_pipeline_initialization(self, config):
        """Test pipeline initializes correctly."""
        with (
            patch("reddit_agent.pipeline.SupabaseDatabase"),
            patch("reddit_agent.pipeline.ContextualClient"),
            patch("reddit_agent.pipeline.RedditScraper"),
        ):
            pipeline = Pipeline(config)
            assert pipeline.config == config
            assert pipeline.stats is not None

    @pytest.mark.asyncio
    async def test_pipeline_context_manager(self, pipeline, mock_db, mock_contextual_client):
        """Test pipeline as async context manager."""
        async with pipeline:
            mock_db.connect.assert_called_once()
            mock_contextual_client.connect.assert_called_once()

        mock_db.close.assert_called_once()
        mock_contextual_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_new_post_existing(self, pipeline, mock_db, sample_tracked_post):
        """Test processing existing post returns existing doc_id."""
        mock_db.get_tracked_post.return_value = sample_tracked_post

        result = await pipeline._process_new_post(MagicMock(id="post123"))

        assert result == sample_tracked_post.contextual_doc_id
        mock_db.save_post.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_new_post_new(self, pipeline, mock_db, mock_contextual_client, sample_post):
        """Test processing new post ingests and creates tracking."""
        mock_db.get_tracked_post.return_value = None
        mock_db.compute_content_hash.return_value = "abc123"
        mock_contextual_client.ingest_document.return_value = "reddit_post_post123"

        result = await pipeline._process_new_post(sample_post)

        assert result == "reddit_post_post123"
        mock_db.save_post.assert_called_once_with(sample_post)
        mock_contextual_client.ingest_document.assert_called_once_with(sample_post)
        mock_db.upsert_tracked_post.assert_called_once()
        assert pipeline.stats.new_posts == 1
        assert pipeline.stats.documents_ingested == 1

    @pytest.mark.asyncio
    async def test_process_new_post_failure_queues(self, pipeline, mock_db, mock_contextual_client, sample_post):
        """Test failed ingestion queues for retry."""
        mock_db.get_tracked_post.return_value = None
        mock_contextual_client.ingest_document.side_effect = Exception("API Error")

        result = await pipeline._process_new_post(sample_post)

        assert result is None
        mock_db.add_to_queue.assert_called_once()
        assert pipeline.stats.sync_errors == 1
        assert pipeline.stats.queued_for_retry == 1

    @pytest.mark.asyncio
    async def test_update_post_already_processed_today(self, pipeline, mock_db, sample_tracked_post):
        """Test update skips if already processed today."""
        sample_tracked_post.last_updated = _utc_now()

        result = await pipeline._update_post(sample_tracked_post)

        assert result is True
        mock_db.upsert_tracked_post.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_post_increments_count_before_refresh(self, pipeline, mock_db, sample_tracked_post):
        """Test update increments count when below refresh_at."""
        sample_tracked_post.last_updated = _utc_now() - timedelta(days=1)
        sample_tracked_post.update_count = 0  # Below default refresh_at=1
        pipeline.config.scraper.refresh_at_count = 1

        result = await pipeline._update_post(sample_tracked_post)

        assert result is True
        mock_db.upsert_tracked_post.assert_called_once()
        assert pipeline.stats.skipped_unchanged == 1

    @pytest.mark.asyncio
    async def test_freeze_post(self, pipeline, mock_db, sample_tracked_post):
        """Test freezing a post."""
        await pipeline._freeze_post(sample_tracked_post)

        assert sample_tracked_post.status == PostStatus.FROZEN
        mock_db.upsert_tracked_post.assert_called_once()
        assert pipeline.stats.frozen_posts == 1

    @pytest.mark.asyncio
    async def test_process_queue_empty(self, pipeline, mock_db):
        """Test processing empty queue."""
        mock_db.get_queue_items.return_value = []

        await pipeline._process_queue()

        mock_db.get_queue_items.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_queue_ingest_action(self, pipeline, mock_db, mock_contextual_client, sample_post):
        """Test queue processing for ingest action."""
        mock_db.get_queue_items.return_value = [
            {"id": 1, "post_id": "post123", "action": "ingest", "subreddit": "test"}
        ]
        mock_db.get_post.return_value = sample_post
        mock_db.get_tracked_post.return_value = MagicMock(contextual_doc_id=None)
        mock_contextual_client.ingest_document.return_value = "doc_123"

        await pipeline._process_queue()

        mock_contextual_client.ingest_document.assert_called_once()
        mock_db.mark_queue_success.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_scrape_and_process_new(self, pipeline, mock_db, sample_post):
        """Test scrape and process new posts."""
        pipeline.scraper = AsyncMock()
        pipeline.scraper.scrape_all_subreddits = AsyncMock(return_value=([sample_post], []))
        mock_db.get_tracked_post.return_value = None
        mock_db.compute_content_hash.return_value = "abc123"

        await pipeline.scrape_and_process_new()

        assert pipeline.stats.posts_scraped == 1

    @pytest.mark.asyncio
    async def test_update_existing_posts(self, pipeline, mock_db, sample_tracked_post):
        """Test update existing posts phase."""
        sample_tracked_post.last_updated = _utc_now() - timedelta(days=1)
        sample_tracked_post.update_count = 0
        mock_db.get_posts_to_update.return_value = [sample_tracked_post]

        await pipeline.update_existing_posts()

        mock_db.get_posts_to_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_freeze_old_posts(self, pipeline, mock_db, sample_tracked_post):
        """Test freeze old posts phase."""
        mock_db.get_posts_to_freeze.return_value = [sample_tracked_post]

        await pipeline.freeze_old_posts()

        mock_db.get_posts_to_freeze.assert_called_once()
        assert pipeline.stats.frozen_posts == 1

    @pytest.mark.asyncio
    async def test_cleanup(self, pipeline, mock_db):
        """Test cleanup phase."""
        mock_db.cleanup_old_posts.return_value = 5

        await pipeline.cleanup()

        mock_db.cleanup_old_posts.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_disabled(self, pipeline, mock_db):
        """Test cleanup disabled when days=0."""
        pipeline.config.scraper.cleanup_after_days = 0

        await pipeline.cleanup()

        mock_db.cleanup_old_posts.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_full_pipeline(self, pipeline, mock_db):
        """Test full pipeline run."""
        mock_db.get_queue_items.return_value = []
        mock_db.get_posts_with_missing_hash.return_value = []
        mock_db.get_posts_to_update.return_value = []
        mock_db.get_posts_to_freeze.return_value = []
        pipeline.scraper = AsyncMock()
        pipeline.scraper.scrape_all_subreddits = AsyncMock(return_value=([], []))

        stats = await pipeline.run()

        assert stats.completed_at is not None
        mock_db.get_stats.assert_called()

    @pytest.mark.asyncio
    async def test_run_scrape_only(self, pipeline, mock_db):
        """Test scrape-only mode."""
        pipeline.scraper = AsyncMock()
        pipeline.scraper.scrape_all_subreddits = AsyncMock(return_value=([], []))

        stats = await pipeline.run_scrape_only()

        assert stats.completed_at is not None

    @pytest.mark.asyncio
    async def test_run_update_only(self, pipeline, mock_db):
        """Test update-only mode."""
        mock_db.get_queue_items.return_value = []
        mock_db.get_posts_to_update.return_value = []
        mock_db.get_posts_to_freeze.return_value = []

        stats = await pipeline.run_update_only()

        assert stats.completed_at is not None

    @pytest.mark.asyncio
    async def test_run_queue_only(self, pipeline, mock_db):
        """Test queue-only mode."""
        mock_db.get_queue_items.return_value = []

        stats = await pipeline.run_queue_only()

        assert stats.completed_at is not None

    @pytest.mark.asyncio
    async def test_fix_missing_hashes_none(self, pipeline, mock_db):
        """Test fix_missing_hashes with no missing hashes."""
        mock_db.get_posts_with_missing_hash.return_value = []

        await pipeline.fix_missing_hashes()

        mock_db.get_posts_with_missing_hash.assert_called_once()

    @pytest.mark.asyncio
    async def test_fix_missing_hashes_with_posts(
        self, pipeline, mock_db, mock_contextual_client, sample_tracked_post, sample_post
    ):
        """Test fix_missing_hashes re-ingests posts."""
        mock_db.get_posts_with_missing_hash.return_value = [sample_tracked_post]
        mock_db.compute_content_hash.return_value = "new_hash"
        pipeline.scraper = AsyncMock()
        pipeline.scraper.refresh_post = AsyncMock(return_value=sample_post)

        await pipeline.fix_missing_hashes()

        mock_contextual_client.smart_sync.assert_called_once()
        mock_db.upsert_tracked_post.assert_called()
