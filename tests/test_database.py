"""Tests for database module."""

import os
from datetime import datetime, timezone

import pytest

from reddit_agent.db import SupabaseDatabase, compute_content_hash
from reddit_agent.models import PostStatus, TrackedPost


class TestContentHash:
    """Tests for content hash computation."""

    def test_compute_content_hash(self, sample_post):
        """Test content hash computation."""
        hash1 = compute_content_hash(sample_post)
        assert isinstance(hash1, str)
        assert len(hash1) == 16

    def test_content_hash_changes_with_content(self, sample_post):
        """Test that hash changes when actual content changes."""
        hash1 = compute_content_hash(sample_post)
        # Change actual content (not just score which doesn't affect RAG)
        sample_post.selftext = "Completely different content now"
        hash2 = compute_content_hash(sample_post)
        assert hash1 != hash2

    def test_content_hash_deterministic(self, sample_post):
        """Test that same content produces same hash."""
        hash1 = compute_content_hash(sample_post)
        hash2 = compute_content_hash(sample_post)
        assert hash1 == hash2

    def test_content_hash_ignores_score(self, sample_post):
        """Test that hash doesn't change when only score changes."""
        hash1 = compute_content_hash(sample_post)
        sample_post.score = 9999
        hash2 = compute_content_hash(sample_post)
        assert hash1 == hash2

    def test_content_hash_ignores_num_comments(self, sample_post):
        """Test that hash doesn't change when only num_comments changes (volatile metadata)."""
        hash1 = compute_content_hash(sample_post)
        sample_post.num_comments = 9999
        hash2 = compute_content_hash(sample_post)
        assert hash1 == hash2


# Integration tests require a real Supabase connection
# Set SUPABASE_TEST_CONNECTION_STRING env var to run these
@pytest.mark.skipif(
    not os.getenv("SUPABASE_TEST_CONNECTION_STRING"),
    reason="Supabase integration tests require SUPABASE_TEST_CONNECTION_STRING",
)
class TestSupabaseIntegration:
    """Integration tests for Supabase database."""

    @pytest.fixture
    async def db(self):
        """Create a test database connection."""
        connection_string = os.environ["SUPABASE_TEST_CONNECTION_STRING"]
        db = SupabaseDatabase(connection_string)
        await db.connect()
        yield db
        await db.close()

    @pytest.mark.asyncio
    async def test_database_connection(self, db):
        """Test database connects successfully."""
        stats = await db.get_stats()
        assert "total_tracked" in stats

    @pytest.mark.asyncio
    async def test_save_and_get_post(self, db, sample_post):
        """Test saving and retrieving a post."""
        await db.save_post(sample_post)
        retrieved = await db.get_post(sample_post.id)

        assert retrieved is not None
        assert retrieved.id == sample_post.id
        assert retrieved.title == sample_post.title
        assert retrieved.score == sample_post.score

    @pytest.mark.asyncio
    async def test_upsert_tracked_post(self, db, sample_post, sample_tracked_post):
        """Test upserting a tracked post."""
        await db.save_post(sample_post)
        await db.upsert_tracked_post(sample_tracked_post)

        tracked = await db.get_tracked_post(sample_post.id)
        assert tracked is not None
        assert tracked.post_id == sample_post.id
        assert tracked.contextual_doc_id == sample_tracked_post.contextual_doc_id

    @pytest.mark.asyncio
    async def test_post_exists(self, db, sample_post, sample_tracked_post):
        """Test checking if post exists."""
        exists_before = await db.post_exists(sample_post.id)
        assert exists_before is False

        await db.save_post(sample_post)
        await db.upsert_tracked_post(sample_tracked_post)

        exists_after = await db.post_exists(sample_post.id)
        assert exists_after is True

    @pytest.mark.asyncio
    async def test_queue_operations(self, db):
        """Test queue add and get."""
        await db.add_to_queue(
            post_id="queue_test_123",
            subreddit="test",
            action="ingest",
            priority=1,
        )

        items = await db.get_queue_items(limit=10)
        our_item = next((i for i in items if i["post_id"] == "queue_test_123"), None)

        assert our_item is not None
        assert our_item["action"] == "ingest"
        assert our_item["priority"] == 1

        if our_item:
            await db.mark_queue_success(our_item["id"])

    @pytest.mark.asyncio
    async def test_get_posts_to_update(self, db, sample_post):
        """Test getting posts that need updating (count-based)."""
        await db.save_post(sample_post)

        tracked_post = TrackedPost(
            post_id=sample_post.id,
            subreddit=sample_post.subreddit,
            created_utc=datetime.now(timezone.utc),
            first_scraped=datetime.now(timezone.utc),
            last_updated=datetime.now(timezone.utc),
            update_count=-1,
            status=PostStatus.NEW,
            contextual_doc_id="reddit_post_post123",
            content_hash="abc123def456",
        )
        await db.upsert_tracked_post(tracked_post)

        # update_count = -1: should be returned (freeze_at_count=3)
        posts = await db.get_posts_to_update(freeze_at_count=3)
        post_ids = [p.post_id for p in posts]
        assert sample_post.id in post_ids

        # update_count = 3: should NOT be returned
        tracked_post.update_count = 3
        await db.upsert_tracked_post(tracked_post)

        posts = await db.get_posts_to_update(freeze_at_count=3)
        post_ids = [p.post_id for p in posts]
        assert sample_post.id not in post_ids

    @pytest.mark.asyncio
    async def test_get_stats(self, db, sample_post, sample_tracked_post):
        """Test getting database statistics."""
        await db.save_post(sample_post)
        await db.upsert_tracked_post(sample_tracked_post)

        stats = await db.get_stats()
        assert stats["total_tracked"] >= 1
        assert "by_status" in stats
        assert "by_subreddit" in stats
        assert "queue_pending" in stats
