"""Pytest fixtures for Reddit Contextual Agent tests."""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

# Set test environment variables before importing modules
os.environ.setdefault("REDDIT_CLIENT_ID", "test_client_id")
os.environ.setdefault("REDDIT_CLIENT_SECRET", "test_client_secret")
os.environ.setdefault("CONTEXTUAL_API_KEY", "test_api_key")
os.environ.setdefault("CONTEXTUAL_AGENT_ID", "test_agent_id")
os.environ.setdefault("CONTEXTUAL_DATASTORE_ID", "test_datastore_id")
os.environ.setdefault("SUPABASE_CONNECTION_STRING", "postgresql://test:test@localhost:5432/test")

from reddit_agent.models import PostStatus, RedditComment, RedditPost, TrackedPost


@pytest.fixture
def sample_comment():
    """Create a sample Reddit comment."""
    # Use recent date for tests that check update window
    recent = datetime.now(timezone.utc) - timedelta(hours=2)
    return RedditComment(
        id="comment123",
        author="test_user",
        body="This is a test comment with some interesting content.",
        score=42,
        created_utc=recent,
        parent_id="t3_post123",
        is_submitter=False,
        edited=False,
        depth=0,
    )


@pytest.fixture
def sample_post(sample_comment):
    """Create a sample Reddit post with comments."""
    # Use recent date for tests that check update window
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    return RedditPost(
        id="post123",
        subreddit="contextengineering",
        author="test_author",
        title="Test Post About Context Engineering",
        selftext="This is a detailed test post about context engineering techniques.",
        url="https://reddit.com/r/contextengineering/comments/post123/test_post/",
        permalink="/r/contextengineering/comments/post123/test_post/",
        score=150,
        upvote_ratio=0.95,
        num_comments=10,
        created_utc=recent,
        edited=False,
        link_flair_text="Discussion",
        is_self=True,
        comments=[sample_comment],
        scraped_at=recent,
        last_updated=recent,
    )


@pytest.fixture
def sample_tracked_post():
    """Create a sample tracked post record."""
    # Use recent date for tests that check update window
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    return TrackedPost(
        post_id="post123",
        subreddit="contextengineering",
        created_utc=recent,
        first_scraped=recent,
        last_updated=recent,
        update_count=0,
        status=PostStatus.NEW,
        contextual_doc_id="reddit_post_post123",
        content_hash="abc123def456",
    )


@pytest.fixture
def mock_praw_submission():
    """Create a mock PRAW submission object."""
    submission = MagicMock()
    submission.id = "post123"
    submission.subreddit.display_name = "contextengineering"
    submission.author = MagicMock()
    submission.author.__str__ = MagicMock(return_value="test_author")
    submission.title = "Test Post About Context Engineering"
    submission.selftext = "This is test content."
    submission.url = "https://reddit.com/r/contextengineering/comments/post123/"
    submission.permalink = "/r/contextengineering/comments/post123/"
    submission.score = 150
    submission.upvote_ratio = 0.95
    submission.num_comments = 10
    submission.created_utc = 1705312800.0  # 2024-01-15 10:00:00 UTC
    submission.edited = False
    submission.link_flair_text = "Discussion"
    submission.link_flair_css_class = None
    submission.total_awards_received = 0
    submission.is_self = True
    submission.is_video = False
    submission.is_original_content = False
    submission.over_18 = False
    submission.spoiler = False
    submission.stickied = False
    submission.locked = False
    submission.archived = False
    submission.thumbnail = "self"

    # Mock comments
    mock_comment = MagicMock()
    mock_comment.id = "comment123"
    mock_comment.author = MagicMock()
    mock_comment.author.__str__ = MagicMock(return_value="commenter")
    mock_comment.body = "Great post!"
    mock_comment.score = 42
    mock_comment.created_utc = 1705320000.0
    mock_comment.parent_id = "t3_post123"
    mock_comment.is_submitter = False
    mock_comment.edited = False
    mock_comment.depth = 0

    submission.comments = MagicMock()
    submission.comments.replace_more = MagicMock()
    submission.comments.list = MagicMock(return_value=[mock_comment])

    return submission


@pytest.fixture
def mock_db():
    """Create a mock database."""
    from reddit_agent.db import compute_content_hash

    db = AsyncMock()
    db.connect = AsyncMock()
    db.close = AsyncMock()
    db.get_tracked_post = AsyncMock(return_value=None)
    db.save_post = AsyncMock()
    db.upsert_tracked_post = AsyncMock()
    db.get_posts_to_update = AsyncMock(return_value=[])
    db.get_posts_to_freeze = AsyncMock(return_value=[])
    db.get_posts_with_missing_hash = AsyncMock(return_value=[])
    db.get_queue_items = AsyncMock(return_value=[])
    db.add_to_queue = AsyncMock()
    db.mark_queue_success = AsyncMock()
    db.mark_queue_failure = AsyncMock()
    db.cleanup_old_posts = AsyncMock(return_value=0)
    db.get_stats = AsyncMock(
        return_value={
            "total_tracked": 0,
            "by_status": {},
            "by_subreddit": {},
            "total_comments": 0,
            "queue_pending": 0,
        }
    )
    # Add compute_content_hash as static method
    db.compute_content_hash = staticmethod(compute_content_hash)
    return db


@pytest.fixture
def mock_contextual_client():
    """Create a mock Contextual AI client."""
    client = AsyncMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.ingest_document = AsyncMock(return_value="reddit_post_post123")
    client.update_metadata = AsyncMock(return_value=True)
    client.update_document_content = AsyncMock(return_value="reddit_post_post123")
    client.smart_sync = AsyncMock(return_value="reddit_post_post123")
    client.health_check = AsyncMock(return_value=True)
    return client
