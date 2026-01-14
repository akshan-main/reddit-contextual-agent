"""Tests for data models."""

from datetime import datetime, timedelta, timezone

from reddit_agent.models import PostStatus, RedditPost


class TestRedditComment:
    """Tests for RedditComment model."""

    def test_create_comment(self, sample_comment):
        """Test creating a comment."""
        assert sample_comment.id == "comment123"
        assert sample_comment.author == "test_user"
        assert sample_comment.score == 42

    def test_computed_permalink(self, sample_comment):
        """Test computed permalink property."""
        permalink = sample_comment.permalink
        assert "comment123" in permalink
        assert "reddit.com" in permalink


class TestRedditPost:
    """Tests for RedditPost model."""

    def test_create_post(self, sample_post):
        """Test creating a post."""
        assert sample_post.id == "post123"
        assert sample_post.subreddit == "contextengineering"
        assert sample_post.score == 150
        assert len(sample_post.comments) == 1

    def test_full_url(self, sample_post):
        """Test full_url computed property."""
        assert "reddit.com" in sample_post.full_url
        assert sample_post.permalink in sample_post.full_url

    def test_age_days(self, sample_post):
        """Test age_days computed property."""
        # Post was created in the past, so age should be positive
        assert sample_post.age_days > 0

    def test_should_update_within_window(self):
        """Test should_update returns True for recent posts."""
        post = RedditPost(
            id="new_post",
            subreddit="test",
            author="author",
            title="New Post",
            url="https://reddit.com/test",
            permalink="/r/test/new_post",
            score=10,
            upvote_ratio=0.9,
            num_comments=5,
            created_utc=datetime.now(timezone.utc) - timedelta(hours=12),
        )
        assert post.should_update(update_window_days=3) is True

    def test_should_update_outside_window(self):
        """Test should_update returns False for old posts."""
        post = RedditPost(
            id="old_post",
            subreddit="test",
            author="author",
            title="Old Post",
            url="https://reddit.com/test",
            permalink="/r/test/old_post",
            score=10,
            upvote_ratio=0.9,
            num_comments=5,
            created_utc=datetime.now(timezone.utc) - timedelta(days=5),
        )
        assert post.should_update(update_window_days=3) is False


class TestPostStatus:
    """Tests for PostStatus enum."""

    def test_status_values(self):
        """Test status enum values."""
        assert PostStatus.NEW.value == "new"
        assert PostStatus.UPDATING.value == "updating"
        assert PostStatus.FROZEN.value == "frozen"
