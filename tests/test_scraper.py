"""Tests for Reddit scraper."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from reddit_agent.scraper import EXCLUDED_AUTHORS, RedditScraper


class TestExcludedAuthors:
    """Tests for author filtering."""

    def test_automoderator_excluded(self):
        """Test that AutoModerator is in excluded list."""
        assert "AutoModerator" in EXCLUDED_AUTHORS
        assert "automoderator" in EXCLUDED_AUTHORS

    def test_deleted_excluded(self):
        """Test that [deleted] is in excluded list."""
        assert "[deleted]" in EXCLUDED_AUTHORS


class TestRedditScraper:
    """Tests for RedditScraper class."""

    @patch("reddit_agent.scraper.praw.Reddit")
    def test_scraper_initialization(self, mock_reddit):
        """Test scraper initializes correctly."""
        from reddit_agent.config import Config

        config = Config()
        scraper = RedditScraper(config)

        assert scraper.config == config
        assert scraper.rate_limiter is not None
        mock_reddit.assert_called_once()

    @patch("reddit_agent.scraper.praw.Reddit")
    def test_submission_to_post_conversion(self, mock_reddit, mock_praw_submission):
        """Test conversion of PRAW submission to RedditPost."""
        from reddit_agent.config import Config

        config = Config()
        scraper = RedditScraper(config)

        post = scraper._submission_to_post(mock_praw_submission, [])

        assert post.id == "post123"
        assert post.subreddit == "contextengineering"
        assert post.title == "Test Post About Context Engineering"
        assert post.score == 150
        assert post.is_self is True

    @patch("reddit_agent.scraper.praw.Reddit")
    def test_submission_to_post_handles_deleted_author(self, mock_reddit):
        """Test handling of deleted author."""
        from reddit_agent.config import Config

        config = Config()
        scraper = RedditScraper(config)

        submission = MagicMock()
        submission.id = "test123"
        submission.subreddit.display_name = "test"
        submission.author = None  # Deleted author
        submission.title = "Test"
        submission.selftext = ""
        submission.url = "https://reddit.com/test"
        submission.permalink = "/r/test/test123"
        submission.score = 10
        submission.upvote_ratio = 0.9
        submission.num_comments = 0
        submission.created_utc = 1705312800.0
        submission.edited = False
        submission.link_flair_text = None
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

        post = scraper._submission_to_post(submission, [])

        assert post.author == "[deleted]"


class TestRateLimiter:
    """Tests for rate limiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_acquire(self):
        """Test rate limiter acquire."""
        from reddit_agent.scraper import RateLimiter

        limiter = RateLimiter(requests_per_minute=60, min_delay=0.1)
        await limiter.acquire()

        assert limiter.last_request_time > 0

    def test_rate_limiter_report_success(self):
        """Test reporting success reduces error count."""
        from reddit_agent.scraper import RateLimiter

        limiter = RateLimiter(requests_per_minute=60, min_delay=0.1)
        limiter._consecutive_errors = 3
        limiter.report_success()

        assert limiter._consecutive_errors == 2

    def test_rate_limiter_report_error(self):
        """Test reporting error increases count."""
        from reddit_agent.scraper import RateLimiter

        limiter = RateLimiter(requests_per_minute=60, min_delay=0.1)
        limiter.report_error()

        assert limiter._consecutive_errors == 1

    def test_rate_limiter_report_rate_limit_error(self):
        """Test rate limit error sets backoff."""
        from reddit_agent.scraper import RateLimiter

        limiter = RateLimiter(requests_per_minute=60, min_delay=0.1)
        limiter.report_error(is_rate_limit=True)

        assert limiter._consecutive_errors == 1
        assert limiter._backoff_until > 0
