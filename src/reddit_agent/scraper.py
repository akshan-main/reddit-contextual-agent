"""Reddit scraper using PRAW - fetches ALL posts from time window, not count-limited."""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import praw
import structlog
from praw.models import MoreComments
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

from .config import Config
from .models import RedditComment, RedditPost

logger = structlog.get_logger()

# Authors to exclude from comments (bots and deleted accounts)
EXCLUDED_AUTHORS = {
    "AutoModerator",
    "automoderator",
    "ModeratorBot",
    "BotDefense",
    "RemindMeBot",
    "SaveVideo",
    "vredditdownloader",
    "[deleted]",
}


class RateLimiter:
    """Adaptive rate limiter for Reddit API calls."""

    def __init__(self, requests_per_minute: int, min_delay: float):
        self.requests_per_minute = requests_per_minute
        self.min_delay = min_delay
        self.last_request_time = 0.0
        self._lock = asyncio.Lock()
        self._consecutive_errors = 0
        self._backoff_until = 0.0

    async def acquire(self) -> None:
        """Wait until we can make another request with adaptive backoff."""
        async with self._lock:
            now = time.time()

            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                logger.warning("rate_limit_backoff", wait_seconds=wait_time)
                await asyncio.sleep(wait_time)
                now = time.time()

            time_since_last = now - self.last_request_time
            adaptive_delay = self.min_delay * (1 + self._consecutive_errors * 0.5)
            wait_time = max(0, adaptive_delay - time_since_last)

            if wait_time > 0:
                logger.debug("rate_limit_wait", wait_seconds=wait_time)
                await asyncio.sleep(wait_time)

            self.last_request_time = time.time()

    def report_success(self) -> None:
        """Report a successful request to reduce backoff."""
        self._consecutive_errors = max(0, self._consecutive_errors - 1)

    def report_error(self, is_rate_limit: bool = False) -> None:
        """Report an error to increase backoff."""
        self._consecutive_errors += 1
        if is_rate_limit:
            self._backoff_until = time.time() + min(300, 60 * self._consecutive_errors)
            logger.warning(
                "rate_limit_hit",
                backoff_until=self._backoff_until,
                consecutive_errors=self._consecutive_errors,
            )


class RedditScraper:
    """
    Scraper for Reddit posts - fetches ALL posts from a time window.

    Key design: Uses time-based fetching, not count limits.
    For daily runs: gets all posts from last 26 hours (2hr overlap for safety).
    """

    def __init__(self, config: Config):
        self.config = config
        self.reddit = praw.Reddit(
            client_id=config.reddit.client_id,
            client_secret=config.reddit.client_secret,
            user_agent=config.reddit.user_agent,
        )
        self.rate_limiter = RateLimiter(
            requests_per_minute=config.scraper.requests_per_minute,
            min_delay=config.scraper.min_request_delay,
        )
        logger.info(
            "scraper_initialized",
            subreddits=config.scraper.subreddits,
            time_window_hours=config.scraper.time_window_hours,
        )

    @retry(
        retry=retry_if_exception_type((praw.exceptions.RedditAPIException, Exception)),
        wait=wait_random_exponential(multiplier=1, min=4, max=120),
        stop=stop_after_attempt(7),
        before_sleep=lambda retry_state: logger.warning(
            "reddit_api_retry",
            attempt=retry_state.attempt_number,
            exception=str(retry_state.outcome.exception()) if retry_state.outcome else None,
        ),
    )
    async def _fetch_all_posts_in_window(
        self,
        subreddit_name: str,
        hours: int = 26,
    ) -> list[praw.models.Submission]:
        """
        Fetch ALL posts from a subreddit within the time window.

        Uses 'new' feed (chronological) and keeps fetching until we hit
        posts older than our cutoff. This ensures we get EVERY post.
        """
        # Use timezone-aware UTC to avoid .timestamp() assuming local timezone
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff_timestamp = cutoff.timestamp()

        loop = asyncio.get_event_loop()
        all_posts = {}

        await self.rate_limiter.acquire()

        try:
            subreddit = await loop.run_in_executor(None, lambda: self.reddit.subreddit(subreddit_name))

            # Fetch from 'new' feed - this is chronological
            # Keep fetching until we hit posts older than our cutoff
            after = None
            batch_size = 100  # Reddit's max per request

            while True:
                await self.rate_limiter.acquire()

                posts = await loop.run_in_executor(
                    None, lambda: list(subreddit.new(limit=batch_size, params={"after": after} if after else {}))
                )
                self.rate_limiter.report_success()

                if not posts:
                    break

                if posts and after is None:
                    first_post_age_hours = (datetime.now(timezone.utc).timestamp() - posts[0].created_utc) / 3600
                    logger.debug(
                        "newest_post_age",
                        subreddit=subreddit_name,
                        post_id=posts[0].id,
                        age_hours=round(first_post_age_hours, 1),
                        cutoff_hours=hours,
                    )

                reached_cutoff = False
                for post in posts:
                    if post.created_utc < cutoff_timestamp:
                        reached_cutoff = True
                        break
                    all_posts[post.id] = post

                if reached_cutoff or len(posts) < batch_size:
                    break

                after = f"t3_{posts[-1].id}"

            # Also check 'hot' to catch any high-engagement posts we might have missed
            await self.rate_limiter.acquire()
            hot_posts = await loop.run_in_executor(None, lambda: list(subreddit.hot(limit=50)))
            self.rate_limiter.report_success()

            for post in hot_posts:
                if post.created_utc >= cutoff_timestamp:
                    all_posts[post.id] = post

            logger.info(
                "fetched_all_posts_in_window",
                subreddit=subreddit_name,
                hours=hours,
                total_posts=len(all_posts),
            )

            return list(all_posts.values())

        except praw.exceptions.RedditAPIException as e:
            if "RATELIMIT" in str(e).upper():
                self.rate_limiter.report_error(is_rate_limit=True)
            else:
                self.rate_limiter.report_error()
            raise

    @retry(
        retry=retry_if_exception_type((praw.exceptions.RedditAPIException, Exception)),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        stop=stop_after_attempt(5),
    )
    async def _fetch_post_by_id(self, post_id: str) -> praw.models.Submission | None:
        """Fetch a specific post by ID."""
        await self.rate_limiter.acquire()
        loop = asyncio.get_event_loop()

        try:
            submission = await loop.run_in_executor(None, lambda: self.reddit.submission(id=post_id))
            _ = await loop.run_in_executor(None, lambda: submission.title)
            self.rate_limiter.report_success()
            return submission
        except Exception as e:
            self.rate_limiter.report_error()
            logger.warning("fetch_post_failed", post_id=post_id, error=str(e))
            raise

    async def _fetch_comments(
        self,
        submission: praw.models.Submission,
        max_comments: int,
    ) -> list[RedditComment]:
        """Fetch comments for a submission, excluding bots."""
        if not self.config.scraper.include_comments:
            return []

        await self.rate_limiter.acquire()
        loop = asyncio.get_event_loop()

        try:
            await loop.run_in_executor(None, lambda: submission.comments.replace_more(limit=5))
            self.rate_limiter.report_success()
        except Exception as e:
            logger.warning("replace_more_failed", post_id=submission.id, error=str(e))

        comments = []
        try:
            comment_list = await loop.run_in_executor(None, lambda: submission.comments.list())
        except Exception as e:
            logger.warning("get_comments_failed", post_id=submission.id, error=str(e))
            return []

        for comment in comment_list:
            if isinstance(comment, MoreComments):
                continue

            try:
                author_name = str(comment.author) if comment.author else "[deleted]"

                # Only skip known bots, keep deleted comments for context
                if author_name.lower() in {a.lower() for a in EXCLUDED_AUTHORS}:
                    continue

                comments.append(
                    RedditComment(
                        id=comment.id,
                        author=author_name,
                        body=comment.body,
                        score=comment.score,
                        created_utc=datetime.fromtimestamp(comment.created_utc, tz=timezone.utc),
                        parent_id=comment.parent_id,
                        is_submitter=comment.is_submitter,
                        edited=bool(comment.edited),
                        depth=comment.depth,
                    )
                )

                if len(comments) >= max_comments:
                    break

            except Exception as e:
                logger.warning("comment_parse_error", error=str(e))
                continue

        return comments

    def _submission_to_post(
        self,
        submission: praw.models.Submission,
        comments: list[RedditComment],
    ) -> RedditPost:
        """Convert a PRAW submission to our RedditPost model."""
        media_url = None
        if hasattr(submission, "url") and submission.url:
            if any(submission.url.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webm"]):
                media_url = submission.url

        return RedditPost(
            id=submission.id,
            subreddit=submission.subreddit.display_name,
            author=str(submission.author) if submission.author else "[deleted]",
            title=submission.title,
            selftext=submission.selftext or "",
            url=submission.url,
            permalink=submission.permalink,
            score=submission.score,
            upvote_ratio=submission.upvote_ratio,
            num_comments=submission.num_comments,
            created_utc=datetime.fromtimestamp(submission.created_utc, tz=timezone.utc),
            edited=bool(submission.edited),
            link_flair_text=submission.link_flair_text,
            link_flair_css_class=submission.link_flair_css_class,
            total_awards_received=submission.total_awards_received,
            is_self=submission.is_self,
            is_video=submission.is_video,
            is_original_content=submission.is_original_content,
            over_18=submission.over_18,
            spoiler=submission.spoiler,
            stickied=submission.stickied,
            locked=submission.locked,
            archived=submission.archived,
            thumbnail=submission.thumbnail if submission.thumbnail != "self" else None,
            media_url=media_url,
            comments=comments,
            scraped_at=datetime.now(timezone.utc),
            last_updated=datetime.now(timezone.utc),
        )

    async def scrape_subreddit(
        self,
        subreddit_name: str,
        hours: int | None = None,
    ) -> list[RedditPost]:
        """
        Scrape ALL posts from a subreddit within the time window.

        Args:
            subreddit_name: Name of subreddit
            hours: Time window in hours (defaults to config.time_window_hours)
        """
        hours = hours or self.config.scraper.time_window_hours
        logger.info("scraping_subreddit", subreddit=subreddit_name, hours=hours)

        try:
            submissions = await self._fetch_all_posts_in_window(subreddit_name, hours)
        except Exception as e:
            logger.error(
                "subreddit_scrape_failed",
                subreddit=subreddit_name,
                error=str(e),
            )
            raise

        posts = []
        for submission in submissions:
            try:
                comments = await self._fetch_comments(
                    submission,
                    max_comments=self.config.scraper.max_comments,
                )
                post = self._submission_to_post(submission, comments)
                posts.append(post)

                logger.debug(
                    "scraped_post",
                    post_id=post.id,
                    title=post.title[:50],
                    score=post.score,
                    comments=len(comments),
                )
            except Exception as e:
                logger.warning(
                    "post_scrape_failed",
                    post_id=submission.id,
                    error=str(e),
                )
                continue

        logger.info(
            "subreddit_scrape_complete",
            subreddit=subreddit_name,
            posts_scraped=len(posts),
        )

        return posts

    async def scrape_all_subreddits(
        self,
        hours: int | None = None,
    ) -> tuple[list[RedditPost], list[str]]:
        """
        Scrape ALL posts from all configured subreddits within time window.

        Returns:
            Tuple of (all posts, failed subreddits)
        """
        hours = hours or self.config.scraper.time_window_hours
        all_posts = []
        failed_subreddits = []

        for subreddit in self.config.scraper.subreddits:
            try:
                posts = await self.scrape_subreddit(subreddit, hours)
                all_posts.extend(posts)
            except Exception as e:
                logger.error(
                    "subreddit_completely_failed",
                    subreddit=subreddit,
                    error=str(e),
                )
                failed_subreddits.append(subreddit)

            await asyncio.sleep(2.0)

        logger.info(
            "all_subreddits_scraped",
            total_posts=len(all_posts),
            subreddits_scraped=len(self.config.scraper.subreddits) - len(failed_subreddits),
            failed_subreddits=len(failed_subreddits),
        )

        return all_posts, failed_subreddits

    def _is_post_deleted(self, submission: praw.models.Submission) -> bool:
        """Check if a post has been deleted or removed."""
        if submission.author is None:
            return True
        if submission.selftext in ("[deleted]", "[removed]"):
            return True
        return False

    async def refresh_post(self, post_id: str) -> RedditPost | None:
        """Refresh a single post's data. Returns None if deleted/removed."""
        logger.debug("refreshing_post", post_id=post_id)

        try:
            submission = await self._fetch_post_by_id(post_id)
            if not submission:
                return None

            if self._is_post_deleted(submission):
                logger.info("post_deleted_detected", post_id=post_id)
                return None

            comments = await self._fetch_comments(
                submission,
                max_comments=self.config.scraper.max_comments,
            )

            post = self._submission_to_post(submission, comments)
            post.update_count += 1

            logger.info(
                "post_refreshed",
                post_id=post_id,
                score=post.score,
                comments=post.num_comments,
            )

            return post

        except Exception as e:
            logger.error("refresh_post_failed", post_id=post_id, error=str(e))
            raise
