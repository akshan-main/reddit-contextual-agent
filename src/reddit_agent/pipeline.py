"""Main pipeline orchestrator for the Reddit agent with Supabase and queue-based processing."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import structlog

from .config import Config
from .contextual_client import ContextualClient
from .db import SupabaseDatabase
from .models import PostStatus, RedditPost, TrackedPost
from .scraper import RedditScraper

logger = structlog.get_logger()

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def _utc_now() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _pacific_today():
    """Return today's date in Pacific timezone."""
    return datetime.now(PACIFIC_TZ).date()


def _to_pacific_date(dt: datetime):
    """Convert a datetime to Pacific date."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PACIFIC_TZ).date()


@dataclass
class PipelineStats:
    """Statistics from a pipeline run."""

    started_at: datetime = field(default_factory=_utc_now)
    completed_at: datetime | None = None

    # Scraping stats
    posts_scraped: int = 0
    new_posts: int = 0
    updated_posts: int = 0
    frozen_posts: int = 0
    posts_deleted: int = 0  # Posts that were deleted/unavailable on Reddit

    # Sync stats
    documents_ingested: int = 0
    documents_reingested: int = 0  # Existing posts re-ingested on Day 3-4 refresh
    skipped_unchanged: int = 0  # Posts where count was just incremented (no API call)
    sync_errors: int = 0

    # Queue stats
    queued_for_retry: int = 0

    # By subreddit
    by_subreddit: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": ((self.completed_at - self.started_at).total_seconds() if self.completed_at else None),
            "posts_scraped": self.posts_scraped,
            "new_posts": self.new_posts,
            "updated_posts": self.updated_posts,
            "frozen_posts": self.frozen_posts,
            "posts_deleted": self.posts_deleted,
            "documents_ingested": self.documents_ingested,
            "documents_reingested": self.documents_reingested,
            "skipped_unchanged": self.skipped_unchanged,
            "sync_errors": self.sync_errors,
            "queued_for_retry": self.queued_for_retry,
            "by_subreddit": self.by_subreddit,
        }


class Pipeline:
    """
    Main pipeline orchestrator for scraping Reddit and syncing to Contextual AI.

    Key features:
    - PostgreSQL for reliable state management
    - Queue-based processing for guaranteed delivery (no posts skipped)
    - Smart sync: Ingest once, metadata-only updates after
    - 3-day update window before freezing posts

    Pipeline flow:
    1. Scrape new posts from all configured subreddits
    2. For each new post: ingest to Contextual AI (full content)
    3. For existing posts within update window: metadata-only update (cheap)
    4. Process retry queue for any failed operations
    5. Freeze posts past the update window
    """

    def __init__(self, config: Config):
        self.config = config
        self.db = SupabaseDatabase(config.supabase.connection_string)
        self.scraper = RedditScraper(config)
        self.contextual = ContextualClient(config.contextual)
        self.stats = PipelineStats()

    async def __aenter__(self):
        await self.db.connect()
        await self.contextual.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.db.close()
        await self.contextual.close()

    async def _handle_deleted_post(self, tracked: TrackedPost) -> None:
        """Remove deleted post from Contextual AI and database."""
        if tracked.contextual_doc_id:
            try:
                await self.contextual.delete_document(tracked.contextual_doc_id)
                logger.info(
                    "deleted_post_removed_from_datastore",
                    post_id=tracked.post_id,
                    doc_id=tracked.contextual_doc_id,
                )
            except Exception as e:
                logger.warning(
                    "delete_from_datastore_failed",
                    post_id=tracked.post_id,
                    error=str(e),
                )
        await self.db.delete_post(tracked.post_id)
        self.stats.posts_deleted += 1

    async def _process_new_post(self, post: RedditPost) -> str | None:
        """
        Process a newly scraped post.

        - If new: Ingest to Contextual AI (full content, triggers indexing)
        - If existing: Skip (will be handled by update logic)
        """
        existing = await self.db.get_tracked_post(post.id)

        if existing:
            # Post already exists, will be handled by update logic
            return existing.contextual_doc_id

        logger.info(
            "processing_new_post",
            post_id=post.id,
            subreddit=post.subreddit,
            title=post.title[:50],
        )

        # Save to local database first
        await self.db.save_post(post)

        # Ingest to Contextual AI (single ingestion per post)
        try:
            doc_id = await self.contextual.ingest_document(post)

            # Create tracking record
            # update_count starts at -1:
            # -1 (Day 1, scrape) -> 0 (Day 2, skip) -> 1 (Day 3, refresh) -> 2 (Day 4, freeze)
            tracked = TrackedPost(
                post_id=post.id,
                subreddit=post.subreddit,
                created_utc=post.created_utc,
                first_scraped=_utc_now(),
                last_updated=_utc_now(),
                update_count=-1,
                status=PostStatus.NEW,
                contextual_doc_id=doc_id,
                content_hash=self.db.compute_content_hash(post),
            )
            await self.db.upsert_tracked_post(tracked)

            self.stats.new_posts += 1
            self.stats.documents_ingested += 1

            return doc_id

        except Exception as e:
            logger.error(
                "ingestion_failed",
                post_id=post.id,
                error=str(e),
            )
            # Queue for retry - guaranteed processing
            await self.db.add_to_queue(
                post_id=post.id,
                subreddit=post.subreddit,
                action="ingest",
                priority=1,
            )
            self.stats.sync_errors += 1
            self.stats.queued_for_retry += 1
            return None

    async def _update_post(self, tracked: TrackedPost) -> bool:
        """
        Update post based on scrape count.

        Configurable via:
        - refresh_at_count: When to start refreshing (default 0 = Day 3)
        - freeze_at_count: When to stop tracking (default 2 = Day 4)
        - always_reingest_on_refresh: If False, only re-ingest when content changed

        Count progression example (defaults):
        -1 (Day 1, scrape) -> 0 (Day 2, skip) -> 1 (Day 3, refresh) -> 2 (Day 4, freeze)
        """
        refresh_at = self.config.scraper.refresh_at_count
        freeze_at = self.config.scraper.freeze_at_count
        always_reingest = self.config.scraper.always_reingest_on_refresh

        # Skip if already processed today (Pacific time, matches 8 AM Pacific run)
        if _to_pacific_date(tracked.last_updated) == _pacific_today():
            logger.debug("already_processed_today", post_id=tracked.post_id)
            return True

        logger.info(
            "processing_post_update",
            post_id=tracked.post_id,
            update_count=tracked.update_count,
            refresh_at=refresh_at,
            freeze_at=freeze_at,
        )

        # If count < refresh_at, just increment and skip (letting comments accumulate)
        if tracked.update_count < refresh_at:
            tracked.update_count += 1
            tracked.last_updated = _utc_now()
            await self.db.upsert_tracked_post(tracked)

            logger.debug(
                "incremented_count_skipping",
                post_id=tracked.post_id,
                new_count=tracked.update_count,
            )
            self.stats.skipped_unchanged += 1
            return True

        # Time for refresh check - fetch from Reddit
        logger.info(
            "refreshing_post",
            post_id=tracked.post_id,
            update_count=tracked.update_count,
        )

        # Refresh post data from Reddit
        try:
            post = await self.scraper.refresh_post(tracked.post_id)
        except Exception as e:
            logger.warning(
                "refresh_failed_queuing",
                post_id=tracked.post_id,
                error=str(e),
            )
            await self.db.add_to_queue(
                post_id=tracked.post_id,
                subreddit=tracked.subreddit,
                action="update",
                priority=0,
            )
            self.stats.queued_for_retry += 1
            return False

        # Handle deleted posts - remove from datastore and database
        if not post:
            logger.warning("post_deleted_or_unavailable", post_id=tracked.post_id)
            await self._handle_deleted_post(tracked)
            return True

        # Check if content actually changed
        new_hash = self.db.compute_content_hash(post)
        content_changed = new_hash != tracked.content_hash

        # Decide whether to re-ingest
        should_reingest = always_reingest or content_changed

        if not should_reingest:
            # Content unchanged - check if metadata changed
            old_post = await self.db.get_post(tracked.post_id)
            metadata_changed = False

            if old_post:
                metadata_changed = (
                    old_post.score != post.score
                    or old_post.num_comments != post.num_comments
                    or old_post.upvote_ratio != post.upvote_ratio
                )

            if metadata_changed and tracked.contextual_doc_id:
                # Metadata-only update (cheaper than re-ingesting)
                logger.info(
                    "metadata_only_update",
                    post_id=tracked.post_id,
                    old_score=old_post.score if old_post else None,
                    new_score=post.score,
                )

                success = await self.contextual.set_metadata(tracked.contextual_doc_id, post)
                if success:
                    # Save updated post with new metadata
                    post.update_count = tracked.update_count + 1
                    await self.db.save_post(post)

                    # Update tracking
                    tracked.update_count += 1
                    tracked.last_updated = _utc_now()
                    await self.db.upsert_tracked_post(tracked)
                    self.stats.updated_posts += 1
                    return True
                else:
                    logger.warning("metadata_update_failed", post_id=tracked.post_id)
                    # Fall through to skip behavior

            # No changes at all, just increment count
            logger.info(
                "no_changes_skipping_reingest",
                post_id=tracked.post_id,
                update_count=tracked.update_count,
            )
            tracked.update_count += 1
            tracked.last_updated = _utc_now()
            await self.db.upsert_tracked_post(tracked)
            self.stats.skipped_unchanged += 1
            return True

        # Save updated post locally
        post.update_count = tracked.update_count + 1
        await self.db.save_post(post)

        try:
            # Re-ingest with all accumulated comments and fresh metadata
            await self.contextual.smart_sync(
                post=post,
                existing_doc_id=tracked.contextual_doc_id,
                content_changed=True,
            )

            # Update tracking
            tracked.last_updated = _utc_now()
            tracked.update_count += 1
            tracked.status = PostStatus.UPDATING
            tracked.content_hash = new_hash
            await self.db.upsert_tracked_post(tracked)

            self.stats.updated_posts += 1
            self.stats.documents_reingested += 1

            logger.info(
                "post_refreshed",
                post_id=tracked.post_id,
                num_comments=post.num_comments,
                score=post.score,
                content_changed=content_changed,
            )
            return True

        except Exception as e:
            logger.error(
                "update_sync_failed",
                post_id=tracked.post_id,
                error=str(e),
            )
            await self.db.add_to_queue(
                post_id=tracked.post_id,
                subreddit=tracked.subreddit,
                action="update",
                priority=0,
            )
            self.stats.sync_errors += 1
            self.stats.queued_for_retry += 1
            return False

    async def _freeze_post(self, tracked: TrackedPost) -> None:
        """Freeze a post that's past the update window."""
        logger.info(
            "freezing_post",
            post_id=tracked.post_id,
            update_count=tracked.update_count,
        )

        tracked.status = PostStatus.FROZEN
        tracked.last_updated = _utc_now()
        await self.db.upsert_tracked_post(tracked)

        self.stats.frozen_posts += 1

    async def _process_queue(self) -> None:
        """Process the retry queue for failed operations."""
        logger.info("processing_retry_queue")

        queue_items = await self.db.get_queue_items(limit=50)

        if not queue_items:
            logger.info("queue_empty")
            return

        logger.info("queue_items_to_process", count=len(queue_items))

        for item in queue_items:
            try:
                if item["action"] == "ingest":
                    # Retry ingestion
                    post = await self.db.get_post(item["post_id"])
                    if post:
                        doc_id = await self.contextual.ingest_document(post)

                        tracked = await self.db.get_tracked_post(item["post_id"])
                        if tracked:
                            tracked.contextual_doc_id = doc_id
                            await self.db.upsert_tracked_post(tracked)

                        self.stats.documents_ingested += 1

                elif item["action"] == "update":
                    # Retry update
                    tracked = await self.db.get_tracked_post(item["post_id"])
                    if tracked:
                        await self._update_post(tracked)

                await self.db.mark_queue_success(item["id"])
                logger.info("queue_item_processed", queue_id=item["id"])

            except Exception as e:
                logger.warning(
                    "queue_item_failed",
                    queue_id=item["id"],
                    error=str(e),
                )
                await self.db.mark_queue_failure(item["id"], str(e))

            # Small delay between queue items
            await asyncio.sleep(1.0)

    async def scrape_and_process_new(self) -> None:
        """Scrape all subreddits and process new posts."""
        logger.info("starting_scrape_phase")

        posts, failed_items = await self.scraper.scrape_all_subreddits()
        self.stats.posts_scraped = len(posts)

        # Queue failed subreddits for retry
        for failed in failed_items:
            if failed.startswith("subreddit:"):
                subreddit = failed.split(":")[1]
                logger.warning("queueing_failed_subreddit", subreddit=subreddit)
                # We'll retry on next run

        # Track by subreddit
        for post in posts:
            if post.subreddit not in self.stats.by_subreddit:
                self.stats.by_subreddit[post.subreddit] = {"scraped": 0, "new": 0}
            self.stats.by_subreddit[post.subreddit]["scraped"] += 1

        # Process each post
        for post in posts:
            # Only process posts within the update window
            if post.should_update(self.config.scraper.update_window_days):
                # Check if this is a new post BEFORE processing
                existing_before = await self.db.get_tracked_post(post.id)
                was_new = existing_before is None

                result = await self._process_new_post(post)

                if result and was_new:
                    self.stats.by_subreddit[post.subreddit]["new"] += 1

        logger.info(
            "scrape_phase_complete",
            total_scraped=self.stats.posts_scraped,
            new_posts=self.stats.new_posts,
        )

    async def update_existing_posts(self) -> None:
        """Update posts within the update cycle."""
        logger.info(
            "starting_update_phase",
            refresh_at=self.config.scraper.refresh_at_count,
            freeze_at=self.config.scraper.freeze_at_count,
        )

        posts_to_update = await self.db.get_posts_to_update(freeze_at_count=self.config.scraper.freeze_at_count)

        logger.info("posts_to_update", count=len(posts_to_update))

        for tracked in posts_to_update:
            await self._update_post(tracked)
            # Small delay between updates
            await asyncio.sleep(0.5)

        logger.info(
            "update_phase_complete",
            updated=self.stats.updated_posts,
            reingested=self.stats.documents_reingested,
            skipped=self.stats.skipped_unchanged,
        )

    async def freeze_old_posts(self) -> None:
        """Freeze posts that have completed the update cycle."""
        logger.info("starting_freeze_phase")

        posts_to_freeze = await self.db.get_posts_to_freeze(freeze_at_count=self.config.scraper.freeze_at_count)

        logger.info("posts_to_freeze", count=len(posts_to_freeze))

        for tracked in posts_to_freeze:
            await self._freeze_post(tracked)

        logger.info(
            "freeze_phase_complete",
            frozen=self.stats.frozen_posts,
        )

    async def fix_missing_hashes(self) -> None:
        """Re-check and re-ingest posts with missing hash."""
        logger.info("checking_missing_hashes")

        posts_missing_hash = await self.db.get_posts_with_missing_hash()

        if not posts_missing_hash:
            logger.debug("no_missing_hashes")
            return

        logger.info("fixing_missing_hashes", count=len(posts_missing_hash))

        for tracked in posts_missing_hash:
            # Force re-check by treating as if content changed
            try:
                post = await self.scraper.refresh_post(tracked.post_id)
                if post:
                    new_hash = self.db.compute_content_hash(post)
                    await self.db.save_post(post)

                    # Re-ingest since hash was missing
                    await self.contextual.smart_sync(
                        post=post,
                        existing_doc_id=tracked.contextual_doc_id,
                        content_changed=True,
                    )

                    tracked.content_hash = new_hash
                    tracked.last_updated = _utc_now()
                    await self.db.upsert_tracked_post(tracked)

                    logger.info("fixed_missing_hash", post_id=tracked.post_id)
                    self.stats.documents_reingested += 1
                else:
                    # Post deleted - remove from datastore and database
                    await self._handle_deleted_post(tracked)

            except Exception as e:
                logger.warning("fix_hash_failed", post_id=tracked.post_id, error=str(e))

    async def cleanup(self) -> None:
        """Remove old posts from database (keeps tracking table clean)."""
        days = self.config.scraper.cleanup_after_days
        if days <= 0:
            return

        removed = await self.db.cleanup_old_posts(days=days)
        if removed > 0:
            logger.info("cleanup_complete", removed=removed, days=days)

    async def run(self) -> PipelineStats:
        """
        Run the complete pipeline.

        1. Process retry queue (handle previous failures)
        2. Fix posts with missing hashes
        3. Scrape new posts from all subreddits
        4. Update existing posts within the update window
        5. Freeze posts past the update window
        6. Process any new queue items from this run
        7. Cleanup old posts from Supabase (> 30 days default)
        """
        logger.info(
            "pipeline_starting",
            subreddits=self.config.scraper.subreddits,
            refresh_at=self.config.scraper.refresh_at_count,
            freeze_at=self.config.scraper.freeze_at_count,
        )

        self.stats = PipelineStats()

        try:
            # Phase 0: Process any pending retry items first
            await self._process_queue()
            # Phase 1: Fix posts with missing hashes
            await self.fix_missing_hashes()
            # Phase 2: Scrape and process new posts
            await self.scrape_and_process_new()
            # Phase 3: Update existing posts within window
            await self.update_existing_posts()
            # Phase 4: Freeze old posts
            await self.freeze_old_posts()
            # Phase 5: Process any new queue items from this run
            await self._process_queue()
            # Phase 6: Cleanup old posts (> 30 days default)
            await self.cleanup()

            self.stats.completed_at = _utc_now()

            # Log final stats
            db_stats = await self.db.get_stats()
            logger.info(
                "pipeline_complete",
                stats=self.stats.to_dict(),
                db_stats=db_stats,
            )

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

        return self.stats

    async def run_scrape_only(self) -> PipelineStats:
        """Run only the scraping phase (useful for testing)."""
        logger.info("running_scrape_only")

        self.stats = PipelineStats()
        await self.scrape_and_process_new()
        self.stats.completed_at = _utc_now()

        return self.stats

    async def run_update_only(self) -> PipelineStats:
        """Run only the update phase (useful for testing)."""
        logger.info("running_update_only")

        self.stats = PipelineStats()
        await self._process_queue()
        await self.update_existing_posts()
        await self.freeze_old_posts()
        self.stats.completed_at = _utc_now()

        return self.stats

    async def run_queue_only(self) -> PipelineStats:
        """Run only the queue processing (useful for retry)."""
        logger.info("running_queue_only")

        self.stats = PipelineStats()
        await self._process_queue()
        self.stats.completed_at = _utc_now()

        return self.stats


async def run_pipeline(config: Config | None = None) -> PipelineStats:
    """Convenience function to run the pipeline."""
    if config is None:
        from .config import load_config

        config = load_config()

    async with Pipeline(config) as pipeline:
        return await pipeline.run()
