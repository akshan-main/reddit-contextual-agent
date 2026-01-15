"""Supabase (PostgreSQL) database for tracking ingested posts.

Used for production deployment. Connects directly to Supabase's PostgreSQL instance.
"""

from datetime import datetime, timedelta, timezone

import asyncpg
import structlog

from ..models import PostStatus, RedditComment, RedditPost, TrackedPost
from .base import compute_content_hash

logger = structlog.get_logger()


class SupabaseDatabase:
    """Supabase PostgreSQL database for tracking Reddit posts.

    Uses asyncpg for direct PostgreSQL connection to Supabase.
    Same interface as SQLiteDatabase for drop-in replacement.
    """

    def __init__(self, connection_string: str):
        """Initialize with Supabase connection string.

        Args:
            connection_string: PostgreSQL connection string from Supabase dashboard.
                Format: postgresql://user:password@host:port/database
        """
        self.connection_string = connection_string
        self._pool: asyncpg.Pool | None = None

    # Re-export compute_content_hash as a static method for compatibility
    compute_content_hash = staticmethod(compute_content_hash)

    async def connect(self) -> None:
        """Connect to the database and create schema."""
        self._pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=1,
            max_size=5,
            # Disable prepared statements for pgbouncer compatibility (Supabase pooler)
            statement_cache_size=0,
        )
        await self._init_schema()
        logger.info("database_connected", backend="supabase")

    async def close(self) -> None:
        """Close the database connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _init_schema(self) -> None:
        """Initialize database schema."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                -- Track which posts have been ingested
                CREATE TABLE IF NOT EXISTS tracked_posts (
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT NOT NULL,
                    created_utc TIMESTAMPTZ NOT NULL,
                    first_scraped TIMESTAMPTZ NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    update_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'new',
                    contextual_doc_id TEXT,
                    content_hash TEXT DEFAULT ''
                );

                -- Cache post data
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    subreddit TEXT NOT NULL,
                    author TEXT NOT NULL,
                    title TEXT NOT NULL,
                    selftext TEXT DEFAULT '',
                    url TEXT NOT NULL,
                    permalink TEXT NOT NULL,
                    score INTEGER DEFAULT 0,
                    upvote_ratio REAL DEFAULT 0.0,
                    num_comments INTEGER DEFAULT 0,
                    created_utc TIMESTAMPTZ NOT NULL,
                    edited BOOLEAN DEFAULT FALSE,
                    link_flair_text TEXT,
                    is_self BOOLEAN DEFAULT TRUE,
                    scraped_at TIMESTAMPTZ NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    update_count INTEGER DEFAULT 0
                );

                -- Cache comments
                CREATE TABLE IF NOT EXISTS comments (
                    id TEXT PRIMARY KEY,
                    post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                    author TEXT NOT NULL,
                    body TEXT NOT NULL,
                    score INTEGER DEFAULT 0,
                    created_utc TIMESTAMPTZ NOT NULL,
                    parent_id TEXT NOT NULL,
                    is_submitter BOOLEAN DEFAULT FALSE,
                    depth INTEGER DEFAULT 0
                );

                -- Retry queue
                CREATE TABLE IF NOT EXISTS scrape_queue (
                    id SERIAL PRIMARY KEY,
                    post_id TEXT NOT NULL,
                    subreddit TEXT NOT NULL,
                    action TEXT NOT NULL DEFAULT 'ingest',
                    priority INTEGER DEFAULT 0,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 5,
                    last_error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    scheduled_for TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(post_id, action)
                );

                -- Indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_tracked_status ON tracked_posts (status, created_utc);
                CREATE INDEX IF NOT EXISTS idx_tracked_subreddit ON tracked_posts (subreddit);
                CREATE INDEX IF NOT EXISTS idx_tracked_first_scraped ON tracked_posts (first_scraped);
                CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts (subreddit, created_utc);
                CREATE INDEX IF NOT EXISTS idx_comments_post ON comments (post_id);
                CREATE INDEX IF NOT EXISTS idx_queue_scheduled ON scrape_queue (scheduled_for)
                    WHERE attempts < max_attempts;
            """)

    async def get_tracked_post(self, post_id: str) -> TrackedPost | None:
        """Get tracking info for a post."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tracked_posts WHERE post_id = $1", post_id)
            if row:
                return TrackedPost(
                    post_id=row["post_id"],
                    subreddit=row["subreddit"],
                    created_utc=row["created_utc"],
                    first_scraped=row["first_scraped"],
                    last_updated=row["last_updated"],
                    update_count=row["update_count"],
                    status=PostStatus(row["status"]),
                    contextual_doc_id=row["contextual_doc_id"],
                    content_hash=row["content_hash"] or "",
                )
        return None

    async def upsert_tracked_post(self, tracked: TrackedPost) -> None:
        """Insert or update tracking record."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tracked_posts
                    (post_id, subreddit, created_utc, first_scraped, last_updated,
                     update_count, status, contextual_doc_id, content_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT(post_id) DO UPDATE SET
                    last_updated = EXCLUDED.last_updated,
                    update_count = EXCLUDED.update_count,
                    status = EXCLUDED.status,
                    contextual_doc_id = COALESCE(EXCLUDED.contextual_doc_id, tracked_posts.contextual_doc_id),
                    content_hash = EXCLUDED.content_hash
                """,
                tracked.post_id,
                tracked.subreddit,
                tracked.created_utc,
                tracked.first_scraped,
                tracked.last_updated,
                tracked.update_count,
                tracked.status.value,
                tracked.contextual_doc_id,
                tracked.content_hash,
            )

    async def save_post(self, post: RedditPost) -> None:
        """Save post and comments to cache."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO posts
                        (id, subreddit, author, title, selftext, url, permalink,
                         score, upvote_ratio, num_comments, created_utc, edited,
                         link_flair_text, is_self, scraped_at, last_updated, update_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    ON CONFLICT(id) DO UPDATE SET
                        score = EXCLUDED.score,
                        upvote_ratio = EXCLUDED.upvote_ratio,
                        num_comments = EXCLUDED.num_comments,
                        last_updated = EXCLUDED.last_updated,
                        update_count = EXCLUDED.update_count
                    """,
                    post.id,
                    post.subreddit,
                    post.author,
                    post.title,
                    post.selftext,
                    post.url,
                    post.permalink,
                    post.score,
                    post.upvote_ratio,
                    post.num_comments,
                    post.created_utc,
                    post.edited,
                    post.link_flair_text,
                    post.is_self,
                    post.scraped_at,
                    post.last_updated,
                    post.update_count,
                )

                # Replace comments
                await conn.execute("DELETE FROM comments WHERE post_id = $1", post.id)
                if post.comments:
                    await conn.executemany(
                        """
                        INSERT INTO comments
                            (id, post_id, author, body, score, created_utc, parent_id, is_submitter, depth)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        [
                            (
                                c.id,
                                post.id,
                                c.author,
                                c.body,
                                c.score,
                                c.created_utc,
                                c.parent_id,
                                c.is_submitter,
                                c.depth,
                            )
                            for c in post.comments
                        ],
                    )

    async def get_post(self, post_id: str) -> RedditPost | None:
        """Get a post from cache."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM posts WHERE id = $1", post_id)
            if not row:
                return None

            comment_rows = await conn.fetch("SELECT * FROM comments WHERE post_id = $1 ORDER BY score DESC", post_id)

        comments = [
            RedditComment(
                id=cr["id"],
                author=cr["author"],
                body=cr["body"],
                score=cr["score"],
                created_utc=cr["created_utc"],
                parent_id=cr["parent_id"],
                is_submitter=cr["is_submitter"],
                depth=cr["depth"],
            )
            for cr in comment_rows
        ]

        return RedditPost(
            id=row["id"],
            subreddit=row["subreddit"],
            author=row["author"],
            title=row["title"],
            selftext=row["selftext"] or "",
            url=row["url"],
            permalink=row["permalink"],
            score=row["score"],
            upvote_ratio=row["upvote_ratio"],
            num_comments=row["num_comments"],
            created_utc=row["created_utc"],
            edited=row["edited"],
            link_flair_text=row["link_flair_text"],
            is_self=row["is_self"],
            comments=comments,
            scraped_at=row["scraped_at"],
            last_updated=row["last_updated"],
            update_count=row["update_count"],
        )

    async def get_posts_to_update(self, freeze_at_count: int = 2) -> list[TrackedPost]:
        """Get posts that need processing in the update cycle."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM tracked_posts
                WHERE status != $1
                  AND update_count < $2
                ORDER BY update_count ASC, first_scraped ASC
                """,
                PostStatus.FROZEN.value,
                freeze_at_count,
            )

        return [
            TrackedPost(
                post_id=row["post_id"],
                subreddit=row["subreddit"],
                created_utc=row["created_utc"],
                first_scraped=row["first_scraped"],
                last_updated=row["last_updated"],
                update_count=row["update_count"],
                status=PostStatus(row["status"]),
                contextual_doc_id=row["contextual_doc_id"],
                content_hash=row["content_hash"] or "",
            )
            for row in rows
        ]

    async def get_posts_to_freeze(self, freeze_at_count: int = 2) -> list[TrackedPost]:
        """Get posts ready to be frozen."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM tracked_posts WHERE status != $1 AND update_count >= $2",
                PostStatus.FROZEN.value,
                freeze_at_count,
            )

        return [
            TrackedPost(
                post_id=row["post_id"],
                subreddit=row["subreddit"],
                created_utc=row["created_utc"],
                first_scraped=row["first_scraped"],
                last_updated=row["last_updated"],
                update_count=row["update_count"],
                status=PostStatus(row["status"]),
                contextual_doc_id=row["contextual_doc_id"],
                content_hash=row["content_hash"] or "",
            )
            for row in rows
        ]

    async def add_to_queue(self, post_id: str, subreddit: str, action: str = "ingest", priority: int = 0) -> None:
        """Add to retry queue."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO scrape_queue (post_id, subreddit, action, priority)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (post_id, action) DO UPDATE SET
                    priority = GREATEST(scrape_queue.priority, EXCLUDED.priority)
                """,
                post_id,
                subreddit,
                action,
                priority,
            )

    async def get_queue_items(self, limit: int = 50) -> list[dict]:
        """Get pending queue items."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM scrape_queue
                WHERE attempts < max_attempts AND scheduled_for <= NOW()
                ORDER BY priority DESC, created_at ASC
                LIMIT $1
                """,
                limit,
            )
            return [dict(row) for row in rows]

    async def mark_queue_success(self, queue_id: int) -> None:
        """Remove successful queue item."""
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM scrape_queue WHERE id = $1", queue_id)

    async def mark_queue_failure(self, queue_id: int, error: str) -> None:
        """Mark queue item as failed with backoff."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT attempts FROM scrape_queue WHERE id = $1", queue_id)
            attempts = row["attempts"] if row else 0

            delay = timedelta(minutes=5 * (2**attempts))  # 5, 10, 20, 40, 80 minutes
            next_retry = datetime.now(timezone.utc) + delay

            await conn.execute(
                """
                UPDATE scrape_queue
                SET attempts = attempts + 1, last_error = $1, scheduled_for = $2
                WHERE id = $3
                """,
                error,
                next_retry,
                queue_id,
            )

    async def post_exists(self, post_id: str) -> bool:
        """Check if post is tracked."""
        async with self._pool.acquire() as conn:
            result = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM tracked_posts WHERE post_id = $1)", post_id)
            return result

    async def get_stats(self) -> dict:
        """Get database stats."""
        async with self._pool.acquire() as conn:
            stats = {}

            stats["total_tracked"] = await conn.fetchval("SELECT COUNT(*) FROM tracked_posts")

            status_rows = await conn.fetch("SELECT status, COUNT(*) as cnt FROM tracked_posts GROUP BY status")
            stats["by_status"] = {row["status"]: row["cnt"] for row in status_rows}

            sub_rows = await conn.fetch("SELECT subreddit, COUNT(*) as cnt FROM tracked_posts GROUP BY subreddit")
            stats["by_subreddit"] = {row["subreddit"]: row["cnt"] for row in sub_rows}

            stats["total_comments"] = await conn.fetchval("SELECT COUNT(*) FROM comments")

            stats["queue_pending"] = await conn.fetchval(
                "SELECT COUNT(*) FROM scrape_queue WHERE attempts < max_attempts"
            )

            return stats

    async def cleanup_old_posts(self, days: int = 30) -> int:
        """Remove posts older than specified days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Get count first
                count = await conn.fetchval("SELECT COUNT(*) FROM tracked_posts WHERE first_scraped < $1", cutoff)

                if count > 0:
                    # Delete comments
                    await conn.execute(
                        """
                        DELETE FROM comments WHERE post_id IN (
                            SELECT post_id FROM tracked_posts WHERE first_scraped < $1
                        )
                        """,
                        cutoff,
                    )
                    # Delete tracked posts
                    await conn.execute("DELETE FROM tracked_posts WHERE first_scraped < $1", cutoff)
                    # Delete orphan posts
                    await conn.execute(
                        """
                        DELETE FROM posts WHERE id NOT IN (
                            SELECT post_id FROM tracked_posts
                        )
                        """
                    )
                    logger.info("cleanup_old_posts", removed=count, days=days)

                return count

    async def get_posts_with_missing_hash(self) -> list[TrackedPost]:
        """Get posts that have no hash (need re-check)."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM tracked_posts
                WHERE (content_hash IS NULL OR content_hash = '')
                  AND status != $1
                """,
                PostStatus.FROZEN.value,
            )

        return [
            TrackedPost(
                post_id=row["post_id"],
                subreddit=row["subreddit"],
                created_utc=row["created_utc"],
                first_scraped=row["first_scraped"],
                last_updated=row["last_updated"],
                update_count=row["update_count"],
                status=PostStatus(row["status"]),
                contextual_doc_id=row["contextual_doc_id"],
                content_hash="",
            )
            for row in rows
        ]

    async def delete_post(self, post_id: str) -> bool:
        """Delete a post from database (Reddit Data API compliance)."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM comments WHERE post_id = $1", post_id)
                await conn.execute("DELETE FROM posts WHERE id = $1", post_id)
                result = await conn.execute("DELETE FROM tracked_posts WHERE post_id = $1", post_id)
                deleted = result.split()[-1] != "0"
                if deleted:
                    logger.info("post_deleted_from_database", post_id=post_id)
                return deleted
