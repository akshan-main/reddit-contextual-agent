"""Database protocol for tracking Reddit posts.

This defines the interface that all database backends must implement.
"""

import hashlib
import json
from typing import Protocol

from ..models import RedditPost, TrackedPost


class DatabaseProtocol(Protocol):
    """Protocol for database implementations.

    Supported backends:
    - SQLiteDatabase: Local development
    - SupabaseDatabase: Production (PostgreSQL)
    """

    async def connect(self) -> None:
        """Connect to the database and initialize schema."""
        ...

    async def close(self) -> None:
        """Close the database connection."""
        ...

    async def get_tracked_post(self, post_id: str) -> TrackedPost | None:
        """Get tracking info for a post."""
        ...

    async def upsert_tracked_post(self, tracked: TrackedPost) -> None:
        """Insert or update tracking record."""
        ...

    async def save_post(self, post: RedditPost) -> None:
        """Save post and comments to local cache."""
        ...

    async def get_post(self, post_id: str) -> RedditPost | None:
        """Get a post from local cache."""
        ...

    async def get_posts_to_update(self, freeze_at_count: int = 2) -> list[TrackedPost]:
        """Get posts that need processing in the update cycle."""
        ...

    async def get_posts_to_freeze(self, freeze_at_count: int = 2) -> list[TrackedPost]:
        """Get posts ready to be frozen."""
        ...

    async def add_to_queue(self, post_id: str, subreddit: str, action: str = "ingest", priority: int = 0) -> None:
        """Add to retry queue."""
        ...

    async def get_queue_items(self, limit: int = 50) -> list[dict]:
        """Get pending queue items."""
        ...

    async def mark_queue_success(self, queue_id: int) -> None:
        """Remove successful queue item."""
        ...

    async def mark_queue_failure(self, queue_id: int, error: str) -> None:
        """Mark queue item as failed with backoff."""
        ...

    async def post_exists(self, post_id: str) -> bool:
        """Check if post is tracked."""
        ...

    async def get_stats(self) -> dict:
        """Get database stats."""
        ...

    async def cleanup_old_posts(self, days: int = 30) -> int:
        """Remove posts older than specified days."""
        ...

    async def get_posts_with_missing_hash(self) -> list[TrackedPost]:
        """Get posts that have no hash (need re-check)."""
        ...


def compute_content_hash(post: RedditPost) -> str:
    """
    Compute hash for detecting when to re-ingest.

    Triggers re-ingestion when:
    - Post title or body changes (edited)
    - ANY new comment is added or removed (tracked via comment IDs)
    - Comment content is edited (tracked via body hash)

    Excludes num_comments because:
    - It changes too frequently on Day 1 (volatile metadata)
    - Comment IDs already capture additions/removals
    - Would cause unnecessary re-ingestion churn

    Includes:
    - title, selftext (post content)
    - all comment IDs (sorted, detect additions/removals)
    - comment body hashes (detect edits)
    """
    # Get ALL comment data (sorted by ID for deterministic hash)
    sorted_comments = sorted(post.comments, key=lambda c: c.id)
    comment_data = [
        {
            "id": c.id,
            "body_hash": hashlib.md5(c.body[:500].encode()).hexdigest()[:8],
        }
        for c in sorted_comments
    ]

    content = json.dumps(
        {
            "title": post.title,
            "selftext": post.selftext[:2000] if post.selftext else "",
            "comments": comment_data,
        },
        sort_keys=True,
    )
    return hashlib.sha256(content.encode()).hexdigest()[:16]
