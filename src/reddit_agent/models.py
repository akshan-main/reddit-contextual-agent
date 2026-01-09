"""Data models for Reddit posts and comments."""

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field, computed_field


def _utc_now() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


class PostStatus(str, Enum):
    """Status of a post in our tracking system."""

    NEW = "new"
    UPDATING = "updating"
    FROZEN = "frozen"  # Past update window, no longer updated


class RedditComment(BaseModel):
    """A Reddit comment."""

    id: str
    author: str
    body: str
    score: int
    created_utc: datetime
    parent_id: str
    is_submitter: bool = False
    edited: bool = False
    depth: int = 0

    @computed_field
    @property
    def permalink(self) -> str:
        return f"https://reddit.com/comments/{self.parent_id.split('_')[-1]}/_/{self.id}"


class RedditPost(BaseModel):
    """A Reddit post with all metadata."""

    # Core identifiers
    id: str
    subreddit: str
    author: str

    # Content
    title: str
    selftext: str = ""
    url: str
    permalink: str

    # Metadata
    score: int
    upvote_ratio: float
    num_comments: int
    created_utc: datetime
    edited: bool = False

    # Flair and awards
    link_flair_text: str | None = None
    link_flair_css_class: str | None = None
    total_awards_received: int = 0

    # Post type flags
    is_self: bool = True
    is_video: bool = False
    is_original_content: bool = False
    over_18: bool = False
    spoiler: bool = False
    stickied: bool = False
    locked: bool = False
    archived: bool = False

    # Media
    thumbnail: str | None = None
    media_url: str | None = None

    # Comments (populated separately)
    comments: list[RedditComment] = Field(default_factory=list)

    # Tracking metadata (added by our system)
    scraped_at: datetime = Field(default_factory=_utc_now)
    last_updated: datetime = Field(default_factory=_utc_now)
    update_count: int = 0

    @computed_field
    @property
    def full_url(self) -> str:
        return f"https://reddit.com{self.permalink}"

    @computed_field
    @property
    def age_days(self) -> float:
        now = datetime.now(timezone.utc)
        created = self.created_utc if self.created_utc.tzinfo else self.created_utc.replace(tzinfo=timezone.utc)
        return (now - created).total_seconds() / 86400

    def should_update(self, update_window_days: int = 3) -> bool:
        """Check if this post should still be updated."""
        return self.age_days <= update_window_days

    def to_document(self) -> dict:
        """
        Convert to a document format suitable for Contextual AI.

        Note: This method is not used in the main pipeline.
        The pipeline uses contextual_client._post_to_html() for HTML ingestion.
        """
        # Build comment text - top 20 by score
        comments_text = ""
        if self.comments:
            sorted_comments = sorted(self.comments, key=lambda c: c.score, reverse=True)[:20]
            if sorted_comments:
                comments_text = "\n\n## Top Comments\n\n"
                for comment in sorted_comments:
                    comments_text += f"**u/{comment.author}** (score: {comment.score}):\n{comment.body}\n\n---\n\n"

        # Main document content
        body_content = self.selftext if self.selftext else "[Link post - no text content]"

        # Include the external URL for link posts
        link_info = ""
        if not self.is_self and self.url and self.url != self.full_url:
            link_info = f"\n**External Link:** {self.url}\n"

        content = f"""# {self.title}

**Subreddit:** r/{self.subreddit}
**Author:** u/{self.author}
**Score:** {self.score} (upvote ratio: {self.upvote_ratio:.0%})
**Comments:** {self.num_comments}
**Posted:** {self.created_utc.strftime("%Y-%m-%d %H:%M UTC")}
**URL:** {self.full_url}
{link_info}
## Content

{body_content}
{comments_text}
"""

        return {
            "document_id": f"reddit_post_{self.id}",
            "content": content,
            "metadata": {
                "source": "reddit",
                "type": "post",
                "subreddit": self.subreddit,
                "post_id": self.id,
                "author": self.author,
                "title": self.title,
                "score": self.score,
                "upvote_ratio": self.upvote_ratio,
                "num_comments": self.num_comments,
                "created_utc": int(
                    (
                        self.created_utc if self.created_utc.tzinfo else self.created_utc.replace(tzinfo=timezone.utc)
                    ).timestamp()
                ),
                "url": self.full_url,
                "external_url": self.url if not self.is_self else None,
                "is_self": self.is_self,
                "flair": self.link_flair_text,
                "scraped_at": self.scraped_at.isoformat(),
                "last_updated": self.last_updated.isoformat(),
            },
        }


class TrackedPost(BaseModel):
    """Tracking record for a post in our database."""

    post_id: str
    subreddit: str
    created_utc: datetime
    first_scraped: datetime
    last_updated: datetime
    update_count: int = 0
    status: PostStatus = PostStatus.NEW
    contextual_doc_id: str | None = None
    content_hash: str = ""  # For detecting actual changes
