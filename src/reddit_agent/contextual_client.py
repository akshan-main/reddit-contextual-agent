"""Contextual AI client using the official SDK.

Uses the contextual-client SDK to:
1. Ingest Reddit posts as HTML documents
2. Update metadata without re-ingestion (score, num_comments)
3. Re-ingest only when comments content changes
"""

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import structlog
from contextual import ContextualAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import ContextualConfig
from .models import RedditPost

logger = structlog.get_logger()

# Pacific timezone
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def format_datetime_dual(dt: datetime) -> str:
    """Format datetime showing both Pacific and UTC for user queries."""
    # Ensure we have UTC
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)

    # Convert to Pacific
    dt_pacific = dt_utc.astimezone(PACIFIC_TZ)

    pacific_str = dt_pacific.strftime("%b %d, %Y at %I:%M %p %Z")
    utc_str = dt_utc.strftime("%Y-%m-%d %H:%M UTC")

    return f"{pacific_str} ({utc_str})"


class ContextualClient:
    """
    Client for Contextual AI datastore operations.

    Ingestion strategy:
    - Posts are converted to HTML for full-text indexing
    - Metadata (score, subreddit, etc.) is set separately for filtering
    - Metadata can be updated without re-ingesting the document
    - Re-ingest only when comments content changes significantly
    """

    def __init__(self, config: ContextualConfig):
        self.config = config
        self._client: ContextualAI | None = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self) -> None:
        """Initialize the Contextual AI client."""
        self._client = ContextualAI(api_key=self.config.api_key)
        logger.info("contextual_client_connected", datastore_id=self.config.datastore_id)

    async def close(self) -> None:
        """Close the client."""
        self._client = None
        logger.info("contextual_client_closed")

    def _post_to_html(self, post: RedditPost) -> str:
        """
        Convert a Reddit post to HTML for ingestion.

        Structure optimized for RAG retrieval:
        - Explicit labels for post vs comments (helps LLM understand context)
        - Full comment metadata (author, score, date, depth, edited, is_submitter)
        - All comments included (bots/deleted already filtered by scraper)
        """
        # Build comments section - ALL comments, sorted by score for readability
        comments_html = ""
        if post.comments:
            sorted_comments = sorted(post.comments, key=lambda x: x.score, reverse=True)
            comment_items = []
            for i, c in enumerate(sorted_comments, 1):
                # Escape HTML in user content
                body = c.body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                author = c.author.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

                # Build metadata tags
                tags = []
                if c.is_submitter:
                    tags.append("OP")  # Original Poster
                if c.edited:
                    tags.append("edited")
                if c.depth > 0:
                    tags.append(f"reply depth {c.depth}")
                tags_str = f" [{', '.join(tags)}]" if tags else ""

                posted_at = format_datetime_dual(c.created_utc)
                comment_items.append(f"""
        <div class="comment" data-comment-id="{c.id}" data-depth="{c.depth}">
            <p><strong>Comment #{i} by u/{author}</strong> ({c.score} points){tags_str}</p>
            <p><small>Posted: {posted_at} | <a href="{c.permalink}">Permalink</a></small></p>
            <blockquote>{body}</blockquote>
        </div>""")

            comments_html = f"""
    <section class="comments">
        <h2>Community Discussion ({len(sorted_comments)} comments)</h2>
        <p><em>These are replies to the post above. Users can reply at the Reddit link.</em></p>
        {"".join(comment_items)}
    </section>"""

        # Escape user content
        def escape_html(text: str) -> str:
            return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        title = escape_html(post.title)
        selftext = escape_html(post.selftext) if post.selftext else ""
        author = escape_html(post.author)

        # Link section for non-self posts
        link_html = ""
        if not post.is_self and post.url and post.url != post.full_url:
            link_html = f'<p><strong>External Link:</strong> <a href="{post.url}">{post.url}</a></p>'

        # Stats line
        stats_html = f"<p><strong>Stats:</strong> {post.score} upvotes, {post.num_comments} comments</p>"

        posted_at = format_datetime_dual(post.created_utc)

        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
</head>
<body>
    <article data-post-id="{post.id}" data-subreddit="{post.subreddit}">
        <header>
            <h1>[POST] {title}</h1>
            <p><strong>Subreddit:</strong> r/{post.subreddit}</p>
            <p><strong>Author:</strong> u/{author}</p>
            <p><strong>Posted:</strong> {posted_at}</p>
            <p><strong>Reddit URL:</strong> <a href="{post.full_url}">{post.full_url}</a></p>
            {link_html}
            {stats_html}
        </header>

        <section class="main-post-content">
            <h2>Post Content</h2>
            {f'<div class="post-body">{selftext}</div>' if selftext else "<p><em>Link post with no text.</em></p>"}
        </section>
        {comments_html}
    </article>
</body>
</html>"""
        return html

    def _get_metadata(self, post: RedditPost) -> dict:
        """
        Build metadata dict for a post.

        Metadata is used for:
        - Filtering queries (e.g., "posts from r/RAG")
        - Enriching context in responses
        - Attribution links
        """
        # Convert to Pacific for user-friendly date filtering
        # Handle both timezone-aware and naive datetimes
        if post.created_utc.tzinfo is None:
            dt_utc = post.created_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = post.created_utc.astimezone(timezone.utc)
        dt_pacific = dt_utc.astimezone(PACIFIC_TZ)

        md = {
            "url": post.full_url,
            "subreddit": post.subreddit,
            "author": post.author,
            "title": post.title,
            "score": int(post.score) if post.score is not None else 0,
            "num_comments": int(post.num_comments) if post.num_comments is not None else 0,
            "upvote_ratio_bp": int(round(float(post.upvote_ratio) * 10000)) if post.upvote_ratio is not None else None,
            "created_utc": dt_utc.isoformat(),
            "created_pacific": dt_pacific.isoformat(),
            "date_pacific": dt_pacific.strftime("%Y-%m-%d"),
            "post_id": post.id,
            "is_self": bool(post.is_self),
        }

        # Only include optional fields if they have values
        if not post.is_self and post.url and post.url != post.full_url:
            md["external_url"] = post.url

        if post.link_flair_text is not None:
            md["flair"] = post.link_flair_text

        # Filter out None values from final dict
        return {k: v for k, v in md.items() if v is not None}

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(5),
        before_sleep=lambda retry_state: logger.warning(
            "contextual_api_retry",
            attempt=retry_state.attempt_number,
        ),
    )
    async def ingest_document(self, post: RedditPost) -> str:
        """
        Ingest a Reddit post as an HTML document.

        1. Uploads HTML content for full-text indexing
        2. Sets metadata for filtering and context enrichment

        Returns the document ID.
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        html_content = self._post_to_html(post)
        doc_id = f"reddit_post_{post.id}"

        html_bytes = html_content.encode("utf-8")
        file_tuple = (f"{doc_id}.html", html_bytes, "text/html")

        logger.info(
            "ingesting_document",
            document_id=doc_id,
            subreddit=post.subreddit,
            title=post.title[:50],
            size_bytes=len(html_bytes),
        )

        loop = asyncio.get_event_loop()

        # Step 1: Ingest the HTML document
        result = await loop.run_in_executor(
            None,
            lambda: self._client.datastores.documents.ingest(
                datastore_id=self.config.datastore_id,
                file=file_tuple,
            ),
        )

        # Get the actual document ID from API response
        result_id = getattr(result, "id", None)
        if result_id is None:
            logger.error(
                "ingest_response_missing_id",
                post_id=post.id,
                result_type=type(result).__name__,
                result_attrs=str(dir(result)),
            )
            result_id = doc_id  # Fallback

        logger.info(
            "document_ingested",
            post_id=post.id,
            document_id=result_id,
            id_source="api_response" if result_id != doc_id else "fallback",
        )

        ok = await self.set_metadata(result_id, post)
        if not ok:
            logger.error("metadata_failed_after_ingest", document_id=result_id, post_id=post.id)

        logger.info(
            "document_ingested",
            document_id=doc_id,
            result_id=result_id,
        )

        return result_id

    async def set_metadata(self, document_id: str, post: RedditPost) -> bool:
        """
        Set or update metadata on a document.

        This can be called without re-ingesting the document content.
        Useful for updating score/num_comments as the post evolves.
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        metadata = self._get_metadata(post)
        if not metadata:
            logger.info("metadata_empty_skipping", document_id=document_id, post_id=post.id)
            return True

        logger.info(
            "setting_metadata",
            document_id=document_id,
            subreddit=post.subreddit,
            score=post.score,
            num_comments=post.num_comments,
        )

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.datastores.documents.set_metadata(
                    datastore_id=self.config.datastore_id,
                    document_id=document_id,
                    custom_metadata=metadata,
                ),
            )
            logger.info("metadata_updated", document_id=document_id)
            return True
        except Exception as e:
            logger.error("set_metadata_failed", document_id=document_id, error=str(e))
            return False

    @retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(3),
    )
    async def update_document(self, post: RedditPost, document_id: str) -> str:
        """
        Update a document by re-ingesting it.

        Used when comments content has changed significantly.
        Deletes the old document and re-ingests with new content.
        """
        if not self._client:
            raise RuntimeError("Client not connected")

        logger.info(
            "updating_document",
            document_id=document_id,
            post_id=post.id,
        )

        # Delete existing document first
        try:
            await self.delete_document(document_id)
        except Exception as e:
            logger.warning("delete_before_update_failed", error=str(e))

        # Re-ingest with updated content
        return await self.ingest_document(post)

    async def delete_document(self, document_id: str) -> bool:
        """Delete a document from the datastore."""
        if not self._client:
            raise RuntimeError("Client not connected")

        logger.info("deleting_document", document_id=document_id)

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.datastores.documents.delete(
                    datastore_id=self.config.datastore_id,
                    document_id=document_id,
                ),
            )
            logger.info("document_deleted", document_id=document_id)
            return True
        except Exception as e:
            logger.warning("delete_failed", document_id=document_id, error=str(e))
            return False

    async def smart_sync(
        self,
        post: RedditPost,
        existing_doc_id: str | None = None,
        content_changed: bool = False,
    ) -> str:
        """
        Sync a post to Contextual AI.

        Strategy:
        - New post: Ingest (HTML + metadata)
        - Existing, content changed: Re-ingest (delete + ingest)
        - Existing, content same: Skip (nothing to update)

        Note: Metadata-only updates (for score/num_comments changes) are handled
        at the pipeline level via set_metadata(). The HTML stats line becomes a
        point-in-time snapshot, while metadata always reflects current values.
        """
        if not existing_doc_id:
            # New document
            return await self.ingest_document(post)

        if content_changed:
            # Comments changed - re-ingest to update indexed content
            return await self.update_document(post, existing_doc_id)

        # Content same - no action needed
        logger.debug("content_unchanged_skipping", post_id=post.id)
        return existing_doc_id

    async def health_check(self) -> bool:
        """Check if the API is accessible."""
        if not self._client:
            return False

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: self._client.datastores.list())
            return True
        except Exception as e:
            logger.error("health_check_failed", error=str(e))
            return False
