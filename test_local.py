#!/usr/bin/env python3
"""
Run the full pipeline locally without Contextual AI ingestion.
Tests: Reddit scrape -> Supabase tracking -> update cycle -> freeze logic

Usage: python test_local.py
"""

import asyncio
import os
import sys
from unittest.mock import AsyncMock

from dotenv import load_dotenv

load_dotenv()


def check_env():
    """Check required environment variables."""
    required = [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "CONTEXTUAL_API_KEY",
        "CONTEXTUAL_DATASTORE_ID",
        "SUPABASE_CONNECTION_STRING",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print("Missing env vars:", ", ".join(missing))
        print("Copy .env.example to .env and fill in credentials.")
        sys.exit(1)
    print("[OK] Environment variables set")


async def run_pipeline_without_ingestion():
    """Run the actual pipeline but mock Contextual AI calls."""
    from reddit_agent.config import load_config
    from reddit_agent.logging_config import setup_logging
    from reddit_agent.pipeline import Pipeline

    setup_logging(level="INFO")

    config = load_config()

    print(f"\nRunning pipeline:")
    print(f"  Subreddits: {', '.join(config.scraper.subreddits)}")
    print(f"  Time window: {config.scraper.time_window_hours}h")
    print(f"  Refresh at count: {config.scraper.refresh_at_count}")
    print(f"  Freeze at count: {config.scraper.freeze_at_count}")

    async with Pipeline(config) as pipeline:
        # Mock Contextual AI client - skip actual ingestion
        pipeline.contextual.ingest_document = AsyncMock(
            side_effect=lambda post: f"mock_doc_{post.id}"
        )
        pipeline.contextual.set_metadata = AsyncMock(return_value=True)
        pipeline.contextual.smart_sync = AsyncMock(
            side_effect=lambda post, **kwargs: f"mock_doc_{post.id}"
        )
        pipeline.contextual.health_check = AsyncMock(return_value=True)

        # Run the full pipeline
        stats = await pipeline.run()

        # Get final database stats
        db_stats = await pipeline.db.get_stats()

    return stats, db_stats


async def main():
    print("=" * 60)
    print("Reddit Contextual Agent - Local Test (Mocked Ingestion)")
    print("=" * 60)

    check_env()

    try:
        stats, db_stats = await run_pipeline_without_ingestion()

        print("\n" + "=" * 60)
        print("PIPELINE RESULTS")
        print("=" * 60)
        print(f"Posts scraped:       {stats.posts_scraped}")
        print(f"New posts:           {stats.new_posts}")
        print(f"Documents ingested:  {stats.documents_ingested} (mocked)")
        print(f"Re-ingested:         {stats.documents_reingested} (mocked)")
        print(f"Skipped (no change): {stats.skipped_unchanged}")
        print(f"Frozen:              {stats.frozen_posts}")
        print(f"Errors:              {stats.sync_errors}")

        if stats.by_subreddit:
            print("\nBy subreddit:")
            for sub, data in stats.by_subreddit.items():
                print(f"  r/{sub}: {data.get('scraped', 0)} scraped, {data.get('new', 0)} new")

        print("\n" + "-" * 60)
        print("DATABASE STATE")
        print("-" * 60)
        print(f"Total tracked:       {db_stats['total_tracked']}")
        print(f"By status:           {db_stats['by_status']}")
        print(f"Total comments:      {db_stats['total_comments']}")
        print(f"Queue pending:       {db_stats['queue_pending']}")

        print("\n" + "=" * 60)
        if stats.sync_errors == 0:
            print("SUCCESS - Pipeline works correctly!")
        else:
            print(f"WARNING - {stats.sync_errors} errors occurred")
        print("=" * 60)

    except Exception as e:
        print(f"\n[FAILED] {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
