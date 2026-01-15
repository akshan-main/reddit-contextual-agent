"""CLI entry point for the Reddit Contextual Agent.

Workflow:
1. Scrape posts from configured subreddits
2. Convert to HTML and ingest to Contextual AI datastore
3. Track posts for 4-day update window (re-ingest when content changes)
4. Query the agent via Contextual AI web UI (Agent Composer)
"""

import argparse
import asyncio
import sys
import time

from .config import load_config
from .logging_config import setup_logging


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="reddit-agent", description="Scrape Reddit posts and ingest to Contextual AI datastore"
    )

    parser.add_argument(
        "--mode",
        choices=["full", "scrape", "update", "queue"],
        default="full",
        help="Pipeline mode: full (default), scrape (new posts only), update (existing only), queue (retry failed)",
    )

    parser.add_argument(
        "--hours",
        type=int,
        help="Time window in hours (default: 26 for daily runs)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs as JSON (for CI)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape without ingesting to Contextual AI",
    )

    args = parser.parse_args()

    setup_logging(level=args.log_level, json_format=args.json_logs)

    try:
        config = load_config()
    except KeyError as e:
        print(f"Error: Missing environment variable: {e}")
        sys.exit(1)

    if args.hours:
        config.scraper.time_window_hours = args.hours

    start = time.time()

    asyncio.run(run_pipeline(config, args.mode, args.dry_run))

    print(f"\nCompleted in {time.time() - start:.1f}s")


async def run_pipeline(config, mode: str = "full", dry_run: bool = False):
    """Run the pipeline in the specified mode."""
    from .pipeline import Pipeline

    print(f"Mode: {mode}")
    print(f"Subreddits: {', '.join(config.scraper.subreddits)}")
    print(f"Time window: {config.scraper.time_window_hours} hours")
    print(f"Update window: {config.scraper.update_window_days} days")

    async with Pipeline(config) as pipeline:
        if dry_run:
            posts, failed = await pipeline.scraper.scrape_all_subreddits()

            print(f"\n{'=' * 60}")
            print(f"DRY RUN: {len(posts)} posts scraped")
            print(f"{'=' * 60}")

            by_sub = {}
            for p in posts:
                by_sub[p.subreddit] = by_sub.get(p.subreddit, 0) + 1

            for sub, count in sorted(by_sub.items(), key=lambda x: -x[1]):
                print(f"  r/{sub}: {count} posts")

            if posts:
                print("\nSample posts:")
                for p in posts[:5]:
                    print(f"  [{p.subreddit}] {p.title[:60]}...")
                    print(f"    Score: {p.score} | Comments: {p.num_comments}")
            return

        # Run the appropriate pipeline mode
        if mode == "full":
            stats = await pipeline.run()
        elif mode == "scrape":
            stats = await pipeline.run_scrape_only()
        elif mode == "update":
            stats = await pipeline.run_update_only()
        elif mode == "queue":
            stats = await pipeline.run_queue_only()
        else:
            print(f"Unknown mode: {mode}")
            sys.exit(1)

        print(f"\n{'=' * 60}")
        print(f"PIPELINE COMPLETE ({mode.upper()} mode)")
        print(f"{'=' * 60}")
        print(f"Posts scraped:      {stats.posts_scraped}")
        print(f"New ingested:       {stats.documents_ingested}")
        print(f"Re-ingested:        {stats.documents_reingested}")
        print(f"Skipped (no change):{stats.skipped_unchanged}")
        print(f"Frozen:             {stats.frozen_posts}")
        print(f"Deleted:            {stats.posts_deleted}")
        print(f"Errors:             {stats.sync_errors}")


if __name__ == "__main__":
    main()
