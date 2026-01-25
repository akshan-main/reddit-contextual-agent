# Reddit Contextual Agent

A data pipeline that scrapes Reddit posts from RAG-focused subreddits and syncs them to [Contextual AI](https://contextual.ai) for building RAG-powered agents.

## Features

- **Scraping**: Fetches ALL posts within a time window
- **Supabase Backend**: PostgreSQL for state management and tracking
- **Update Cycle**: Posts tracked for 4 days with refresh, then frozen
- **Re-ingestion**: Only re-ingest when content changes (post edits, new comments, or comment edits)
- **Deletion Handling**: Automatically removes deleted/removed posts from datastore (Reddit Data API compliance)
- **Bot Filtering**: Excludes AutoModerator and known bots
- **GitHub Actions**: Daily automated scraping at 8 AM Pacific

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  Reddit API     │────▶│   Scraper    │────▶│  Supabase           │
│  (PRAW)         │     │              │     │  (PostgreSQL)       │
└─────────────────┘     └──────────────┘     └─────────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────────┐
                                              │  Contextual AI      │
                                              │  (RAG Datastore)    │
                                              └─────────────────────┘
```

### Update Cycle

Posts go through a count-based lifecycle:
- **Day 1** (`update_count=-1`): Initial scrape and ingest
- **Day 2** (`update_count=0`): Skip (no fetch from Reddit)
- **Day 3** (`update_count=1`): Refresh - fetch from Reddit, re-ingest if content changed, otherwise metadata-only update (score/num_comments/upvote_ratio)
- **Day 4** (`update_count=2`): Refresh and freeze - same as Day 3, then status set to FROZEN

Deleted posts are automatically removed from datastore and database when detected during refresh.

**Retention:** posts are stored in Supabase for up to 30 days (unless deleted/removed).
**Freshness tracking:** posts are checked for updates for up to 4 days; after that, they are considered stable and no longer updated

## Setup

### Prerequisites

- Python 3.10+
- Reddit API credentials ([create app](https://reddit.com/prefs/apps))
- Contextual AI account ([contextual.ai](https://contextual.ai))
- Supabase account ([supabase.com](https://supabase.com))

### Installation

```bash
git clone https://github.com/akshan-main/reddit-contextual-agent.git
cd reddit-contextual-agent
python -m venv venv
source venv/bin/activate
pip install -e .
```

### Configuration

Copy `.env.example` to `.env` and fill in the api keys and supabase link

## Usage

### CLI

```bash
# Run full pipeline
python -m reddit_agent --mode full

# Scrape only (no updates)
python -m reddit_agent --mode scrape

# Update existing posts only
python -m reddit_agent --mode update

# With JSON logs (for CI)
python -m reddit_agent --mode full --json-logs
```

### Local Testing

Test the pipeline without actual Contextual AI ingestion:

```bash
python test_local.py
```

## GitHub Actions

The workflow runs daily at 8 AM Pacific time.

### Required Secrets

Add these to your repository settings:

| Secret | Description |
|--------|-------------|
| `REDDIT_CLIENT_ID` | Reddit app client ID |
| `REDDIT_CLIENT_SECRET` | Reddit app secret |
| `CONTEXTUAL_API_KEY` | Contextual AI API key |
| `CONTEXTUAL_DATASTORE_ID` | Target datastore ID |
| `SUPABASE_CONNECTION_STRING` | Pooler connection string |

### Manual Trigger

1. Go to **Actions** tab
2. Select **Reddit Scraper**
3. Click **Run workflow**
4. Choose mode: `full`, `scrape`, `update`, or `queue`

## Data Model

### What's Scraped

| Field | Description |
|-------|-------------|
| `id`, `subreddit`, `author`, `title`, `selftext` | Core post data |
| `score`, `num_comments`, `upvote_ratio` | Engagement metrics |
| `created_utc`, `edited` | Timestamps |
| `comments` (up to 100) | With author, body, score(proxy for upvotes - downvotes), depth |

### What's Ingested

Posts are converted to HTML documents with:
- Full post content with metadata
- All comments (sorted by score)
- Timezone (Pacific + UTC)

Metadata includes: `subreddit`, `author`, `title`, `score`, `upvote_ratio_bp` (integer basis points, e.g. 9200 = 92%), `num_comments`, `created_utc`, `created_pacific`, `date_pacific`, `post_id`, `is_self`, `external_url`, `flair`

### Sample Ingested Documents

![sample ingestion in datastore](docs/sample_ingested_documents.png)

## Project Structure

```
reddit-contextual-agent/
├── docs/
├── src/reddit_agent/        # All source modules
├── tests/
├── .github/workflows/
├── test_local.py            # Local testing
├── pyproject.toml
└── .env.example
```

## Verifying Metadata Updates

To verify that metadata is being sent correctly to Contextual AI and to catch any document ID mismatches:

### 1. Run the pipeline and capture logs

```bash
python -m reddit_agent --mode scrape --log-level INFO 2>&1 | tee pipeline.log
```

Look for logs with `document_id=` to capture the ID, e.g.:
```
ingesting_document post_id=abc123 document_id=12345678-...
metadata_updated document_id=12345678-...
```

**Critical:** Verify that both lines have the **same** `document_id`. If they differ, there's a document ID mismatch bug.

### 2. Query the metadata endpoint

```bash
export CONTEXTUAL_API_KEY="your_key"
export DATASTORE_ID="your_datastore_id"
export DOC_ID="12345678-..."  # From logs above

curl -sS -H "Authorization: Bearer $CONTEXTUAL_API_KEY" \
"https://api.contextual.ai/v1/datastores/$DATASTORE_ID/documents/$DOC_ID/metadata" | jq '.custom_metadata'
```

**Expected output:**
```json
{
  "url": "https://reddit.com/r/RAG/comments/abc123/...",
  "subreddit": "RAG",
  "author": "username",
  "title": "Discussion about RAG",
  "score": 245,
  "upvote_ratio_bp": 9200,
  "num_comments": 12,
  "created_utc": "2026-01-25T10:30:45.123456+00:00",
  "created_pacific": "2026-01-25T02:30:45.123456-08:00",
  "date_pacific": "2026-01-25",
  "post_id": "abc123",
  "is_self": true,
  "flair": "Discussion"
}
```

**If metadata shows `{}`:** See troubleshooting below.

### 3. Monitor metadata-only updates

Run update mode and watch for metadata changes:

```bash
python -m reddit_agent --mode update --log-level DEBUG 2>&1 | grep "metadata"
```

Expected log when only score changes:
```
metadata_only_update post_id=abc123 old_score=150 new_score=245
metadata_updated document_id=12345678-...
```

### Troubleshooting

**Metadata is empty `{}`:**

1. Check for API errors in logs:
   ```bash
   grep "set_metadata_failed\|ingest_response_missing_id" pipeline.log
   ```

2. Verify document exists:
   ```bash
   curl -sS -H "Authorization: Bearer $CONTEXTUAL_API_KEY" \
   "https://api.contextual.ai/v1/datastores/$DATASTORE_ID/documents/$DOC_ID" | jq '.document.id'
   ```

3. Verify datastore ID is correct:
   ```bash
   curl -sS -H "Authorization: Bearer $CONTEXTUAL_API_KEY" \
   "https://api.contextual.ai/v1/datastores" | jq '.datastores[] | .id'
   ```

**Document ID mismatch:**

If logs show different IDs for ingest vs metadata update, check:
- `ingest_response_missing_id` error in logs (means API isn't returning ID)
- Fallback to `reddit_post_<id>` format being used instead of API UUID

# NOTE
The license in this repository applies to the code in this repository. Scraped Reddit content remains subject to Reddit's terms and the original authors' rights.