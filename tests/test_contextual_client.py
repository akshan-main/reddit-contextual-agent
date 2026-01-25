"""Tests for Contextual AI client."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from reddit_agent.config import ContextualConfig
from reddit_agent.contextual_client import ContextualClient


class TestContextualClient:
    """Tests for ContextualClient class."""

    @pytest.fixture
    def config(self):
        """Create test config."""
        return ContextualConfig()

    @pytest.fixture
    def client(self, config):
        """Create client instance."""
        return ContextualClient(config)

    def test_client_initialization(self, client, config):
        """Test client initializes with config."""
        assert client.config == config
        assert client._client is None

    @pytest.mark.asyncio
    async def test_client_connect(self, client):
        """Test client connection."""
        with patch("reddit_agent.contextual_client.ContextualAI") as mock_ai:
            await client.connect()
            mock_ai.assert_called_once_with(api_key=client.config.api_key)
            assert client._client is not None

    @pytest.mark.asyncio
    async def test_client_close(self, client):
        """Test client close."""
        client._client = MagicMock()
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_client_context_manager(self, client):
        """Test client as async context manager."""
        with patch("reddit_agent.contextual_client.ContextualAI"):
            async with client:
                assert client._client is not None
            assert client._client is None

    def test_post_to_html(self, client, sample_post):
        """Test converting post to HTML."""
        html = client._post_to_html(sample_post)

        assert "<!DOCTYPE html>" in html
        assert sample_post.title in html
        assert sample_post.subreddit in html
        assert sample_post.author in html
        assert "[POST]" in html  # Post marker
        assert "Community Discussion" in html  # Comments section

    def test_post_to_html_escapes_html(self, client, sample_post):
        """Test HTML escaping in post conversion."""
        sample_post.title = "Test <script>alert('xss')</script>"
        html = client._post_to_html(sample_post)

        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_post_to_html_no_comments(self, client, sample_post):
        """Test HTML generation with no comments."""
        sample_post.comments = []
        html = client._post_to_html(sample_post)

        assert "Community Discussion" not in html

    def test_get_metadata(self, client, sample_post):
        """Test metadata extraction."""
        metadata = client._get_metadata(sample_post)

        assert metadata["subreddit"] == sample_post.subreddit
        assert metadata["author"] == sample_post.author
        assert metadata["title"] == sample_post.title
        assert metadata["score"] == int(sample_post.score)
        assert metadata["upvote_ratio_bp"] == int(round(float(sample_post.upvote_ratio) * 10000))
        assert metadata["num_comments"] == int(sample_post.num_comments)
        assert metadata["post_id"] == sample_post.id
        assert metadata["is_self"] == sample_post.is_self
        assert "url" in metadata
        assert "created_utc" in metadata
        assert "created_pacific" in metadata
        assert "date_pacific" in metadata
        assert "flair" in metadata
        # external_url only present for link posts (is_self=False)
        assert "external_url" not in metadata  # sample_post is a text post (is_self=True)

    def test_get_metadata_handles_naive_datetime(self, client, sample_post):
        """Test metadata handles naive datetime."""
        sample_post.created_utc = datetime(2024, 1, 15, 12, 0, 0)  # naive
        metadata = client._get_metadata(sample_post)

        assert "created_utc" in metadata
        assert "created_pacific" in metadata

    @pytest.mark.asyncio
    async def test_ingest_document(self, client, sample_post):
        """Test document ingestion."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.id = f"reddit_post_{sample_post.id}"
        mock_client.datastores.documents.ingest.return_value = mock_result
        mock_client.datastores.documents.set_metadata.return_value = None
        client._client = mock_client

        doc_id = await client.ingest_document(sample_post)

        assert doc_id == f"reddit_post_{sample_post.id}"
        mock_client.datastores.documents.ingest.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingest_document_not_connected(self, client, sample_post):
        """Test ingestion fails when not connected."""
        # The @retry decorator wraps RuntimeError in RetryError
        with pytest.raises(Exception):  # Either RuntimeError or RetryError
            await client.ingest_document(sample_post)

    @pytest.mark.asyncio
    async def test_set_metadata(self, client, sample_post):
        """Test setting metadata."""
        mock_client = MagicMock()
        mock_client.datastores.documents.set_metadata.return_value = None
        client._client = mock_client

        result = await client.set_metadata("doc_123", sample_post)

        assert result is True
        mock_client.datastores.documents.set_metadata.assert_called_once()

    @pytest.mark.asyncio
    async def test_set_metadata_failure(self, client, sample_post):
        """Test set_metadata handles failure."""
        mock_client = MagicMock()
        mock_client.datastores.documents.set_metadata.side_effect = Exception("API Error")
        client._client = mock_client

        result = await client.set_metadata("doc_123", sample_post)

        assert result is False

    @pytest.mark.asyncio
    async def test_delete_document(self, client):
        """Test document deletion."""
        mock_client = MagicMock()
        mock_client.datastores.documents.delete.return_value = None
        client._client = mock_client

        result = await client.delete_document("doc_123")

        assert result is True
        mock_client.datastores.documents.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_document_failure(self, client):
        """Test delete handles failure."""
        mock_client = MagicMock()
        mock_client.datastores.documents.delete.side_effect = Exception("Not found")
        client._client = mock_client

        result = await client.delete_document("doc_123")

        assert result is False

    @pytest.mark.asyncio
    async def test_smart_sync_new_document(self, client, sample_post):
        """Test smart_sync creates new document."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.id = f"reddit_post_{sample_post.id}"
        mock_client.datastores.documents.ingest.return_value = mock_result
        mock_client.datastores.documents.set_metadata.return_value = None
        client._client = mock_client

        doc_id = await client.smart_sync(sample_post, existing_doc_id=None)

        assert doc_id == f"reddit_post_{sample_post.id}"

    @pytest.mark.asyncio
    async def test_smart_sync_content_changed(self, client, sample_post):
        """Test smart_sync re-ingests when content changed."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.id = f"reddit_post_{sample_post.id}"
        mock_client.datastores.documents.ingest.return_value = mock_result
        mock_client.datastores.documents.delete.return_value = None
        mock_client.datastores.documents.set_metadata.return_value = None
        client._client = mock_client

        await client.smart_sync(
            sample_post,
            existing_doc_id="old_doc_id",
            content_changed=True,
        )

        # Should have called delete then ingest
        mock_client.datastores.documents.delete.assert_called_once()
        mock_client.datastores.documents.ingest.assert_called_once()

    @pytest.mark.asyncio
    async def test_smart_sync_no_changes(self, client, sample_post):
        """Test smart_sync skips when no changes."""
        mock_client = MagicMock()
        client._client = mock_client

        doc_id = await client.smart_sync(
            sample_post,
            existing_doc_id="existing_doc_id",
            content_changed=False,
        )

        assert doc_id == "existing_doc_id"
        mock_client.datastores.documents.ingest.assert_not_called()
        mock_client.datastores.documents.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_health_check_success(self, client):
        """Test health check success."""
        mock_client = MagicMock()
        mock_client.datastores.list.return_value = []
        client._client = mock_client

        result = await client.health_check()

        assert result is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, client):
        """Test health check failure."""
        mock_client = MagicMock()
        mock_client.datastores.list.side_effect = Exception("API Error")
        client._client = mock_client

        result = await client.health_check()

        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_not_connected(self, client):
        """Test health check when not connected."""
        result = await client.health_check()

        assert result is False
