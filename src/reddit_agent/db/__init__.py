"""Database layer using Supabase (PostgreSQL)."""

from .base import DatabaseProtocol, compute_content_hash
from .supabase import SupabaseDatabase

__all__ = ["DatabaseProtocol", "SupabaseDatabase", "compute_content_hash"]
