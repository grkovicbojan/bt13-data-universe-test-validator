"""PostgreSQL persistence for testnet validator dashboard features."""

from __future__ import annotations

import os
from typing import Optional

from vali_utils.postgres.store import ValidatorPostgresStore

_store: Optional[ValidatorPostgresStore] = None


def get_validator_store() -> Optional[ValidatorPostgresStore]:
    """Return the shared store, or None when DATABASE_URL is unset."""
    global _store
    if _store is not None:
        return _store
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return None
    _store = ValidatorPostgresStore(url)
    return _store


def init_validator_store() -> Optional[ValidatorPostgresStore]:
    """Connect and ensure schema; safe to call at validator startup."""
    return get_validator_store()
