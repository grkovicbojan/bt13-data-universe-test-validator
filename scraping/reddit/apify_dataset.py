"""Helpers for Apify Reddit dataset rows and scrape_cache coercion."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional, Set

# Keys accepted by RedditContent (field names and aliases).
_REDDIT_CONTENT_KEYS: Set[str] = {
    "id",
    "url",
    "username",
    "communityName",
    "body",
    "createdAt",
    "dataType",
    "title",
    "parentId",
    "media",
    "is_nsfw",
    "isNsfw",
    "score",
    "upvote_ratio",
    "num_comments",
    "scrapedAt",
}


def is_raw_apify_reddit_dataset_item(item: Any) -> bool:
    """True when ``item`` is a live Apify macrocosmos/reddit-scraper dataset row."""
    if not isinstance(item, dict):
        return False
    return bool(item.get("url")) and bool(item.get("id"))


def normalize_apify_reddit_item(item: dict) -> dict:
    """Same normalization as RedditMCScraper._normalize_apify_item."""
    normalized = dict(item)
    if "isNsfw" in normalized:
        normalized["is_nsfw"] = normalized.pop("isNsfw")
    return normalized


def apify_reddit_item_fields(item: dict) -> Dict[str, Any]:
    """Keep only RedditContent fields from a full Apify dataset row."""
    normalized = normalize_apify_reddit_item(item)
    return {key: value for key, value in normalized.items() if key in _REDDIT_CONTENT_KEYS}


def reddit_content_from_apify_dataset_item(
    item: dict,
    *,
    scraped_at: Optional[dt.datetime] = None,
):
    """Build RedditContent the same way as a live Apify fetch."""
    from scraping.reddit.model import RedditContent

    fields = apify_reddit_item_fields(item)
    scraped = scraped_at or dt.datetime.now(dt.timezone.utc)
    return RedditContent(**fields, scrapedAt=scraped)


def reddit_content_from_cache_payload(payload: Any):
    """Parse scrape_cache Reddit JSON (full Apify row or legacy RedditContent)."""
    from scraping.reddit.model import RedditContent

    if not isinstance(payload, dict):
        return None
    try:
        return RedditContent.parse_obj(payload)
    except Exception:
        pass
    if not is_raw_apify_reddit_dataset_item(payload):
        return None
    try:
        scraped_raw = payload.get("scrapedAt")
        scraped_at: Optional[dt.datetime] = None
        if isinstance(scraped_raw, dt.datetime):
            scraped_at = scraped_raw
        elif scraped_raw:
            scraped_at = dt.datetime.fromisoformat(str(scraped_raw).replace("Z", "+00:00"))
        return reddit_content_from_apify_dataset_item(payload, scraped_at=scraped_at)
    except Exception:
        return None
