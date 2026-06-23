"""PostgreSQL-backed cache for live scraper fetches (Apify / URL validation)."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import bittensor as bt

from scraping.reddit.apify_dataset import reddit_content_from_cache_payload
from scraping.reddit.model import RedditContent
from scraping.x.apify_dataset import coerce_scrape_cache_payload_to_apify_item
from scraping.x.model import XContent


def _get_store():
    try:
        from vali_utils.postgres import get_validator_store

        return get_validator_store()
    except Exception as e:
        bt.logging.debug(f"Scrape cache unavailable: {e}")
        return None


def get_cached_reddit_content(url: str) -> Optional[RedditContent]:
    store = _get_store()
    if not store:
        return None
    try:
        payload = store.get_scrape_cache(url, "reddit")
        if not payload:
            return None
        return reddit_content_from_cache_payload(payload)
    except Exception as e:
        bt.logging.debug(f"Reddit scrape cache read failed for {url}: {e}")
        return None


def store_reddit_content(url: str, content: RedditContent) -> None:
    store = _get_store()
    if not store:
        return
    try:
        data = content.dict(by_alias=True)
        if isinstance(data.get("createdAt"), dt.datetime):
            data["createdAt"] = data["createdAt"].isoformat()
        if isinstance(data.get("scrapedAt"), dt.datetime):
            data["scrapedAt"] = data["scrapedAt"].isoformat()
        store.put_scrape_cache(url, "reddit", data)
    except Exception as e:
        bt.logging.debug(f"Reddit scrape cache write failed for {url}: {e}")


def get_cached_x_apify_dataset_item(url: str) -> Optional[Dict[str, Any]]:
    """Return scrape_cache X data coerced to an Apify dataset row for parsing."""
    store = _get_store()
    if not store:
        return None
    try:
        payload = store.get_scrape_cache(url, "x")
        if not payload:
            return None
        coerced = coerce_scrape_cache_payload_to_apify_item(payload)
        if coerced is None:
            bt.logging.debug(f"X scrape cache row is not a supported shape for {url}")
        return coerced
    except Exception as e:
        bt.logging.debug(f"X scrape cache read failed for {url}: {e}")
        return None


def store_x_content(url: str, content: XContent) -> None:
    store = _get_store()
    if not store:
        return
    try:
        data = content.dict()
        for field in ("timestamp", "scraped_at"):
            value = data.get(field)
            if isinstance(value, dt.datetime):
                data[field] = value.isoformat()
        store.put_scrape_cache(url, "x", data)
    except Exception as e:
        bt.logging.debug(f"X scrape cache write failed for {url}: {e}")


def cache_lookup(platform: str, url: str) -> Optional[Any]:
    if platform == "reddit":
        return get_cached_reddit_content(url)
    if platform == "x":
        return get_cached_x_apify_dataset_item(url)
    return None
