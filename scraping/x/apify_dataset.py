"""Helpers for Apify X/Twitter dataset rows and scrape_cache coercion."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

_APIFY_CREATED_AT_FMT = "%a %b %d %H:%M:%S %z %Y"


def is_raw_apify_x_dataset_item(item: Any) -> bool:
    """True when ``item`` is a live Apify twitter-scraper-lite dataset row."""
    if not isinstance(item, dict):
        return False
    if not all(field in item for field in ("text", "url", "createdAt")):
        return False
    author = item.get("author")
    return isinstance(author, dict) and bool(author.get("userName"))


def is_xcontent_cache_payload(item: Any) -> bool:
    """True when ``item`` is an XContent-shaped row (e.g. from sync script)."""
    if not isinstance(item, dict):
        return False
    if is_raw_apify_x_dataset_item(item):
        return False
    return bool(item.get("username")) and bool(item.get("url")) and "text" in item


def find_raw_apify_x_dataset_item(
    dataset: List[dict], uri: str, *, normalize_url
) -> Optional[dict]:
    """Return the raw Apify row whose URL matches ``uri``."""
    target = normalize_url(uri)
    for item in dataset:
        if not is_raw_apify_x_dataset_item(item):
            continue
        if normalize_url(str(item.get("url") or "")) == target:
            return item
    return None


def _parse_timestamp(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _timestamp_to_apify_created_at(value: Any) -> Optional[str]:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.strftime(_APIFY_CREATED_AT_FMT)


def _entities_from_tweet_hashtags(tags: Any) -> Dict[str, List[dict]]:
    hashtags: List[dict] = []
    symbols: List[dict] = []
    if not isinstance(tags, list):
        return {"hashtags": hashtags, "symbols": symbols}
    for index, tag in enumerate(tags):
        if not isinstance(tag, str) or not tag:
            continue
        if tag.startswith("#"):
            hashtags.append({"text": tag[1:], "indices": [index, index + len(tag)]})
        elif tag.startswith("$"):
            symbols.append({"text": tag[1:], "indices": [index, index + len(tag)]})
    return {"hashtags": hashtags, "symbols": symbols}


def xcontent_cache_payload_to_apify_item(payload: Dict[str, Any]) -> Optional[dict]:
    """Rebuild an Apify-like dataset row from XContent-shaped scrape_cache JSON."""
    if not is_xcontent_cache_payload(payload):
        return None

    created_at = _timestamp_to_apify_created_at(payload.get("timestamp"))
    if not created_at:
        return None

    media = payload.get("media") or []
    media_rows: List[dict] = []
    if isinstance(media, list):
        for entry in media:
            if isinstance(entry, str):
                media_rows.append({"media_url_https": entry})
            elif isinstance(entry, dict) and entry.get("media_url_https"):
                media_rows.append(entry)

    user_verified = payload.get("user_verified")
    user_blue_verified = payload.get("user_blue_verified")

    author: Dict[str, Any] = {
        "id": payload.get("user_id"),
        "userName": payload.get("username"),
        "name": payload.get("user_display_name"),
        "isVerified": bool(user_verified),
        "isBlueVerified": user_blue_verified,
        "verified": bool(user_verified),
        "description": payload.get("user_description"),
        "location": payload.get("user_location"),
        "profilePicture": payload.get("profile_image_url"),
        "coverPicture": payload.get("cover_picture_url"),
        "followers": payload.get("user_followers_count"),
        "following": payload.get("user_following_count"),
    }

    entities = _entities_from_tweet_hashtags(payload.get("tweet_hashtags"))

    return {
        "id": payload.get("tweet_id"),
        "text": payload.get("text") or "",
        "url": payload.get("url"),
        "createdAt": created_at,
        "lang": payload.get("language"),
        "likeCount": payload.get("like_count"),
        "retweetCount": payload.get("retweet_count"),
        "replyCount": payload.get("reply_count"),
        "quoteCount": payload.get("quote_count"),
        "viewCount": payload.get("view_count"),
        "bookmarkCount": payload.get("bookmark_count"),
        "isReply": payload.get("is_reply"),
        "isQuote": payload.get("is_quote"),
        "conversationId": payload.get("conversation_id"),
        "inReplyToUserId": payload.get("in_reply_to_user_id"),
        "inReplyToUsername": payload.get("in_reply_to_username"),
        "quoteId": payload.get("quoted_tweet_id"),
        "entities": entities,
        "media": media_rows or None,
        "author": author,
    }


def coerce_scrape_cache_payload_to_apify_item(payload: Any) -> Optional[dict]:
    """Normalize any supported X scrape_cache row to an Apify dataset item."""
    if not isinstance(payload, dict):
        return None
    if is_raw_apify_x_dataset_item(payload):
        return payload
    return xcontent_cache_payload_to_apify_item(payload)
