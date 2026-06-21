"""Shared helpers for building on-demand job payloads."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from vali_utils.dashboard.label_loader import MAX_KEYWORDS_PER_OD_JOB, OdJobFileEntry

VALID_KEYWORD_MODES = {"any", "all"}
DEFAULT_OD_LOOKBACK_DAYS = 7


def resolve_od_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    lookback_days: int = DEFAULT_OD_LOOKBACK_DAYS,
) -> tuple[str, str]:
    """Return ISO start/end dates, using defaults when fields are empty."""
    now = datetime.now(timezone.utc)
    resolved_end = end_date.strip() if end_date and end_date.strip() else now.isoformat()
    if start_date and start_date.strip():
        resolved_start = start_date.strip()
    else:
        resolved_start = (now - timedelta(days=lookback_days)).isoformat()
    return resolved_start, resolved_end


def _build_job_inner(
    platform: str,
    *,
    keywords: Optional[List[str]] = None,
    subreddit: Optional[str] = None,
    usernames: Optional[List[str]] = None,
    url: Optional[str] = None,
) -> dict:
    platform = platform.lower()
    if platform == "reddit":
        job_inner: dict = {"platform": "reddit"}
        if subreddit:
            job_inner["subreddit"] = subreddit
        if keywords:
            job_inner["keywords"] = keywords[:MAX_KEYWORDS_PER_OD_JOB]
        if usernames:
            job_inner["usernames"] = usernames
        return job_inner

    job_inner = {"platform": "x"}
    if keywords:
        job_inner["keywords"] = keywords[:MAX_KEYWORDS_PER_OD_JOB]
    if usernames:
        job_inner["usernames"] = usernames
    if url:
        job_inner["url"] = url
    return job_inner


def build_od_job_payload(
    platform: str,
    keywords: List[str],
    *,
    subreddit: str = "",
    usernames: Optional[List[str]] = None,
    url: Optional[str] = None,
    limit: int = 50,
    ttl_minutes: int = 30,
    keyword_mode: str = "any",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Build the JSON body posted to the local on-demand API."""
    mode = keyword_mode if keyword_mode in VALID_KEYWORD_MODES else "any"
    start, end = resolve_od_date_range(start_date, end_date)
    job_inner = _build_job_inner(
        platform,
        keywords=keywords or None,
        subreddit=subreddit or None,
        usernames=usernames,
        url=url,
    )

    return {
        "job": {"job": job_inner},
        "limit": limit,
        "keyword_mode": mode,
        "ttl_minutes": ttl_minutes,
        "start_date": start,
        "end_date": end,
    }


def build_od_job_payload_from_entry(entry: OdJobFileEntry) -> dict:
    """Build an API payload from a file entry."""
    start, end = resolve_od_date_range(entry.start_date, entry.end_date)

    job_inner = _build_job_inner(
        entry.platform,
        keywords=entry.keywords,
        subreddit=entry.subreddit,
        usernames=entry.usernames,
        url=entry.url,
    )

    return {
        "job": {"job": job_inner},
        "limit": entry.limit,
        "keyword_mode": entry.keyword_mode,
        "ttl_minutes": entry.ttl_minutes,
        "start_date": start,
        "end_date": end,
    }


def build_od_job_payload_from_spec(
    spec: OdJobFileEntry,
    limit: int,
    ttl_minutes: int,
    keyword_mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Backward-compatible helper — prefer build_od_job_payload_from_entry."""
    return build_od_job_payload(
        spec.platform,
        spec.keywords or [],
        subreddit=spec.subreddit or "",
        usernames=spec.usernames,
        url=spec.url,
        limit=limit,
        ttl_minutes=ttl_minutes,
        keyword_mode=keyword_mode,
        start_date=start_date or spec.start_date,
        end_date=end_date or spec.end_date,
    )
