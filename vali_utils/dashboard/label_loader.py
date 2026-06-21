"""
Load on-demand job definitions from external JSON files.

Each file must be a JSON array of job objects matching the constellation OD API
(OnDemandJobPayloadX / OnDemandJobPayloadReddit plus limit, dates, ttl, keyword_mode).
"""

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Literal, Optional

import bittensor as bt

from common.constants import MIN_OD_TTL_MINUTES

LabelRotation = Literal["sequential", "random"]

MAX_KEYWORDS_PER_OD_JOB = 5
VALID_PLATFORMS = {"x", "reddit"}
VALID_KEYWORD_MODES = {"any", "all"}


@dataclass
class OdJobFileEntry:
    """One on-demand job definition loaded from an external file."""

    platform: str
    keywords: Optional[List[str]] = None
    usernames: Optional[List[str]] = None
    subreddit: Optional[str] = None
    url: Optional[str] = None
    limit: int = 50
    keyword_mode: str = "any"
    ttl_minutes: int = 30
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    source_index: int = 0

    def summary(self) -> str:
        parts = [f"[{self.platform}]"]
        if self.subreddit:
            parts.append(f"sub={self.subreddit}")
        if self.keywords:
            parts.append(f"keywords={self.keywords}")
        if self.usernames:
            parts.append(f"usernames={self.usernames}")
        if self.url:
            parts.append(f"url={self.url}")
        parts.append(f"limit={self.limit}")
        return " ".join(parts)


# Backward-compatible alias used by scheduler imports.
LabelJobSpec = OdJobFileEntry


def _clean_str_list(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("Expected a list of strings")
    cleaned = [str(v).strip() for v in value if str(v).strip()]
    return cleaned or None


def _parse_entry(raw: dict, index: int) -> OdJobFileEntry:
    if not isinstance(raw, dict):
        raise ValueError(f"Entry {index}: expected a JSON object")

    job = raw.get("job", raw)
    if not isinstance(job, dict):
        raise ValueError(f"Entry {index}: 'job' must be an object")

    platform = str(job.get("platform", "")).lower().strip()
    if platform not in VALID_PLATFORMS:
        raise ValueError(
            f"Entry {index}: platform must be 'x' or 'reddit', got {platform!r}"
        )

    keywords = _clean_str_list(job.get("keywords"))
    usernames = _clean_str_list(job.get("usernames"))
    subreddit = job.get("subreddit")
    subreddit = str(subreddit).strip() if subreddit else None
    url = job.get("url")
    url = str(url).strip() if url else None

    if platform == "reddit":
        if not any([subreddit, keywords, usernames]):
            raise ValueError(
                f"Entry {index}: reddit job needs subreddit, keywords, or usernames"
            )
    else:
        if not any([keywords, usernames, url]):
            raise ValueError(
                f"Entry {index}: x job needs keywords, usernames, or url"
            )

    if keywords and len(keywords) > MAX_KEYWORDS_PER_OD_JOB:
        raise ValueError(
            f"Entry {index}: at most {MAX_KEYWORDS_PER_OD_JOB} keywords allowed"
        )

    limit = int(raw.get("limit", job.get("limit", 50)))
    if limit < 1 or limit > 1000:
        raise ValueError(f"Entry {index}: limit must be between 1 and 1000")

    keyword_mode = str(raw.get("keyword_mode", job.get("keyword_mode", "any"))).lower()
    if keyword_mode not in VALID_KEYWORD_MODES:
        raise ValueError(f"Entry {index}: keyword_mode must be 'any' or 'all'")

    ttl_raw = raw.get("ttl_minutes", job.get("ttl_minutes"))
    ttl_minutes = int(ttl_raw) if ttl_raw is not None else 30
    if ttl_minutes < MIN_OD_TTL_MINUTES or ttl_minutes > 1440:
        raise ValueError(
            f"Entry {index}: ttl_minutes must be between "
            f"{MIN_OD_TTL_MINUTES} and 1440"
        )

    start_date = raw.get("start_date", job.get("start_date"))
    end_date = raw.get("end_date", job.get("end_date"))
    start_date = str(start_date).strip() if start_date else None
    end_date = str(end_date).strip() if end_date else None

    return OdJobFileEntry(
        platform=platform,
        keywords=keywords,
        usernames=usernames,
        subreddit=subreddit,
        url=url,
        limit=limit,
        keyword_mode=keyword_mode,
        ttl_minutes=ttl_minutes,
        start_date=start_date,
        end_date=end_date,
        source_index=index,
    )


def load_od_job_entries(
    file_path: str,
    *,
    platform_filter: Optional[str] = None,
) -> List[OdJobFileEntry]:
    """Parse a JSON file into on-demand job entries."""
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Job file not found: {path}")
    if not path.is_file():
        raise ValueError(f"Job path is not a file: {path}")

    raw = path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e

    if not isinstance(data, list):
        raise ValueError("Job file must be a JSON array of job objects")

    entries: List[OdJobFileEntry] = []
    for index, item in enumerate(data):
        entries.append(_parse_entry(item, index))

    if platform_filter and platform_filter.lower() not in ("all", "*", "any"):
        pf = platform_filter.lower()
        entries = [e for e in entries if e.platform == pf]

    bt.logging.info(f"Loaded {len(entries)} OD job entries from {path}")
    return entries


def load_label_specs(
    file_path: str,
    *,
    file_format: str = "od_jobs",
    default_platform: str = "x",
    platform_filter: Optional[str] = None,
) -> List[OdJobFileEntry]:
    """Backward-compatible wrapper — only native OD job JSON is supported."""
    if file_format not in ("od_jobs", "auto"):
        raise ValueError(
            "Unsupported file format. Use a JSON array of on-demand job definitions."
        )
    return load_od_job_entries(file_path, platform_filter=platform_filter)


def pick_next_spec(
    specs: List[OdJobFileEntry],
    *,
    rotation: LabelRotation,
    cursor: int,
) -> tuple[OdJobFileEntry, int]:
    """Select the next job entry and return the updated cursor."""
    if not specs:
        raise ValueError("No job entries available")

    if rotation == "random":
        return random.choice(specs), cursor

    index = cursor % len(specs)
    return specs[index], cursor + 1


def preview_od_jobs(
    file_path: str,
    *,
    platform_filter: Optional[str] = None,
    limit: int = 20,
) -> dict:
    """Return a summary of jobs loaded from a file (for the dashboard UI)."""
    entries = load_od_job_entries(file_path, platform_filter=platform_filter)
    preview = [
        {
            "platform": e.platform,
            "keywords": e.keywords,
            "usernames": e.usernames,
            "subreddit": e.subreddit,
            "url": e.url,
            "limit": e.limit,
            "keyword_mode": e.keyword_mode,
            "ttl_minutes": e.ttl_minutes,
            "start_date": e.start_date,
            "end_date": e.end_date,
            "source_index": e.source_index,
            "summary": e.summary(),
        }
        for e in entries[:limit]
    ]
    return {
        "file_path": str(Path(file_path).expanduser().resolve()),
        "format": "od_jobs",
        "total": len(entries),
        "preview": preview,
    }


def preview_labels(
    file_path: str,
    *,
    file_format: str = "od_jobs",
    default_platform: str = "x",
    platform_filter: Optional[str] = None,
    limit: int = 20,
) -> dict:
    """Backward-compatible preview entry point."""
    return preview_od_jobs(file_path, platform_filter=platform_filter, limit=limit)
