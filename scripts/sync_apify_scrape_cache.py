#!/usr/bin/env python3
"""Import Apify actor dataset items into PostgreSQL scrape_cache.

The test validator checks scrape_cache by URL before calling Apify
(apidojo_scraper / reddit_mc_scraper). X rows are stored as full Apify
dataset items (same JSON as iterate_items / all fields). Run this to
backfill from your Apify account so validation reuses cached rows.

Requires:
  APIFY_API_TOKEN  — Apify API key
  DATABASE_URL     — same Postgres as the test validator

Examples:
  python scripts/sync_apify_scrape_cache.py --recent-runs 30
  python scripts/sync_apify_scrape_cache.py --url https://x.com/user/status/123
  python scripts/sync_apify_scrape_cache.py --recent-runs 10 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

from apify_client import ApifyClient

# Postgres only — avoids pulling bittensor via scraping.* imports.
from scraping.x.apify_dataset import is_raw_apify_x_dataset_item
from vali_utils.postgres.store import ValidatorPostgresStore

X_ACTOR_ID = "nfp1fpt5gUlBwPcor"
REDDIT_ACTOR_ID = "macrocosmos/reddit-scraper"

_OK_RUN_STATUSES = frozenset(
    {"SUCCEEDED", "TIMED-OUT", "RUNNING", "READY", "ABORTING"}
)

_DEFAULT_MAX_TOTAL_CHARGE_USD = 1.0


def _max_total_charge_usd() -> float:
    raw = os.getenv("APIFY_MAX_TOTAL_CHARGE_USD", str(_DEFAULT_MAX_TOTAL_CHARGE_USD))
    try:
        return max(0.02, float(raw))
    except (TypeError, ValueError):
        return _DEFAULT_MAX_TOTAL_CHARGE_USD


def _get_store() -> Optional[ValidatorPostgresStore]:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return None
    return ValidatorPostgresStore(url)


def _normalize_x_url(url: str) -> str:
    return (url or "").replace("twitter.com/", "x.com/")


def _x_url_from_apify_item(item: dict) -> str:
    return _normalize_x_url(str(item.get("url") or ""))


def _normalize_reddit_item(item: dict) -> dict:
    out = dict(item)
    if "isNsfw" in out:
        out["is_nsfw"] = out.pop("isNsfw")
    return out


def _parse_reddit_item(item: dict) -> Optional[Dict[str, Any]]:
    normalized = _normalize_reddit_item(item)
    if not normalized.get("url") or not normalized.get("id"):
        return None
    normalized["scrapedAt"] = dt.datetime.now(dt.timezone.utc).isoformat()
    for key in ("createdAt", "scrapedAt"):
        if isinstance(normalized.get(key), dt.datetime):
            normalized[key] = normalized[key].isoformat()
    return normalized


def _put_cache(
    store: ValidatorPostgresStore,
    url: str,
    platform: str,
    payload: Dict[str, Any],
) -> None:
    store.put_scrape_cache(url, platform, payload)


def _cache_has(
    store: ValidatorPostgresStore, platform: str, url: str
) -> bool:
    return store.get_scrape_cache(url, platform) is not None


def _parse_since_hours(value: Optional[float]) -> Optional[dt.datetime]:
    if value is None:
        return None
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=value)


def _run_started_at(run: dict) -> Optional[dt.datetime]:
    raw = run.get("startedAt") or run.get("createdAt")
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _iter_dataset_items(client: ApifyClient, dataset_id: str) -> Iterable[dict]:
    if not dataset_id:
        return
    for item in client.dataset(dataset_id).iterate_items():
        if isinstance(item, dict):
            yield item


def _store_payload(
    store: ValidatorPostgresStore,
    platform: str,
    url: str,
    payload: Dict[str, Any],
    *,
    force: bool,
    dry_run: bool,
) -> str:
    if not url:
        return "skip_no_url"
    if not force and _cache_has(store, platform, url):
        return "skipped"
    if dry_run:
        return "would_store"
    _put_cache(store, url, platform, payload)
    return "stored"


def sync_actor_runs(
    client: ApifyClient,
    store: ValidatorPostgresStore,
    *,
    actor_id: str,
    platform: str,
    recent_runs: int,
    since: Optional[dt.datetime],
    include_running: bool,
    force: bool,
    dry_run: bool,
) -> Dict[str, int]:
    stats = {
        "runs_seen": 0,
        "runs_used": 0,
        "items_seen": 0,
        "stored": 0,
        "skipped": 0,
        "unparsed": 0,
        "would_store": 0,
    }

    runs = client.actor(actor_id).runs().list(limit=recent_runs, desc=True)
    seen_urls: set[str] = set()

    for run in runs.items:
        stats["runs_seen"] += 1
        status = str(run.get("status", "")).upper()
        if status not in _OK_RUN_STATUSES:
            continue
        if status == "RUNNING" and not include_running:
            continue
        started = _run_started_at(run)
        if since is not None and started is not None and started < since:
            continue

        stats["runs_used"] += 1
        run_id = run.get("id", "")
        dataset_id = run.get("defaultDatasetId", "")
        print(f"  run {run_id} status={status} dataset={dataset_id}")

        for item in _iter_dataset_items(client, dataset_id):
            stats["items_seen"] += 1
            if platform == "x":
                if not is_raw_apify_x_dataset_item(item):
                    stats["unparsed"] += 1
                    continue
                url = _x_url_from_apify_item(item)
                payload = item
            else:
                payload = _parse_reddit_item(item)
                if payload is None:
                    stats["unparsed"] += 1
                    continue
                url = str(payload.get("url", "")).strip()

            if url in seen_urls and not force:
                stats["skipped"] += 1
                continue
            seen_urls.add(url)

            outcome = _store_payload(
                store, platform, url, payload, force=force, dry_run=dry_run
            )
            if outcome == "stored":
                stats["stored"] += 1
            elif outcome == "would_store":
                stats["would_store"] += 1
            elif outcome == "skipped":
                stats["skipped"] += 1

    return stats


def prefetch_url(
    client: ApifyClient,
    store: ValidatorPostgresStore,
    url: str,
    *,
    force: bool,
    dry_run: bool,
) -> Tuple[str, str]:
    url = url.strip()
    charge = _max_total_charge_usd()

    if "reddit.com" in url.lower():
        platform = "reddit"
        if not force and _cache_has(store, platform, url):
            return platform, "skipped"
        if dry_run:
            return platform, "would_store"
        run = client.actor(REDDIT_ACTOR_ID).call(
            run_input={"url": url},
            timeout_secs=300,
            max_total_charge_usd=charge,
        )
        for item in _iter_dataset_items(client, run.get("defaultDatasetId", "")):
            payload = _parse_reddit_item(item)
            if payload and payload.get("url"):
                _put_cache(store, payload["url"], platform, payload)
                return platform, "stored"
        return platform, "unparsed"

    if re.search(r"(x\.com|twitter\.com)/.+/status/\d+", url, re.I):
        platform = "x"
        cache_url = _normalize_x_url(url)
        if not force and _cache_has(store, platform, cache_url):
            return platform, "skipped"
        if dry_run:
            return platform, "would_store"
        run = client.actor(X_ACTOR_ID).call(
            run_input={
                "startUrls": [url],
                "maxItems": 1,
                "maxRequestRetries": 5,
                "includeSearchTerms": False,
                "sort": "Latest",
            },
            timeout_secs=120,
            max_total_charge_usd=charge,
        )
        for item in _iter_dataset_items(client, run.get("defaultDatasetId", "")):
            if not is_raw_apify_x_dataset_item(item):
                continue
            if _x_url_from_apify_item(item) == cache_url:
                _put_cache(store, cache_url, platform, item)
                return platform, "stored"
        return platform, "unparsed"

    return "unknown", "unsupported_url"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Apify actor datasets into PostgreSQL scrape_cache."
    )
    parser.add_argument("--recent-runs", type=int, default=20)
    parser.add_argument("--since-hours", type=float, default=None)
    parser.add_argument("--platform", choices=("x", "reddit", "all"), default="all")
    parser.add_argument(
        "--include-running",
        action="store_true",
        help="Also read datasets from runs still in RUNNING state.",
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--url", default=None, help="Prefetch one URL via Apify.")
    args = parser.parse_args()

    token = (os.getenv("APIFY_API_TOKEN") or "").strip()
    if not token:
        print("ERROR: APIFY_API_TOKEN is not set.", file=sys.stderr)
        return 1

    try:
        store = _get_store()
    except Exception as exc:
        print(f"ERROR: Postgres connection failed: {exc}", file=sys.stderr)
        return 1
    if store is None:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    client = ApifyClient(token)
    since = _parse_since_hours(args.since_hours)

    try:
        if args.url:
            platform, outcome = prefetch_url(
                client, store, args.url, force=args.force, dry_run=args.dry_run
            )
            print(f"url={args.url} platform={platform} outcome={outcome}")
            return 0 if outcome in ("stored", "skipped", "would_store") else 1

        actors: List[Tuple[str, str]] = []
        if args.platform in ("x", "all"):
            actors.append(("x", X_ACTOR_ID))
        if args.platform in ("reddit", "all"):
            actors.append(("reddit", REDDIT_ACTOR_ID))

        grand = {"stored": 0, "skipped": 0, "unparsed": 0, "would_store": 0}

        for platform, actor_id in actors:
            print(f"\n=== {platform} actor {actor_id} ===")
            stats = sync_actor_runs(
                client,
                store,
                actor_id=actor_id,
                platform=platform,
                recent_runs=args.recent_runs,
                since=since,
                include_running=args.include_running,
                force=args.force,
                dry_run=args.dry_run,
            )
            print(
                f"runs {stats['runs_used']}/{stats['runs_seen']} | "
                f"items {stats['items_seen']} | "
                f"stored {stats['stored']} | "
                f"skipped {stats['skipped']} | "
                f"unparsed {stats['unparsed']}"
                + (f" | would_store {stats['would_store']}" if args.dry_run else "")
            )
            for key in grand:
                if key in stats:
                    grand[key] += stats[key]

        print(
            "\nDone."
            f" stored={grand['stored']}"
            f" skipped={grand['skipped']}"
            f" unparsed={grand['unparsed']}"
            + (f" would_store={grand['would_store']}" if args.dry_run else "")
        )
        return 0
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
