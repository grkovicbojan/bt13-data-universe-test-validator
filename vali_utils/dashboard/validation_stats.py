"""
Per-path validation statistics for the validator dashboard.

Tracks job and entity counts (total vs passed) for P2P, S3, and On-Demand
validation paths, per miner and as session-wide running totals.
"""

from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from vali_utils.dashboard.events import get_event_bus


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_path_stats(path: str) -> Dict[str, Any]:
    return {
        "path": path,
        "jobs_total": 0,
        "jobs_checked": 0,
        "jobs_passed": 0,
        "jobs_failed": 0,
        "jobs_skipped": 0,
        "entities_total": 0,
        "entities_checked": 0,
        "entities_passed": 0,
        "entities_failed": 0,
        "last_timestamp": None,
        "last_status": "pending",
        "detail": {},
    }


def _empty_session() -> Dict[str, Dict[str, Any]]:
    return {
        "p2p": _empty_path_stats("p2p"),
        "s3": _empty_path_stats("s3"),
        "od": _empty_path_stats("od"),
    }


@dataclass
class PathStatsUpdate:
    """Single-path validation counters for one eval cycle."""

    path: str
    jobs_total: int = 0
    jobs_checked: int = 0
    jobs_passed: int = 0
    jobs_failed: int = 0
    jobs_skipped: int = 0
    entities_total: int = 0
    entities_checked: int = 0
    entities_passed: int = 0
    entities_failed: int = 0
    status: str = "unknown"  # passed | failed | partial | skipped
    detail: Dict[str, Any] = field(default_factory=dict)


class ValidationStatsStore:
    """Thread-safe validation counters (latest per miner + session totals)."""

    def __init__(self):
        self._lock = threading.RLock()
        self._miners: Dict[int, Dict[str, Any]] = {}
        self._session = _empty_session()

    def clear(self) -> None:
        with self._lock:
            self._miners.clear()
            self._session = _empty_session()

    def remove_miners(self, uids: List[int]) -> None:
        """Drop per-miner snapshots for the given UIDs (session totals unchanged)."""
        with self._lock:
            for uid in uids:
                self._miners.pop(int(uid), None)

    def _merge_path(self, base: Dict[str, Any], update: PathStatsUpdate) -> Dict[str, Any]:
        out = deepcopy(base)
        out.update(
            {
                "path": update.path,
                "jobs_total": update.jobs_total,
                "jobs_checked": update.jobs_checked,
                "jobs_passed": update.jobs_passed,
                "jobs_failed": update.jobs_failed,
                "jobs_skipped": update.jobs_skipped,
                "entities_total": update.entities_total,
                "entities_checked": update.entities_checked,
                "entities_passed": update.entities_passed,
                "entities_failed": update.entities_failed,
                "last_timestamp": _now_iso(),
                "last_status": update.status,
                "detail": update.detail,
            }
        )
        return out

    def _accumulate_session(self, update: PathStatsUpdate) -> None:
        path = update.path
        if path not in self._session:
            return
        s = self._session[path]
        s["jobs_total"] += update.jobs_total
        s["jobs_checked"] += update.jobs_checked
        s["jobs_passed"] += update.jobs_passed
        s["jobs_failed"] += update.jobs_failed
        s["jobs_skipped"] += update.jobs_skipped
        s["entities_total"] += update.entities_total
        s["entities_checked"] += update.entities_checked
        s["entities_passed"] += update.entities_passed
        s["entities_failed"] += update.entities_failed
        s["last_timestamp"] = _now_iso()
        s["last_status"] = update.status

    def record(self, uid: int, hotkey: str, update: PathStatsUpdate) -> Dict[str, Any]:
        with self._lock:
            if uid not in self._miners:
                self._miners[uid] = {
                    "uid": uid,
                    "hotkey": hotkey,
                    "p2p": _empty_path_stats("p2p"),
                    "s3": _empty_path_stats("s3"),
                    "od": _empty_path_stats("od"),
                    "last_updated": None,
                }
            miner = self._miners[uid]
            miner["hotkey"] = hotkey
            miner[update.path] = self._merge_path(miner[update.path], update)
            miner["last_updated"] = _now_iso()
            self._accumulate_session(update)
            snapshot = self.get_snapshot_unlocked(uid)

        get_event_bus().publish(
            "validation_stats_updated",
            uid,
            hotkey,
            {
                "uid": uid,
                "hotkey": hotkey,
                "path": update.path,
                "miner": snapshot,
                "session": self.get_session(),
            },
        )
        return snapshot

    def get_snapshot_unlocked(self, uid: int) -> Optional[Dict[str, Any]]:
        miner = self._miners.get(uid)
        return deepcopy(miner) if miner else None

    def get_miner(self, uid: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self.get_snapshot_unlocked(uid)

    def get_all_miners(self, uids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._miners.values())
        if uids is not None:
            uid_set = set(uids)
            items = [m for m in items if m["uid"] in uid_set]
        return sorted(items, key=lambda m: m["uid"])

    def get_session(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return deepcopy(self._session)

    def to_api_response(self, uids: Optional[List[int]] = None) -> Dict[str, Any]:
        return {
            "miners": self.get_all_miners(uids),
            "session": self.get_session(),
            "timestamp": _now_iso(),
        }


_store: Optional[ValidationStatsStore] = None


def get_validation_stats() -> ValidationStatsStore:
    global _store
    if _store is None:
        _store = ValidationStatsStore()
    return _store


def record_p2p_stats(
    uid: int,
    hotkey: str,
    *,
    jobs_total: int = 1,
    jobs_passed: int = 0,
    jobs_failed: int = 0,
    entities_total: int = 0,
    entities_checked: int = 0,
    entities_passed: int = 0,
    entities_failed: int = 0,
    status: str = "failed",
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    get_validation_stats().record(
        uid,
        hotkey,
        PathStatsUpdate(
            path="p2p",
            jobs_total=jobs_total,
            jobs_checked=jobs_total,
            jobs_passed=jobs_passed,
            jobs_failed=jobs_failed,
            entities_total=entities_total,
            entities_checked=entities_checked,
            entities_passed=entities_passed,
            entities_failed=entities_failed,
            status=status,
            detail=detail or {},
        ),
    )


def record_s3_stats(
    uid: int,
    hotkey: str,
    result: Any,
) -> None:
    is_valid = bool(getattr(result, "is_valid", False))
    entities_validated = int(getattr(result, "entities_validated", 0) or 0)
    entities_passed = int(getattr(result, "entities_passed_scraper", 0) or 0)
    jobs_total = int(getattr(result, "total_active_jobs", 0) or 0)
    files_checked = int(getattr(result, "recent_files_count", 0) or 0)

    get_validation_stats().record(
        uid,
        hotkey,
        PathStatsUpdate(
            path="s3",
            jobs_total=jobs_total,
            jobs_checked=files_checked,
            jobs_passed=jobs_total if is_valid else 0,
            jobs_failed=0 if is_valid else max(jobs_total, 1),
            entities_total=entities_validated,
            entities_checked=entities_validated,
            entities_passed=entities_passed,
            entities_failed=max(0, entities_validated - entities_passed),
            status="passed" if is_valid else "failed",
            detail={
                "is_valid": is_valid,
                "validation_pct": float(getattr(result, "validation_percentage", 0) or 0),
                "expected_jobs": int(getattr(result, "expected_jobs_count", 0) or 0),
                "files_checked": files_checked,
                "job_coverage_rate": float(getattr(result, "job_coverage_rate", 0) or 0),
                "job_match_rate": float(getattr(result, "job_match_rate", 0) or 0),
                "scraper_success_rate": float(getattr(result, "scraper_success_rate", 0) or 0),
                "entities_matched_job": int(getattr(result, "entities_matched_job", 0) or 0),
                "entities_checked_job_match": int(
                    getattr(result, "entities_checked_for_job_match", 0) or 0
                ),
                "reason": getattr(result, "reason", ""),
            },
        ),
    )


def record_od_stats(
    uid: int,
    hotkey: str,
    *,
    jobs_total: int,
    jobs_checked: int,
    jobs_passed: int,
    jobs_failed: int,
    jobs_skipped: int = 0,
    jobs_credibility_bumped: int = 0,
    entities_total: int,
    entities_checked: int,
    entities_passed: int,
    entities_failed: int = 0,
    status: str = "partial",
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    get_validation_stats().record(
        uid,
        hotkey,
        PathStatsUpdate(
            path="od",
            jobs_total=jobs_total,
            jobs_checked=jobs_checked,
            jobs_passed=jobs_passed,
            jobs_failed=jobs_failed,
            jobs_skipped=jobs_skipped,
            entities_total=entities_total,
            entities_checked=entities_checked,
            entities_passed=entities_passed,
            entities_failed=entities_failed,
            status=status,
            detail={
                "credibility_bumped": jobs_credibility_bumped,
                **(detail or {}),
            },
        ),
    )
