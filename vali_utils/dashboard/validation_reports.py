"""
Structured validation failure reports for the validator dashboard.

Captures job context, miner submission previews, and failure reasons for
P2P, S3, and on-demand validation paths.
"""

from __future__ import annotations

import json
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

from common.data import DataEntity
from vali_utils.dashboard.events import get_event_bus


MAX_CONTENT_PREVIEW = 800
MAX_ENTITY_PREVIEWS = 5
MAX_REPORTS = 150


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def preview_entity(entity: DataEntity, max_content: int = MAX_CONTENT_PREVIEW) -> Dict[str, Any]:
    """Serialize a DataEntity for dashboard display (truncated)."""
    raw = entity.content or b""
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = str(raw)

    parsed: Optional[Any] = None
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None

    label = getattr(entity.label, "value", None) or str(entity.label)
    return {
        "uri": entity.uri,
        "label": label,
        "datetime": entity.datetime.isoformat() if entity.datetime else None,
        "content_size_bytes": entity.content_size_bytes,
        "content_preview": text[:max_content]
        + ("…" if len(text) > max_content else ""),
        "content_json": parsed,
    }


def preview_entities(entities: List[DataEntity], limit: int = MAX_ENTITY_PREVIEWS) -> List[Dict]:
    return [preview_entity(e) for e in entities[:limit]]


def serialize_od_job(job: Any) -> Dict[str, Any]:
    """Best-effort OD job serialization."""
    try:
        if hasattr(job, "model_dump"):
            return job.model_dump(mode="json")
    except Exception:
        pass
    return {"raw": str(job)}


@dataclass
class OdValidationResult:
    """Result of a single OD submission validation."""

    passed: Optional[bool]
    entity_count: int
    report: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationFailureReport:
    """One recorded validation failure."""

    id: str
    validation_type: str  # p2p | s3 | od
    uid: int
    hotkey: str
    timestamp: str
    passed: bool = False
    failure_phase: str = ""
    reason: str = ""
    job: Dict[str, Any] = field(default_factory=dict)
    submission: Dict[str, Any] = field(default_factory=dict)
    expected: Dict[str, Any] = field(default_factory=dict)
    failures: List[Dict[str, Any]] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    hints: List[str] = field(default_factory=list)
    # Ordered failure messages from first failing step through final reason.
    validation_trail: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ValidationReportStore:
    """Thread-safe ring buffer of recent validation failure reports."""

    def __init__(self):
        self._lock = threading.RLock()
        self._reports: Deque[ValidationFailureReport] = deque(maxlen=MAX_REPORTS)

    def record(self, report: ValidationFailureReport) -> ValidationFailureReport:
        with self._lock:
            self._reports.appendleft(report)
        report_dict = report.to_dict()
        try:
            from vali_utils.postgres import get_validator_store

            store = get_validator_store()
            if store is not None:
                store.insert_validation_comparison(report_dict)
        except Exception as e:
            import bittensor as bt

            bt.logging.warning(f"Failed to persist validation comparison: {e}")
        try:
            get_event_bus().publish(
                "validation_failure",
                report.uid,
                report.hotkey,
                report_dict,
            )
        except Exception as e:
            import bittensor as bt

            bt.logging.warning(
                f"Failed to publish validation_failure SSE for UID {report.uid}: {e}"
            )
        return report

    def get_recent(
        self,
        limit: int = 50,
        offset: int = 0,
        uid: Optional[int] = None,
        validation_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        try:
            from vali_utils.postgres import get_validator_store

            store = get_validator_store()
            if store is not None:
                result = store.list_validation_comparisons(
                    limit=limit,
                    offset=offset,
                    uid=uid,
                    validation_type=validation_type,
                )
                return result["failures"]
        except Exception as e:
            import bittensor as bt

            bt.logging.warning(f"Postgres validation history read failed: {e}")

        with self._lock:
            items = list(self._reports)
        if uid is not None:
            items = [r for r in items if r.uid == uid]
        if validation_type:
            items = [r for r in items if r.validation_type == validation_type]
        items.sort(key=lambda r: r.timestamp, reverse=True)
        return [r.to_dict() for r in items[offset : offset + limit]]

    def count(
        self,
        uid: Optional[int] = None,
        validation_type: Optional[str] = None,
    ) -> int:
        try:
            from vali_utils.postgres import get_validator_store

            store = get_validator_store()
            if store is not None:
                return int(
                    store.list_validation_comparisons(
                        limit=1,
                        offset=0,
                        uid=uid,
                        validation_type=validation_type,
                    )["total"]
                )
        except Exception:
            pass
        with self._lock:
            items = list(self._reports)
        if uid is not None:
            items = [r for r in items if r.uid == uid]
        if validation_type:
            items = [r for r in items if r.validation_type == validation_type]
        return len(items)

    def clear(self) -> int:
        """Remove all stored validation failure reports. Returns count removed."""
        pg_removed = 0
        try:
            from vali_utils.postgres import get_validator_store

            store = get_validator_store()
            if store is not None:
                pg_removed = store.clear_validation_comparisons()
        except Exception as e:
            import bittensor as bt

            bt.logging.warning(f"Postgres validation history clear failed: {e}")
        with self._lock:
            count = len(self._reports)
            self._reports.clear()
            return max(count, pg_removed)


_store: Optional[ValidationReportStore] = None

_REDDIT_COMPARE_FIELDS = [
    "id",
    "url",
    "username",
    "community",
    "communityName",
    "body",
    "title",
    "createdAt",
    "dataType",
    "parentId",
    "score",
    "num_comments",
    "scrapedAt",
]

_X_COMPARE_FIELDS = [
    "username",
    "text",
    "url",
    "timestamp",
    "tweet_id",
    "like_count",
    "retweet_count",
    "reply_count",
    "view_count",
]

_REASON_FOCUS_FIELD = [
    ("bodies do not match", "body"),
    ("titles do not match", "title"),
    ("ids do not match", "id"),
    ("urls do not match", "url"),
    ("usernames do not match", "username"),
    ("communities do not match", "community"),
    ("timestamps do not match", "createdAt"),
    ("data types do not match", "dataType"),
    ("parent ids do not match", "parentId"),
    ("claimed bytes are too big", "_content_size"),
    ("label:", "label"),
    ("datetime:", "datetime"),
    ("uri:", "uri"),
    ("source:", "source"),
    ("text", "text"),
    ("tweet", "text"),
]


def _infer_failure_focus(reason: str) -> Optional[str]:
    lower = str(reason or "").lower()
    for needle, field in _REASON_FOCUS_FIELD:
        if needle in lower:
            return field
    return None


def _truncate_value(value: Any, limit: int = 280) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "…"
    return value


def enrich_content_comparison(comparison: Dict[str, Any]) -> Dict[str, Any]:
    """Add field-level diffs and content-size summary for dashboard display."""
    if not comparison:
        return comparison

    miner = comparison.get("miner_submission") or {}
    validator = comparison.get("validator_fetched") or {}
    miner_json = miner.get("content_json") if isinstance(miner.get("content_json"), dict) else {}
    validator_json = (
        validator.get("content_json")
        if isinstance(validator.get("content_json"), dict)
        else {}
    )

    platform = str(comparison.get("platform") or "").lower()
    if not platform:
        label = str(miner.get("label") or "")
        if label.startswith("r/") or "reddit.com" in str(miner.get("uri") or ""):
            platform = "reddit"
        elif "x.com" in str(miner.get("uri") or "") or "twitter.com" in str(miner.get("uri") or ""):
            platform = "x"

    fields = _REDDIT_COMPARE_FIELDS if platform == "reddit" else _X_COMPARE_FIELDS
    field_diffs: List[Dict[str, Any]] = []
    for field_name in fields:
        miner_val = miner_json.get(field_name)
        validator_val = validator_json.get(field_name)
        if miner_val is None and validator_val is None:
            continue
        field_diffs.append(
            {
                "field": field_name,
                "miner": _truncate_value(miner_val),
                "validator": _truncate_value(validator_val),
                "match": miner_val == validator_val,
            }
        )

    miner_size = miner.get("content_size_bytes")
    validator_size = validator.get("content_size_bytes")
    size_comparison = {
        "miner_content_size_bytes": miner_size,
        "validator_content_size_bytes": validator_size,
        "delta_bytes": (
            (miner_size - validator_size)
            if isinstance(miner_size, int) and isinstance(validator_size, int)
            else None
        ),
    }

    reason = comparison.get("validator_message") or ""
    comparison["field_diffs"] = field_diffs
    comparison["size_comparison"] = size_comparison
    comparison["failure_focus"] = _infer_failure_focus(reason)
    comparison["has_validator_preview"] = validator_json != {}
    return comparison


def _enrich_failure_entries(failures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for failure in failures:
        entry = dict(failure)
        comparison = entry.get("content_comparison")
        if comparison:
            entry["content_comparison"] = enrich_content_comparison(dict(comparison))
        enriched.append(entry)
    return enriched


def build_validation_trail(
    *,
    failure_phase: str = "",
    reason: str = "",
    failures: Optional[List[Dict[str, Any]]] = None,
    issues: Optional[List[str]] = None,
    explicit_trail: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Build an ordered list of {phase, message} from failure data."""
    if explicit_trail:
        return [
            {
                "phase": str(item.get("phase", failure_phase) or failure_phase),
                "message": str(item.get("message", "") or "").strip(),
            }
            for item in explicit_trail
            if str(item.get("message", "") or "").strip()
        ]

    trail: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _add(phase: str, message: str) -> None:
        msg = str(message or "").strip()
        if not msg or msg in seen:
            return
        seen.add(msg)
        trail.append({"phase": phase or failure_phase or "failed", "message": msg})

    for failure in failures or []:
        phase = str(
            failure.get("phase") or failure.get("type") or failure_phase or "failed"
        )
        comparison = failure.get("content_comparison") or {}
        _add(
            phase,
            failure.get("validator_message")
            or failure.get("detail")
            or failure.get("reason"),
        )
        comp_msg = comparison.get("validator_message")
        if comp_msg:
            _add(phase, comp_msg)

    for issue in issues or []:
        _add(failure_phase or "s3_validation", issue)

    if reason:
        _add(failure_phase or "failed", reason)

    return trail


def get_validation_reports() -> ValidationReportStore:
    global _store
    if _store is None:
        _store = ValidationReportStore()
    return _store


def record_od_failure(
    uid: int,
    hotkey: str,
    job: Any,
    submission: Any,
    report: Dict[str, Any],
) -> None:
    """Persist an OD validation failure report."""
    job_data = serialize_od_job(job)
    expected = report.get("expected") or {
        "platform": job_data.get("job", {}).get("platform")
        if isinstance(job_data.get("job"), dict)
        else None,
        "keywords": job_data.get("job", {}).get("keywords")
        if isinstance(job_data.get("job"), dict)
        else None,
        "subreddit": job_data.get("job", {}).get("subreddit")
        if isinstance(job_data.get("job"), dict)
        else None,
        "usernames": job_data.get("job", {}).get("usernames")
        if isinstance(job_data.get("job"), dict)
        else None,
        "start_date": job_data.get("start_date"),
        "end_date": job_data.get("end_date"),
        "limit": job_data.get("limit"),
        "keyword_mode": job_data.get("keyword_mode"),
    }
    check_failures = _enrich_failure_entries(list(report.get("check_failures", [])))
    entity_validation_results = list(report.get("entity_validation_results") or [])
    for item in entity_validation_results:
        comparison = item.get("content_comparison")
        if comparison:
            item["content_comparison"] = enrich_content_comparison(dict(comparison))
    failed_entity = report.get("failed_entity")
    if failed_entity and not check_failures:
        check_failures.append(
            {
                "phase": report.get("failure_phase", ""),
                "uri": failed_entity.get("uri"),
                "detail": report.get("reason", ""),
                "validator_message": report.get("reason", ""),
                "miner_submission": failed_entity,
            }
        )

    failure_phase = report.get("failure_phase", "")
    reason = report.get("reason", "OD validation failed")
    validation_trail = build_validation_trail(
        failure_phase=failure_phase,
        reason=reason,
        failures=check_failures,
        explicit_trail=report.get("validation_trail"),
    )

    get_validation_reports().record(
        ValidationFailureReport(
            id=str(uuid.uuid4()),
            validation_type="od",
            uid=uid,
            hotkey=hotkey,
            timestamp=_now_iso(),
            failure_phase=failure_phase,
            reason=reason,
            job=job_data,
            submission={
                "job_id": getattr(submission, "job_id", report.get("job_id", "")),
                "s3_path": getattr(submission, "s3_path", None),
                "entity_count": report.get("entity_count", 0),
                "entities": report.get("entity_previews", []),
                "failed_entity": failed_entity,
                "entity_validation_results": entity_validation_results,
            },
            expected=expected,
            failures=check_failures,
            hints=report.get("hints", []),
            validation_trail=validation_trail,
        )
    )


def record_p2p_failure(
    uid: int,
    hotkey: str,
    bucket_id: str,
    reason: str,
    failure_phase: str,
    entities: Optional[List[DataEntity]] = None,
    validation_results: Optional[List[Any]] = None,
    failure_entries: Optional[List[Dict[str, Any]]] = None,
    hints: Optional[List[str]] = None,
) -> None:
    """Persist a P2P validation failure report."""
    failures: List[Dict[str, Any]] = []
    if failure_entries:
        failures = _enrich_failure_entries(list(failure_entries))
    elif validation_results:
        for entity, result in zip(entities or [], validation_results):
            if not getattr(result, "is_valid", False):
                validator_msg = getattr(result, "reason", "") or "Invalid"
                failures.append(
                    {
                        "phase": failure_phase,
                        "uri": entity.uri,
                        "detail": validator_msg,
                        "validator_message": validator_msg,
                        "reason": validator_msg,
                        "miner_submission": preview_entity(entity),
                        "entity": preview_entity(entity),
                    }
                )

    validation_trail = build_validation_trail(
        failure_phase=failure_phase,
        reason=reason,
        failures=failures,
    )

    merged_hints = list(hints or []) + _p2p_hints(failure_phase, reason)
    seen_hints: set[str] = set()
    report_hints: List[str] = []
    for hint in merged_hints:
        if hint and hint not in seen_hints:
            seen_hints.add(hint)
            report_hints.append(hint)

    get_validation_reports().record(
        ValidationFailureReport(
            id=str(uuid.uuid4()),
            validation_type="p2p",
            uid=uid,
            hotkey=hotkey,
            timestamp=_now_iso(),
            failure_phase=failure_phase,
            reason=reason,
            job={"bucket_id": bucket_id},
            submission={
                "entity_count": len(entities or []),
                "entities": preview_entities(entities or []),
            },
            failures=failures,
            hints=report_hints,
            validation_trail=validation_trail,
        )
    )


def record_s3_failure(uid: int, hotkey: str, result: Any) -> None:
    """Persist an S3 validation failure report."""
    issues = list(getattr(result, "validation_issues", []) or [])
    sample_results = list(getattr(result, "sample_validation_results", []) or [])
    mismatches = list(getattr(result, "sample_job_mismatches", []) or [])

    failures = []
    for line in sample_results:
        if line.strip().startswith("❌"):
            failures.append({"type": "scraper", "detail": line})
    for line in mismatches:
        failures.append({"type": "job_match", "detail": line})

    s3_reason = getattr(result, "reason", "S3 validation failed")
    validation_trail = build_validation_trail(
        failure_phase="s3_validation",
        reason=s3_reason,
        failures=failures,
        issues=issues,
    )

    get_validation_reports().record(
        ValidationFailureReport(
            id=str(uuid.uuid4()),
            validation_type="s3",
            uid=uid,
            hotkey=hotkey,
            timestamp=_now_iso(),
            failure_phase="s3_validation",
            reason=s3_reason,
            job={
                "total_active_jobs": getattr(result, "total_active_jobs", 0),
                "expected_jobs_count": getattr(result, "expected_jobs_count", 0),
                "recent_files_count": getattr(result, "recent_files_count", 0),
                "total_files_count": getattr(result, "total_files_count", 0),
                "total_rows": getattr(result, "total_rows", 0),
                "avg_rows_per_file": round(
                    getattr(result, "total_rows", 0) / max(getattr(result, "total_files_count", 0), 1),
                    1,
                ),
                "job_coverage_rate": getattr(result, "job_coverage_rate", 0),
                "job_match_rate": getattr(result, "job_match_rate", 0),
                "scraper_success_rate": getattr(result, "scraper_success_rate", 0),
                "duplicate_percentage": getattr(result, "duplicate_percentage", 0),
            },
            submission={
                "total_size_mb": getattr(result, "total_size_bytes", 0) / (1024 * 1024),
                "effective_size_mb": getattr(result, "effective_size_bytes", 0) / (1024 * 1024),
            },
            issues=issues,
            failures=failures,
            hints=_s3_hints(issues),
            validation_trail=validation_trail,
        )
    )


def _p2p_hints(phase: str, reason: str) -> List[str]:
    hints = []
    if phase == "scraper":
        hints.append("Validator re-fetched the URI and content did not match miner submission.")
    if "x402" in reason or "Apify" in reason:
        hints.append("Scraper may need APIFY_API_TOKEN, or use REDDIT_DOM scraper for local testnet.")
    if phase == "basic":
        hints.append("Check bucket label, size_bytes, and entity format match the index.")
    if phase == "uniqueness":
        hints.append("Bucket contains duplicate entities — each URI must be unique.")
    return hints


def _s3_hints(issues: List[str]) -> List[str]:
    hints = []
    joined = " ".join(issues).lower()
    if "scraper" in joined:
        hints.append("Parquet rows failed live scraper re-validation — check DOM vs Apify scraper alignment.")
    if "job match" in joined:
        hints.append("Uploaded data labels/keywords may not match Dynamic Desirability job config.")
    if "duplicate" in joined:
        hints.append("Remove duplicate URIs/IDs within parquet files.")
    if "no files" in joined:
        hints.append("Miner has not uploaded parquet files to the local API / S3 path yet.")
    return hints
