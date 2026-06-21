"""
Local filesystem storage that mirrors the Macrocosmos S3 key layout.

Miner uploads land under:
  data/hotkey={hotkey}/job_id={job_id}/*.parquet

On-demand submissions land under:
  on_demand/submissions/{job_id}/{hotkey}/data.json
"""

import json
import os
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote

import bittensor as bt


def project_local_api_data_dir() -> Path:
    """Default storage root inside the repository: <repo>/local_api_data."""
    return Path(__file__).resolve().parents[2] / "local_api_data"


class LocalApiStorage:
    """Thread-safe local storage for miner parquet files and on-demand jobs."""

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "data").mkdir(exist_ok=True)
        (self.data_dir / "on_demand" / "jobs").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "on_demand" / "submissions").mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        bt.logging.info(f"Local API storage root: {self.data_dir}")

    # ------------------------------------------------------------------
    # S3-style parquet storage
    # ------------------------------------------------------------------

    def miner_prefix(self, hotkey: str) -> str:
        return f"data/hotkey={hotkey}/"

    def resolve_key_path(self, key: str) -> Path:
        """Map an S3-style key to a local filesystem path."""
        safe_key = key.lstrip("/")
        return self.data_dir / safe_key

    def list_parquet_files(self, miner_hotkey: str) -> List[Dict[str, Any]]:
        """List parquet files for a miner with S3-compatible metadata."""
        prefix = self.miner_prefix(miner_hotkey)
        root = self.data_dir / prefix
        if not root.exists():
            return []

        files: List[Dict[str, Any]] = []
        for path in root.rglob("*.parquet"):
            rel = path.relative_to(self.data_dir).as_posix()
            stat = path.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            files.append(
                {
                    "key": rel,
                    "size": stat.st_size,
                    "last_modified": mtime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                }
            )
        return sorted(files, key=lambda f: f["key"])

    def save_upload(self, key: str, content: bytes) -> Path:
        """Persist an uploaded file at the given S3-style key."""
        dest = self.resolve_key_path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            f.write(content)
        return dest

    def read_file(self, key: str) -> Optional[bytes]:
        path = self.resolve_key_path(unquote(key))
        if not path.exists() or not path.is_file():
            return None
        with open(path, "rb") as f:
            return f.read()

    def build_s3_list_xml(
        self,
        miner_hotkey: str,
        continuation_token: Optional[str] = None,
        max_keys: int = 1000,
    ) -> str:
        """Build an S3 ListBucketResult XML document for ValidatorS3Access."""
        all_files = self.list_parquet_files(miner_hotkey)
        start = 0
        if continuation_token:
            try:
                start = int(continuation_token)
            except ValueError:
                start = 0

        page = all_files[start : start + max_keys]
        is_truncated = (start + max_keys) < len(all_files)
        next_token = str(start + max_keys) if is_truncated else ""

        contents = []
        for f in page:
            key = quote(f["key"], safe="/=")
            contents.append(
                f"<Contents>"
                f"<Key>{key}</Key>"
                f"<Size>{f['size']}</Size>"
                f"<LastModified>{f['last_modified']}</LastModified>"
                f"</Contents>"
            )

        token_xml = (
            f"<NextContinuationToken>{next_token}</NextContinuationToken>"
            if is_truncated
            else ""
        )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"<IsTruncated>{str(is_truncated).lower()}</IsTruncated>"
            f"{token_xml}"
            f"{''.join(contents)}"
            "</ListBucketResult>"
        )

    # ------------------------------------------------------------------
    # On-demand job storage
    # ------------------------------------------------------------------

    def _job_path(self, job_id: str) -> Path:
        return self.data_dir / "on_demand" / "jobs" / f"{job_id}.json"

    def create_od_job(self, job_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new on-demand job and persist it."""
        with self._lock:
            job_id = job_payload.get("id") or str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            expire_at = now + timedelta(minutes=job_payload.get("ttl_minutes", 30))

            record = {
                "id": job_id,
                "created_at": now.isoformat(),
                "expire_at": expire_at.isoformat(),
                "job": job_payload.get("job", {}),
                "start_date": job_payload.get("start_date"),
                "end_date": job_payload.get("end_date"),
                "limit": job_payload.get("limit", 100),
                "keyword_mode": job_payload.get("keyword_mode", "any"),
                "status": "active",
            }
            with open(self._job_path(job_id), "w") as f:
                json.dump(record, f, indent=2, default=str)
            bt.logging.info(f"Local API: created OD job {job_id}")
            return record

    def list_od_jobs(self) -> List[Dict[str, Any]]:
        jobs = []
        jobs_dir = self.data_dir / "on_demand" / "jobs"
        for path in jobs_dir.glob("*.json"):
            try:
                with open(path) as f:
                    jobs.append(json.load(f))
            except Exception as e:
                bt.logging.warning(f"Failed to read job {path}: {e}")
        return jobs

    @staticmethod
    def _parse_dt(value: str) -> datetime:
        """Parse an ISO datetime string, always returning a timezone-aware value."""
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def get_active_od_jobs(
        self,
        since: datetime,
        *,
        created_since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return non-expired active jobs, optionally limited to a creation window.

        ``since`` is kept for API compatibility with miner poll requests.
        When ``created_since`` is set, only jobs created at or after that time
        are returned (used to ignore pre-session jobs on miner restart).
        """
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        if created_since is not None and created_since.tzinfo is None:
            created_since = created_since.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        active = []
        for job in self.list_od_jobs():
            expire = self._parse_dt(job["expire_at"])
            if expire <= now or job.get("status") != "active":
                continue
            if created_since is not None:
                created_raw = job.get("created_at")
                if not created_raw:
                    continue
                created = self._parse_dt(created_raw)
                if created < created_since:
                    continue
            active.append(job)
        return active

    def get_expired_jobs(
        self,
        expired_since: datetime,
        expired_until: datetime,
        *,
        created_since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return jobs whose expiry falls within the given window."""
        if expired_since.tzinfo is None:
            expired_since = expired_since.replace(tzinfo=timezone.utc)
        if expired_until.tzinfo is None:
            expired_until = expired_until.replace(tzinfo=timezone.utc)
        if created_since is not None and created_since.tzinfo is None:
            created_since = created_since.replace(tzinfo=timezone.utc)

        result = []
        for job in self.list_od_jobs():
            expire = self._parse_dt(job["expire_at"])
            if not (expired_since <= expire <= expired_until):
                continue
            if created_since is not None:
                created_raw = job.get("created_at")
                if not created_raw:
                    continue
                created = self._parse_dt(created_raw)
                if created < created_since:
                    continue
            result.append(job)
        return result

    def save_od_submission(
        self, job_id: str, miner_hotkey: str, content: bytes
    ) -> Dict[str, Any]:
        """Store a miner's on-demand submission JSON."""
        with self._lock:
            sub_dir = (
                self.data_dir / "on_demand" / "submissions" / job_id / miner_hotkey
            )
            sub_dir.mkdir(parents=True, exist_ok=True)
            dest = sub_dir / "data.json"
            with open(dest, "wb") as f:
                f.write(content)

            stat = dest.stat()
            now = datetime.now(timezone.utc)
            return {
                "job_id": job_id,
                "miner_hotkey": miner_hotkey,
                "s3_path": str(dest.relative_to(self.data_dir)),
                "s3_content_length": stat.st_size,
                "s3_last_modified": now.isoformat(),
                "submitted_at": now.isoformat(),
            }

    def get_od_submission(
        self, job_id: str, miner_hotkey: str
    ) -> Optional[Dict[str, Any]]:
        dest = (
            self.data_dir
            / "on_demand"
            / "submissions"
            / job_id
            / miner_hotkey
            / "data.json"
        )
        if not dest.exists():
            return None
        stat = dest.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        return {
            "job_id": job_id,
            "miner_hotkey": miner_hotkey,
            "s3_path": str(dest.relative_to(self.data_dir)),
            "s3_content_length": stat.st_size,
            "s3_last_modified": mtime.isoformat(),
            "submitted_at": mtime.isoformat(),
        }

    def read_od_submission_bytes(self, job_id: str, miner_hotkey: str) -> Optional[bytes]:
        dest = (
            self.data_dir
            / "on_demand"
            / "submissions"
            / job_id
            / miner_hotkey
            / "data.json"
        )
        if not dest.exists():
            return None
        with open(dest, "rb") as f:
            return f.read()

    def _delete_od_submissions_for_job(self, job_id: str) -> tuple[int, int]:
        """Remove all miner submission files for one job. Returns (files, bytes)."""
        sub_root = self.data_dir / "on_demand" / "submissions" / job_id
        return self._wipe_directory(sub_root)

    def delete_od_job(
        self, job_id: str, *, include_submissions: bool = True
    ) -> Dict[str, Any]:
        """Delete one OD job file and optionally its miner submissions."""
        with self._lock:
            removed = {
                "job_id": job_id,
                "job_removed": False,
                "submission_files": 0,
                "submission_bytes": 0,
            }
            path = self._job_path(job_id)
            if path.exists():
                path.unlink()
                removed["job_removed"] = True

            if include_submissions:
                fcount, fbytes = self._delete_od_submissions_for_job(job_id)
                removed["submission_files"] = fcount
                removed["submission_bytes"] = fbytes

            if removed["job_removed"] or removed["submission_files"]:
                bt.logging.info(
                    "Deleted OD job %s (job=%s, submission_files=%s)",
                    job_id,
                    removed["job_removed"],
                    removed["submission_files"],
                )
            return removed

    def clear_od_jobs(self, *, include_submissions: bool = True) -> Dict[str, Any]:
        """Delete all OD job definition files and optionally all submissions."""
        with self._lock:
            jobs_root = self.data_dir / "on_demand" / "jobs"
            job_files, _ = self._count_files_under(jobs_root)
            if jobs_root.exists():
                shutil.rmtree(jobs_root)
            jobs_root.mkdir(parents=True, exist_ok=True)

            submission_files = 0
            submission_bytes = 0
            if include_submissions:
                sub_root = self.data_dir / "on_demand" / "submissions"
                submission_files, submission_bytes = self._wipe_directory(sub_root)

            result = {
                "jobs_removed": job_files,
                "submission_files": submission_files,
                "submission_bytes": submission_bytes,
                "data_dir": str(self.data_dir),
            }
            bt.logging.info(
                "Cleared OD jobs: %s job file(s), %s submission file(s)",
                job_files,
                submission_files,
            )
            return result

    def get_od_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        path = self._job_path(job_id)
        if not path.exists():
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None

    def list_od_submissions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List miner OD submission files newest-first."""
        root = self.data_dir / "on_demand" / "submissions"
        if not root.exists():
            return []

        entries: List[Dict[str, Any]] = []
        for job_dir in root.iterdir():
            if not job_dir.is_dir():
                continue
            job_id = job_dir.name
            job_record = self.get_od_job(job_id) or {}
            job_inner = job_record.get("job", {})
            if isinstance(job_inner, dict) and "job" in job_inner:
                job_inner = job_inner.get("job", job_inner)

            for hotkey_dir in job_dir.iterdir():
                if not hotkey_dir.is_dir():
                    continue
                data_path = hotkey_dir / "data.json"
                if not data_path.exists():
                    continue

                stat = data_path.stat()
                entity_count = 0
                parse_ok = True
                try:
                    with open(data_path) as f:
                        payload = json.load(f)
                    entity_count = len(payload.get("data_entities", []))
                except Exception:
                    parse_ok = False

                entries.append(
                    {
                        "job_id": job_id,
                        "miner_hotkey": hotkey_dir.name,
                        "file_path": str(data_path),
                        "relative_path": data_path.relative_to(self.data_dir).as_posix(),
                        "size_bytes": stat.st_size,
                        "modified_at": datetime.fromtimestamp(
                            stat.st_mtime, tz=timezone.utc
                        ).isoformat(),
                        "entity_count": entity_count,
                        "parse_ok": parse_ok,
                        "job_platform": job_inner.get("platform"),
                        "job_keywords": job_inner.get("keywords"),
                        "job_usernames": job_inner.get("usernames"),
                    }
                )

        entries.sort(key=lambda item: item["modified_at"], reverse=True)
        return entries[: max(1, limit)]

    def _count_files_under(self, root: Path) -> tuple[int, int]:
        """Return (file_count, total_bytes) for all files under root."""
        if not root.exists():
            return 0, 0
        count = 0
        total_bytes = 0
        for path in root.rglob("*"):
            if path.is_file():
                count += 1
                total_bytes += path.stat().st_size
        return count, total_bytes

    def _wipe_directory(self, root: Path) -> tuple[int, int]:
        """Delete all contents under root and recreate the directory."""
        count, total_bytes = self._count_files_under(root)
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)
        return count, total_bytes

    def clear_miner_submission_history(
        self,
        *,
        include_od_submissions: bool = True,
        include_parquet: bool = True,
    ) -> Dict[str, Any]:
        """Remove on-disk miner upload history (OD JSON + parquet)."""
        with self._lock:
            removed: Dict[str, Any] = {
                "od_submissions_files": 0,
                "od_submissions_bytes": 0,
                "parquet_files": 0,
                "parquet_bytes": 0,
            }

            if include_od_submissions:
                sub_root = self.data_dir / "on_demand" / "submissions"
                fcount, fbytes = self._wipe_directory(sub_root)
                removed["od_submissions_files"] = fcount
                removed["od_submissions_bytes"] = fbytes

            if include_parquet:
                data_root = self.data_dir / "data"
                fcount, fbytes = self._wipe_directory(data_root)
                removed["parquet_files"] = fcount
                removed["parquet_bytes"] = fbytes

            removed["data_dir"] = str(self.data_dir)
            removed["total_files_removed"] = (
                removed["od_submissions_files"] + removed["parquet_files"]
            )
            removed["total_bytes_removed"] = (
                removed["od_submissions_bytes"] + removed["parquet_bytes"]
            )
            bt.logging.info(
                "Cleared miner submission history: "
                f"{removed['total_files_removed']} file(s), "
                f"{removed['total_bytes_removed']} byte(s) under {self.data_dir}"
            )
            return removed

    def get_stats(self) -> Dict[str, Any]:
        """Return storage statistics for the dashboard."""
        parquet_count = sum(1 for _ in (self.data_dir / "data").rglob("*.parquet"))
        job_count = len(list((self.data_dir / "on_demand" / "jobs").glob("*.json")))
        submission_count = sum(
            1 for _ in (self.data_dir / "on_demand" / "submissions").rglob("data.json")
        )
        total_bytes = sum(
            f.stat().st_size for f in self.data_dir.rglob("*") if f.is_file()
        )
        return {
            "data_dir": str(self.data_dir),
            "parquet_files": parquet_count,
            "od_jobs": job_count,
            "od_submissions": submission_count,
            "total_bytes": total_bytes,
        }
