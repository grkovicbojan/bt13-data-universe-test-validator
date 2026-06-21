"""
Automatic on-demand job scheduler for testnet.

Creates OD jobs on a configurable interval when no external Constellation
API is available. Supports keywords from the UI or from an external label file.
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import bittensor as bt
import httpx

from vali_utils.dashboard.events import get_event_bus
from vali_utils.dashboard.label_loader import (
    MAX_KEYWORDS_PER_OD_JOB,
    OdJobFileEntry,
    load_od_job_entries,
    pick_next_spec,
)
from vali_utils.dashboard.od_job_utils import (
    build_od_job_payload,
    build_od_job_payload_from_entry,
)
from vali_utils.dashboard.settings import DashboardSettings, SettingsManager


class OnDemandJobScheduler:
    """Background thread that posts OD jobs to the local API."""

    POLL_SECONDS = 10
    DEFAULT_INTERVAL_MINUTES = 5

    def __init__(self, settings: SettingsManager):
        self._settings = settings
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._last_run: Optional[datetime] = None
        self._jobs_created = 0
        self._last_error: Optional[str] = None
        self._was_enabled = False
        self._last_label_used: Optional[str] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="od-job-scheduler"
        )
        self._thread.start()
        bt.logging.info("On-demand job scheduler started")

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    @staticmethod
    def is_enabled(settings: DashboardSettings) -> bool:
        """Return True when automatic OD job creation should run."""
        return bool(
            settings.auto_od_enabled and settings.auto_od_interval_minutes > 0
        )

    @property
    def status(self) -> dict:
        s = self._settings.get()
        enabled = self.is_enabled(s)
        next_run = None
        if enabled and self._last_run and s.auto_od_interval_minutes > 0:
            next_run = (
                self._last_run + timedelta(minutes=s.auto_od_interval_minutes)
            ).isoformat()
        label_file_ok = False
        label_count = 0
        if s.auto_od_keyword_source == "file" and s.auto_od_label_file_path:
            try:
                label_count = len(self._load_entries(s))
                label_file_ok = label_count > 0
            except Exception as e:
                self._last_error = str(e)

        return {
            "enabled": enabled,
            "auto_od_enabled": s.auto_od_enabled,
            "interval_minutes": s.auto_od_interval_minutes,
            "keyword_source": s.auto_od_keyword_source,
            "label_file_path": s.auto_od_label_file_path,
            "label_file_ok": label_file_ok,
            "label_count": label_count,
            "label_cursor": s.auto_od_label_cursor,
            "last_label_used": self._last_label_used,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "next_run": next_run,
            "jobs_created": self._jobs_created,
            "last_error": self._last_error,
        }

    def sync_after_settings_save(
        self, old: DashboardSettings, new: DashboardSettings
    ):
        """Apply scheduler state after dashboard settings are saved."""
        old_enabled = self.is_enabled(old)
        new_enabled = self.is_enabled(new)

        if not new_enabled:
            self._was_enabled = False
            bt.logging.info("OD scheduler disabled via settings")
            return

        label_source_changed = old.auto_od_keyword_source != new.auto_od_keyword_source
        label_file_changed = (
            old.auto_od_label_file_path != new.auto_od_label_file_path
            or old.label_file_platform_filter != new.label_file_platform_filter
            or old.auto_od_label_rotation != new.auto_od_label_rotation
        )

        if not old_enabled or self._last_run is None or self._jobs_created == 0:
            bt.logging.info(
                f"OD scheduler enabled (interval={new.auto_od_interval_minutes} min), "
                "creating job now"
            )
            self._last_run = None
            self._maybe_create_job(new, force=True)
        elif label_source_changed or label_file_changed:
            bt.logging.info("OD scheduler label source changed, creating job now")
            self._maybe_create_job(new, force=True)
        elif (
            new.auto_od_interval_minutes < old.auto_od_interval_minutes
            and self._last_run is not None
        ):
            elapsed = (
                datetime.now(timezone.utc) - self._last_run
            ).total_seconds() / 60
            if elapsed >= new.auto_od_interval_minutes:
                self._maybe_create_job(new, force=True)

        self._was_enabled = True

    def _loop(self):
        while not self._stop.is_set():
            settings = self._settings.get()
            if self.is_enabled(settings):
                self._maybe_create_job(settings)
            elif self._was_enabled:
                self._was_enabled = False

            self._stop.wait(timeout=self.POLL_SECONDS)

    def _maybe_create_job(self, settings: DashboardSettings, force: bool = False):
        if not self.is_enabled(settings):
            return

        now = datetime.now(timezone.utc)
        interval = settings.auto_od_interval_minutes

        if not force and self._last_run:
            elapsed = (now - self._last_run).total_seconds() / 60
            if elapsed < interval:
                return

        try:
            result, label_used = self._create_job(settings)
            self._last_run = now
            self._jobs_created += 1
            self._last_error = None
            self._last_label_used = label_used
            job_id = result.get("id", "?")
            bt.logging.success(
                f"OD scheduler created job {job_id} "
                f"(#{self._jobs_created}, interval={interval} min, labels={label_used})"
            )
            get_event_bus().publish(
                "od_job_created",
                uid=-1,
                hotkey="scheduler",
                data={
                    "job_id": job_id,
                    "source": "auto_scheduler",
                    "labels": label_used,
                    "message": f"Auto OD job created: {label_used}",
                },
            )
        except Exception as e:
            self._last_error = str(e)
            bt.logging.warning(f"OD scheduler failed to create job: {e}")

    def create_job_now(self, settings=None) -> dict:
        """Create a job immediately (manual trigger from dashboard)."""
        settings = settings or self._settings.get()
        result, _ = self._create_job(settings)
        self._last_run = datetime.now(timezone.utc)
        self._jobs_created += 1
        self._last_error = None
        return result

    def _load_entries(self, settings: DashboardSettings) -> list[OdJobFileEntry]:
        if settings.auto_od_keyword_source != "file":
            return []
        if not settings.auto_od_label_file_path:
            raise ValueError("Job file path is empty")
        platform_filter = settings.label_file_platform_filter or "all"
        return load_od_job_entries(
            settings.auto_od_label_file_path,
            platform_filter=platform_filter,
        )

    def _resolve_file_entry(self, settings: DashboardSettings) -> tuple[OdJobFileEntry, dict]:
        entries = self._load_entries(settings)
        if not entries:
            raise ValueError("Job file contains no entries for the selected platform filter")

        rotation = settings.auto_od_label_rotation
        if rotation not in ("sequential", "random"):
            rotation = "sequential"

        entry, new_cursor = pick_next_spec(
            entries,
            rotation=rotation,  # type: ignore
            cursor=settings.auto_od_label_cursor,
        )
        self._settings.update(auto_od_label_cursor=new_cursor)
        return entry, build_od_job_payload_from_entry(entry)

    def _create_job(self, settings: DashboardSettings) -> tuple[dict, str]:
        if settings.auto_od_keyword_source == "file":
            entry, payload = self._resolve_file_entry(settings)
            label_desc = entry.summary()
        else:
            keywords = list(settings.auto_od_keywords)[:MAX_KEYWORDS_PER_OD_JOB]
            label_desc = ",".join(keywords) if keywords else "manual"
            payload = build_od_job_payload(
                settings.auto_od_platform.lower(),
                keywords,
                subreddit=settings.auto_od_subreddit,
                usernames=settings.auto_od_usernames or None,
                limit=settings.auto_od_limit,
                ttl_minutes=settings.auto_od_ttl_minutes,
                keyword_mode=settings.auto_od_keyword_mode,
                start_date=settings.auto_od_start_date or None,
                end_date=settings.auto_od_end_date or None,
            )

        url = f"{settings.local_api_url.rstrip('/')}/on-demand/constellation/jobs"
        with httpx.Client(timeout=15) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json(), label_desc
