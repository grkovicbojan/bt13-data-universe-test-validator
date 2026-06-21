"""
Runtime settings for the validator dashboard.

All options exposed in the web UI are stored here and read by the evaluator
without restarting the validator process.
"""

import json
import os
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class DashboardSettings:
    """Mutable runtime configuration controlled via the web dashboard."""

    # When set, only these miner UIDs are evaluated (empty = all miners).
    target_miner_uids: List[int] = field(default_factory=list)

    # Seconds between evaluation cycles when target_miner_uids is set (avoids tight loops).
    # Align with miner LOCAL_TESTNET_EVAL_PERIOD (5 min) when using local API.
    target_eval_interval_seconds: int = 300

    # Optional UID -> "ip:port" overrides for local testnet (e.g. {"179": "127.0.0.1:8093"}).
    miner_axon_overrides: Dict[str, str] = field(default_factory=dict)

    # Skip S3/parquet validation (useful when no uploads exist yet).
    skip_s3_validation: bool = False

    # Skip P2P index/bucket/scraper validation (useful when testing S3/OD only).
    skip_p2p_validation: bool = False

    # Re-run S3/parquet validation after this many minutes (default ~2 hours).
    s3_validation_interval_minutes: int = 120

    # P2P scraper depth: "sample" (1–2 entities) or "full" (every entity in bucket).
    # Basic/uniqueness checks always run on the full bucket response.
    p2p_scraper_validation_mode: str = "sample"

    # S3 parquet depth: "sample" (~10% files) or "full" (all active job files).
    s3_validation_mode: str = "sample"

    # On-demand depth: "sample" (3 jobs, 5 entities) or "full" (all jobs/entities).
    od_validation_mode: str = "sample"

    # Local testnet: count P2P toward capped score without requiring S3/OD first.
    relax_weight_caps: bool = True

    # Number of miners evaluated per batch.
    eval_batch_size: int = 5

    # Local Data Universe API URL (replaces remote S3 auth service).
    local_api_url: str = "http://localhost:8100"

    # Local API data directory on disk.
    local_api_data_dir: str = ""

    # Local API port.
    local_api_port: int = 8100

    # Master switch for automatic on-demand job creation.
    auto_od_enabled: bool = False

    # Auto-generate on-demand jobs on this interval (minutes). 0 = disabled.
    auto_od_interval_minutes: int = 5

    # Template used for auto-generated X on-demand jobs.
    auto_od_platform: str = "x"
    auto_od_keywords: List[str] = field(default_factory=lambda: ["#bittensor"])
    auto_od_usernames: List[str] = field(default_factory=list)
    auto_od_subreddit: str = ""
    auto_od_limit: int = 50
    auto_od_keyword_mode: str = "any"
    auto_od_ttl_minutes: int = 30
    # ISO datetime strings; empty = use default lookback (7 days) ending at now.
    auto_od_start_date: str = ""
    auto_od_end_date: str = ""

    # Keyword source for auto OD jobs: "manual" (UI field) or "file" (external file).
    auto_od_keyword_source: str = "manual"

    # Path to external OD job JSON file (array of x/reddit job definitions).
    auto_od_label_file_path: str = ""

    # Platform filter for external job file (batch preview/create + auto file source).
    label_file_platform_filter: str = "all"

    # How to pick the next label from the file: sequential or random.
    auto_od_label_rotation: str = "sequential"

    # Cursor for sequential rotation (persisted across restarts).
    auto_od_label_cursor: int = 0

    # Pause the evaluation loop without stopping the validator.
    evaluation_paused: bool = False

    # Trigger a one-shot evaluation of target miners immediately.
    trigger_eval_now: bool = False


class SettingsManager:
    """Thread-safe settings store with optional JSON persistence."""

    def __init__(self, persist_path: Optional[str] = None):
        self._lock = threading.RLock()
        self._settings = DashboardSettings()
        self._persist_path = persist_path
        if persist_path and os.path.exists(persist_path):
            self._load()

    def get(self) -> DashboardSettings:
        with self._lock:
            return DashboardSettings(**asdict(self._settings))

    def update(self, **kwargs) -> DashboardSettings:
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self._settings, key):
                    setattr(self._settings, key, value)
            self._save()
            return DashboardSettings(**asdict(self._settings))

    def consume_trigger_eval(self) -> bool:
        """Return True once if a manual eval trigger was requested."""
        with self._lock:
            if self._settings.trigger_eval_now:
                self._settings.trigger_eval_now = False
                self._save()
                return True
            return False

    def _save(self):
        if not self._persist_path:
            return
        Path(self._persist_path).parent.mkdir(parents=True, exist_ok=True)
        with open(self._persist_path, "w") as f:
            json.dump(asdict(self._settings), f, indent=2)

    def _load(self):
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
            # Migrate legacy configs that only used interval > 0 as the enable flag.
            if "auto_od_enabled" not in data and data.get("auto_od_interval_minutes", 0) > 0:
                data["auto_od_enabled"] = True
            if "relax_weight_caps" not in data:
                data["relax_weight_caps"] = True
            if "s3_validation_interval_minutes" not in data:
                data["s3_validation_interval_minutes"] = 120
            for key, default in (
                ("p2p_scraper_validation_mode", "sample"),
                ("s3_validation_mode", "sample"),
                ("od_validation_mode", "sample"),
                ("label_file_platform_filter", "all"),
            ):
                if key not in data:
                    data[key] = default
            with self._lock:
                for key, value in data.items():
                    if hasattr(self._settings, key):
                        setattr(self._settings, key, value)
        except Exception:
            pass


# Module-level singleton used by evaluator and dashboard routes.
_settings_manager: Optional[SettingsManager] = None


def get_settings_manager(persist_path: Optional[str] = None) -> SettingsManager:
    """Return the shared settings manager, binding a persist path when provided."""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = SettingsManager(persist_path)
    elif persist_path and not _settings_manager._persist_path:
        # Late-bind persist path (local_api may init before dashboard).
        _settings_manager._persist_path = persist_path
        if os.path.exists(persist_path):
            _settings_manager._load()
        else:
            _settings_manager._save()
    return _settings_manager
