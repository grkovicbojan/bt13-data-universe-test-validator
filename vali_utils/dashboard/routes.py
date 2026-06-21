"""
Dashboard API routes mounted on the validator FastAPI server.

Provides:
  - Real-time evaluation event stream (SSE)
  - Runtime settings read/write
  - Miner score snapshots
  - On-demand job management
  - Metagraph miner list for target selection
"""

import asyncio
import json
import os
import shutil
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import bittensor as bt
import httpx
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from common.constants import MIN_OD_TTL_MINUTES
from vali_utils.dashboard.events import get_event_bus
from vali_utils.dashboard.label_loader import (
    MAX_KEYWORDS_PER_OD_JOB,
    load_od_job_entries,
    preview_od_jobs,
)
from vali_utils.dashboard.od_job_utils import (
    VALID_KEYWORD_MODES,
    build_od_job_payload,
    build_od_job_payload_from_entry,
)
from vali_utils.dashboard.score_metrics import (
    build_miner_score_row,
    get_score_history,
)
from vali_utils.dashboard.scheduler import OnDemandJobScheduler
from vali_utils.dashboard.settings import DashboardSettings, get_settings_manager
from vali_utils.dashboard.validation_reports import get_validation_reports
from vali_utils.dashboard.validation_stats import get_validation_stats

router = APIRouter()


class SettingsUpdateRequest(BaseModel):
    """Partial settings update from the web UI."""

    target_miner_uids: Optional[List[int]] = None
    skip_s3_validation: Optional[bool] = None
    skip_p2p_validation: Optional[bool] = None
    s3_validation_interval_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    p2p_scraper_validation_mode: Optional[str] = None
    s3_validation_mode: Optional[str] = None
    od_validation_mode: Optional[str] = None
    relax_weight_caps: Optional[bool] = None
    eval_batch_size: Optional[int] = Field(default=None, ge=1, le=50)
    local_api_url: Optional[str] = None
    local_api_data_dir: Optional[str] = None
    local_api_port: Optional[int] = None
    auto_od_enabled: Optional[bool] = None
    auto_od_interval_minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    auto_od_platform: Optional[str] = None
    auto_od_keywords: Optional[List[str]] = None
    auto_od_usernames: Optional[List[str]] = None
    auto_od_subreddit: Optional[str] = None
    auto_od_limit: Optional[int] = Field(default=None, ge=1, le=1000)
    auto_od_keyword_mode: Optional[str] = None
    auto_od_ttl_minutes: Optional[int] = Field(default=None, ge=MIN_OD_TTL_MINUTES, le=1440)
    auto_od_keyword_source: Optional[str] = None
    auto_od_label_file_path: Optional[str] = None
    label_file_platform_filter: Optional[str] = None
    auto_od_label_rotation: Optional[str] = None
    auto_od_start_date: Optional[str] = None
    auto_od_end_date: Optional[str] = None
    evaluation_paused: Optional[bool] = None
    target_eval_interval_seconds: Optional[int] = Field(default=None, ge=5, le=3600)
    miner_axon_overrides: Optional[Dict[str, str]] = None


class ResetScoresRequest(BaseModel):
    """Reset local validator scores (does not change on-chain weights)."""

    uids: Optional[List[int]] = None
    clear_history: bool = True
    clear_validation_reports: bool = True


class ClearMinerSubmissionsRequest(BaseModel):
    """Delete miner upload files from local API storage."""

    include_od_submissions: bool = True
    include_parquet: bool = True


class ClearOdJobsRequest(BaseModel):
    """Delete all local OD job definitions."""

    include_submissions: bool = True


class CreateOdJobRequest(BaseModel):
    """Manual on-demand job creation from the dashboard."""

    platform: str = "x"
    keywords: List[str] = Field(
        default_factory=lambda: ["#bittensor"],
        max_length=MAX_KEYWORDS_PER_OD_JOB,
    )
    usernames: List[str] = Field(default_factory=list)
    subreddit: str = ""
    limit: int = Field(default=50, ge=1, le=1000)
    keyword_mode: str = "any"
    ttl_minutes: int = Field(default=30, ge=MIN_OD_TTL_MINUTES, le=1440)
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class CreateOdJobsFromFileRequest(BaseModel):
    """Create one or more OD jobs from an external JSON job file."""

    file_path: str
    platform: str = "all"
    start_index: int = Field(default=0, ge=0)
    count: int = Field(default=1, ge=1, le=50)


MAX_BATCH_OD_JOBS = 50


def _get_validator(request: Request):
    if not hasattr(request.app.state, "validator"):
        raise HTTPException(503, "Validator not initialized")
    return request.app.state.validator


def _get_scheduler(request: Request) -> Optional[OnDemandJobScheduler]:
    return getattr(request.app.state, "od_scheduler", None)


def _resolve_local_storage(validator) -> "LocalApiStorage":
    from vali_utils.local_api.storage import LocalApiStorage

    storage = getattr(getattr(validator, "local_api", None), "storage", None)
    if storage is not None:
        return storage

    settings = get_settings_manager().get()
    data_dir = settings.local_api_data_dir
    if not data_dir or not os.path.isdir(data_dir):
        raise HTTPException(404, "Local API storage not configured")
    return LocalApiStorage(data_dir)


@router.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Serve the evaluation dashboard HTML."""
    static_path = (
        __import__("pathlib").Path(__file__).parent / "static" / "index.html"
    )
    if not static_path.exists():
        raise HTTPException(500, "Dashboard UI not found")
    return HTMLResponse(static_path.read_text())


@router.get("/static/{filename}")
async def dashboard_static(filename: str):
    static_dir = __import__("pathlib").Path(__file__).parent / "static"
    path = static_dir / filename
    if not path.exists():
        raise HTTPException(404)
    media = "text/css" if filename.endswith(".css") else "application/javascript"
    return HTMLResponse(path.read_text(), media_type=media)


@router.get("/api/status")
async def get_status(request: Request, validator=Depends(_get_validator)):
    """Return validator health and network summary."""
    settings = get_settings_manager().get()
    scorer = validator.evaluator.get_scorer()
    with scorer.lock:
        scores = scorer.scores.tolist()
        cred = scorer.miner_credibility.squeeze().tolist()
        s3_boost = scorer.s3_boosts.tolist()
        od_boost = scorer.ondemand_boosts.tolist()

    return {
        "healthy": validator.is_healthy(),
        "netuid": validator.config.netuid,
        "validator_hotkey": validator.wallet.hotkey.ss58_address,
        "validator_uid": validator.uid,
        "block": int(validator.metagraph.block),
        "evaluation_cycles": validator.evaluation_cycles_since_startup,
        "settings": asdict(settings),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/miners")
async def list_miners(request: Request, validator=Depends(_get_validator)):
    """Return all miners on the metagraph for target selection."""
    from common import utils

    miner_uids = utils.get_miner_uids(
        validator.metagraph, validator.config.vpermit_rao_limit
    )
    miners = []
    for uid in miner_uids:
        miners.append(
            {
                "uid": uid,
                "hotkey": validator.metagraph.hotkeys[uid],
                "ip": validator.metagraph.axons[uid].ip,
                "port": validator.metagraph.axons[uid].port,
                "stake": float(validator.metagraph.S[uid]),
            }
        )
    return {"miners": miners}


@router.get("/api/scores")
async def get_scores(request: Request, validator=Depends(_get_validator)):
    """Return current miner scores for the dashboard table."""
    scorer = validator.evaluator.get_scorer()
    settings = get_settings_manager().get()
    target_set = set(settings.target_miner_uids)

    relax_caps = settings.relax_weight_caps
    capped_scores = scorer.get_scores_for_weights(relax_service_cap=relax_caps)
    rows = []
    for uid in range(len(validator.metagraph.hotkeys)):
        if target_set and uid not in target_set:
            continue
        if not utils_is_miner(uid, validator):
            continue
        row = build_miner_score_row(
            uid,
            validator.metagraph.hotkeys[uid],
            scorer,
            validator.metagraph,
            capped_scores,
            relax_service_cap=relax_caps,
        )
        rows.append(row)
        get_score_history().record(uid, row)

    rows.sort(key=lambda r: r["capped_score"], reverse=True)
    return {
        "scores": rows,
        "disable_set_weights": bool(
            getattr(validator.config.neuron, "disable_set_weights", False)
        ),
    }


@router.get("/api/scores/history")
async def get_score_history_api(
    request: Request,
    validator=Depends(_get_validator),
    uid: Optional[int] = None,
    limit: int = 60,
):
    """Return score time-series for dashboard charts."""
    settings = get_settings_manager().get()
    target_set = set(settings.target_miner_uids)
    history = get_score_history()

    if uid is not None:
        if target_set and uid not in target_set:
            raise HTTPException(404, f"UID {uid} not in target miners")
        return {"uid": uid, "history": history.get(uid, limit)}

    uids = list(target_set) if target_set else [
        u for u in range(len(validator.metagraph.hotkeys))
        if utils_is_miner(u, validator)
    ]
    return {"history": history.get_multi(uids, limit)}


def utils_is_miner(uid: int, validator) -> bool:
    from common import utils
    return utils.is_miner(
        uid, validator.metagraph, validator.config.vpermit_rao_limit
    )


@router.get("/api/events")
async def get_events(limit: int = 100):
    """Return recent evaluation events (polling fallback)."""
    limit = max(1, min(limit, 500))
    return {"events": get_event_bus().get_recent(limit)}


@router.post("/api/events/clear")
async def clear_events():
    """Delete all buffered Live Evaluation Feed events."""
    removed = get_event_bus().clear()
    return {
        "status": "ok",
        "removed": removed,
        "message": f"Cleared {removed} evaluation feed event(s).",
    }


@router.get("/api/validation-stats")
async def get_validation_stats_api(
    request: Request,
    validator=Depends(_get_validator),
    uid: Optional[int] = None,
):
    """Return per-path job/entity validation stats (session + per miner)."""
    settings = get_settings_manager().get()
    target_set = set(settings.target_miner_uids)
    uids = None
    if uid is not None:
        uids = [uid]
    elif target_set:
        uids = list(target_set)
    return get_validation_stats().to_api_response(uids=uids)


@router.get("/api/validation-failures")
async def get_validation_failures(
    uid: Optional[int] = None,
    validation_type: Optional[str] = None,
    limit: int = 50,
):
    """Return recent validation failure reports for the dashboard."""
    limit = max(1, min(limit, 150))
    return {
        "failures": get_validation_reports().get_recent(
            limit=limit,
            uid=uid,
            validation_type=validation_type,
        )
    }


@router.post("/api/validation-failures/clear")
async def clear_validation_failures():
    """Delete all validation failure reports from dashboard memory."""
    removed = get_validation_reports().clear()
    return {
        "status": "ok",
        "removed": removed,
        "message": f"Cleared {removed} validation failure report(s).",
    }


@router.get("/api/events/stream")
async def event_stream(request: Request):
    """Server-Sent Events stream for real-time evaluation updates."""

    async def generate():
        from vali_utils.dashboard.events import EvaluationEvent

        bus = get_event_bus()
        subscriber = bus.subscribe()
        # Replay recent history for late-connecting clients.
        for event_dict in bus.get_recent(50):
            yield bus.to_sse(EvaluationEvent(**event_dict))
        bus.sync_subscriber(subscriber)

        while True:
            if await request.is_disconnected():
                break
            triggered = await asyncio.get_event_loop().run_in_executor(
                None, lambda: bus.wait_for_event(subscriber, timeout=25.0)
            )
            if triggered:
                for event_dict in bus.drain_subscriber(subscriber):
                    yield bus.to_sse(EvaluationEvent(**event_dict))
            else:
                # Keep-alive ping
                yield ": keepalive\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/settings")
async def get_settings():
    return {"settings": asdict(get_settings_manager().get())}


_VALIDATION_MODES = {"sample", "full"}


@router.put("/api/settings")
async def update_settings(req: SettingsUpdateRequest, request: Request):
    mgr = get_settings_manager()
    old = mgr.get()
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    for mode_key in (
        "p2p_scraper_validation_mode",
        "s3_validation_mode",
        "od_validation_mode",
    ):
        if mode_key in updates and updates[mode_key] not in _VALIDATION_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"{mode_key} must be 'sample' or 'full'",
            )
    if "auto_od_keyword_mode" in updates and updates["auto_od_keyword_mode"] not in VALID_KEYWORD_MODES:
        raise HTTPException(
            status_code=400,
            detail="auto_od_keyword_mode must be 'any' or 'all'",
        )
    settings = mgr.update(**updates)

    # Keep scheduler in sync with every settings save.
    scheduler = _get_scheduler(request)
    if scheduler:
        scheduler.sync_after_settings_save(old, settings)

    return {"settings": asdict(settings)}


@router.post("/api/scores/reset")
async def reset_scores(
    req: ResetScoresRequest,
    request: Request,
    validator=Depends(_get_validator),
):
    """Reset local miner scores, boosts, credibility, and dashboard history."""
    settings = get_settings_manager().get()
    reset_uids = validator.evaluator.reset_scores(
        uids=req.uids,
        clear_history=req.clear_history,
        clear_validation_reports=req.clear_validation_reports,
    )

    # Push fresh zeroed snapshots so the UI reconnects immediately after reset.
    from vali_utils.dashboard.score_metrics import emit_miner_score_update

    scorer = validator.evaluator.get_scorer()
    target_set = set(settings.target_miner_uids)
    score_rows = []
    for uid in reset_uids:
        if target_set and uid not in target_set:
            continue
        if not utils_is_miner(uid, validator):
            continue
        hotkey = validator.metagraph.hotkeys[uid]
        row = emit_miner_score_update(
            uid, hotkey, scorer, validator.metagraph, phase="reset"
        )
        score_rows.append(row)

    validation_stats_snapshot = get_validation_stats().to_api_response()

    get_event_bus().publish(
        "validation_stats_updated",
        validator.uid,
        validator.wallet.hotkey.ss58_address,
        validation_stats_snapshot,
    )
    get_event_bus().publish(
        "scores_reset",
        validator.uid,
        validator.wallet.hotkey.ss58_address,
        {
            "reset_count": len(reset_uids),
            "uids": reset_uids,
            "scores": score_rows,
            "validation_stats": validation_stats_snapshot,
            "message": f"Reset scores for {len(reset_uids)} miner slot(s).",
        },
    )
    return {
        "status": "ok",
        "reset_count": len(reset_uids),
        "uids": reset_uids,
        "scores": score_rows,
        "validation_stats": validation_stats_snapshot,
        "message": "Local scores reset. On-chain metagraph.I is unchanged.",
    }


@router.post("/api/evaluate/pause")
async def pause_evaluation():
    """Pause the automatic miner evaluation loop."""
    settings = get_settings_manager().update(evaluation_paused=True)
    get_event_bus().publish(
        "evaluation_state",
        0,
        "",
        {"evaluation_paused": True, "message": "Miner evaluation paused."},
    )
    return {"status": "paused", "evaluation_paused": True, "settings": asdict(settings)}


@router.post("/api/evaluate/resume")
async def resume_evaluation():
    """Resume evaluation and trigger an immediate eval cycle."""
    settings = get_settings_manager().update(
        evaluation_paused=False,
        trigger_eval_now=True,
    )
    get_event_bus().publish(
        "evaluation_state",
        0,
        "",
        {
            "evaluation_paused": False,
            "message": "Miner evaluation resumed.",
        },
    )
    return {
        "status": "running",
        "evaluation_paused": False,
        "message": "Miner evaluation resumed.",
        "settings": asdict(settings),
    }


@router.post("/api/evaluate/trigger")
async def trigger_evaluation():
    """Request immediate evaluation of target miners."""
    get_settings_manager().update(trigger_eval_now=True)
    get_event_bus().publish(
        "evaluation_state",
        0,
        "",
        {"message": "One-shot evaluation triggered."},
    )
    return {"status": "triggered"}


@router.post("/api/od-jobs/create")
async def create_od_job(req: CreateOdJobRequest, request: Request):
    """Manually create an on-demand job via the local API."""
    settings = get_settings_manager().get()
    if req.keyword_mode not in VALID_KEYWORD_MODES:
        raise HTTPException(400, "keyword_mode must be 'any' or 'all'")
    payload = build_od_job_payload(
        req.platform,
        req.keywords,
        subreddit=req.subreddit,
        usernames=req.usernames or None,
        limit=req.limit,
        ttl_minutes=req.ttl_minutes,
        keyword_mode=req.keyword_mode,
        start_date=req.start_date,
        end_date=req.end_date,
    )

    url = f"{settings.local_api_url.rstrip('/')}/on-demand/constellation/jobs"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        raise HTTPException(500, f"Failed to create OD job: {e}")


@router.post("/api/od-jobs/create-from-file")
async def create_od_jobs_from_file(req: CreateOdJobsFromFileRequest):
    """Create multiple on-demand jobs from definitions in an external JSON file."""
    settings = get_settings_manager().get()
    count = min(req.count, MAX_BATCH_OD_JOBS)

    try:
        entries = load_od_job_entries(
            req.file_path,
            platform_filter=req.platform,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(400, f"Failed to parse job file: {e}")

    if not entries:
        raise HTTPException(
            400,
            "No jobs found in file for the selected platform filter",
        )
    if req.start_index >= len(entries):
        raise HTTPException(
            400,
            f"start_index {req.start_index} out of range (file has {len(entries)} entries)",
        )

    selected = entries[req.start_index : req.start_index + count]
    url = f"{settings.local_api_url.rstrip('/')}/on-demand/constellation/jobs"
    created: List[dict] = []
    errors: List[dict] = []

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            for offset, entry in enumerate(selected):
                index = req.start_index + offset
                payload = build_od_job_payload_from_entry(entry)
                try:
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                    result = resp.json()
                    created.append(
                        {
                            "index": index,
                            "id": result.get("id"),
                            "platform": entry.platform,
                            "summary": entry.summary(),
                        }
                    )
                except Exception as e:
                    errors.append(
                        {
                            "index": index,
                            "platform": entry.platform,
                            "summary": entry.summary(),
                            "error": str(e),
                        }
                    )
    except Exception as e:
        raise HTTPException(500, f"Failed to create OD jobs: {e}")

    get_event_bus().publish(
        "od_jobs_batch_created",
        uid=-1,
        hotkey="dashboard",
        data={
            "created_count": len(created),
            "failed_count": len(errors),
            "total_in_file": len(entries),
            "file_path": req.file_path,
        },
    )

    return {
        "status": "ok",
        "total_in_file": len(entries),
        "start_index": req.start_index,
        "requested_count": count,
        "created_count": len(created),
        "failed_count": len(errors),
        "created": created,
        "errors": errors,
    }


@router.get("/api/od-jobs")
async def list_od_jobs(validator=Depends(_get_validator)):
    """List on-demand jobs from local API storage."""
    try:
        storage = _resolve_local_storage(validator)
    except HTTPException:
        settings = get_settings_manager().get()
        url = f"{settings.local_api_url.rstrip('/')}/on-demand/constellation/jobs"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            raise HTTPException(502, f"Local API unreachable: {e}") from e

    jobs = storage.list_od_jobs()
    jobs.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return {
        "jobs": jobs,
        "data_dir": str(storage.data_dir),
        "jobs_root": str(storage.data_dir / "on_demand" / "jobs"),
    }


@router.delete("/api/od-jobs/{job_id}")
async def delete_od_job(
    job_id: str,
    include_submissions: bool = True,
    validator=Depends(_get_validator),
):
    """Delete one OD job definition from local storage."""
    storage = _resolve_local_storage(validator)
    result = storage.delete_od_job(job_id, include_submissions=include_submissions)
    if not result["job_removed"] and not result["submission_files"]:
        raise HTTPException(404, f"OD job not found: {job_id}")
    return {
        "status": "ok",
        "message": f"Deleted OD job {job_id}.",
        **result,
    }


@router.post("/api/od-jobs/clear")
async def clear_od_jobs(
    req: ClearOdJobsRequest,
    validator=Depends(_get_validator),
):
    """Delete all OD job definitions from local storage."""
    storage = _resolve_local_storage(validator)
    result = storage.clear_od_jobs(include_submissions=req.include_submissions)
    return {
        "status": "ok",
        "message": (
            f"Removed {result['jobs_removed']} OD job file(s)"
            + (
                f" and {result['submission_files']} submission file(s)."
                if req.include_submissions
                else "."
            )
        ),
        **result,
    }


@router.get("/api/od-jobs/scheduler")
async def scheduler_status(request: Request):
    scheduler = _get_scheduler(request)
    if not scheduler:
        return {"enabled": False, "message": "Scheduler not running"}
    status = scheduler.status
    status["settings_interval"] = get_settings_manager().get().auto_od_interval_minutes
    return status


@router.get("/api/label-file/preview")
async def preview_label_file(
    file_path: Optional[str] = None,
    platform: str = "all",
):
    """Preview on-demand jobs loaded from an external JSON file."""
    settings = get_settings_manager().get()
    path = file_path or settings.auto_od_label_file_path
    if not path:
        raise HTTPException(400, "No job file path specified")

    platform_filter = platform or settings.label_file_platform_filter or "all"

    try:
        return preview_od_jobs(path, platform_filter=platform_filter)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(400, f"Failed to parse job file: {e}")


@router.post("/api/label-file/upload")
async def upload_label_file(
    request: Request,
    file: UploadFile = File(...),
    validator=Depends(_get_validator),
):
    """Upload a label file and store it under the validator data directory."""
    if not file.filename:
        raise HTTPException(400, "Filename required")

    safe_name = Path(file.filename).name
    if not safe_name or safe_name in (".", ".."):
        raise HTTPException(400, "Invalid filename")

    dest_dir = Path(validator.config.neuron.full_path) / "label_files"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / safe_name

    try:
        with open(dest_path, "wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as e:
        raise HTTPException(500, f"Upload failed: {e}")

    bt.logging.info(f"Uploaded label file to {dest_path}")
    return {
        "status": "ok",
        "file_path": str(dest_path.resolve()),
        "filename": safe_name,
    }


@router.get("/api/label-file/defaults")
async def list_default_label_files():
    """Return common label file paths available in the project."""
    root = Path(__file__).resolve().parents[2]
    examples_dir = root / "scripts" / "testnet" / "od_job_examples"
    candidates = [
        examples_dir / "example_od_jobs.json",
        examples_dir / "example_reddit_jobs.json",
        examples_dir / "example_x_jobs.json",
    ]
    files = []
    for path in candidates:
        if path.exists():
            files.append(
                {
                    "path": str(path.resolve()),
                    "name": path.name,
                    "format_hint": path.suffix,
                }
            )
    return {"files": files}


@router.get("/api/local-api/health")
async def local_api_health():
    settings = get_settings_manager().get()
    url = f"{settings.local_api_url.rstrip('/')}/health"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url)
            data = resp.json()
            if settings.local_api_data_dir:
                data.setdefault("storage", {})["data_dir"] = settings.local_api_data_dir
            return data
    except Exception as e:
        return {
            "status": "unreachable",
            "error": str(e),
            "storage": {"data_dir": settings.local_api_data_dir},
        }


@router.get("/api/od-submissions")
async def list_od_submissions(limit: int = 50, validator=Depends(_get_validator)):
    """List miner on-demand submission files on disk."""
    limit = max(1, min(limit, 200))
    try:
        storage = _resolve_local_storage(validator)
    except HTTPException:
        return {
            "data_dir": get_settings_manager().get().local_api_data_dir,
            "submissions": [],
            "submissions_root": "",
        }

    data_dir = str(storage.data_dir)
    return {
        "data_dir": data_dir,
        "submissions": storage.list_od_submissions(limit=limit),
        "submissions_root": str(Path(data_dir) / "on_demand" / "submissions"),
        "stats": storage.get_stats(),
    }


@router.post("/api/od-submissions/clear")
async def clear_miner_submissions(
    req: ClearMinerSubmissionsRequest,
    validator=Depends(_get_validator),
):
    """Delete all miner submission files from local API storage."""
    if not req.include_od_submissions and not req.include_parquet:
        raise HTTPException(400, "Select at least one category to clear")

    storage = _resolve_local_storage(validator)
    result = storage.clear_miner_submission_history(
        include_od_submissions=req.include_od_submissions,
        include_parquet=req.include_parquet,
    )
    return {
        "status": "ok",
        "message": (
            f"Removed {result['total_files_removed']} miner file(s) "
            f"({result['total_bytes_removed']} bytes)."
        ),
        **result,
    }


@router.get("/api/od-submissions/{job_id}/{miner_hotkey}")
async def get_od_submission(
    job_id: str,
    miner_hotkey: str,
    validator=Depends(_get_validator),
):
    """Return one miner submission JSON (for dashboard preview)."""
    storage = _resolve_local_storage(validator)
    raw = storage.read_od_submission_bytes(job_id, miner_hotkey)
    if raw is None:
        raise HTTPException(404, "Submission not found")

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise HTTPException(
            422,
            f"Submission file is not valid JSON: {e}",
        ) from e

    return {
        "job_id": job_id,
        "miner_hotkey": miner_hotkey,
        "file_path": str(
            storage.data_dir
            / "on_demand"
            / "submissions"
            / job_id
            / miner_hotkey
            / "data.json"
        ),
        "submission": payload,
    }
