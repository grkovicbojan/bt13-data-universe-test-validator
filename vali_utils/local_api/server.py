"""
Local Data Universe API server.

Replaces the remote Macrocosmos API + S3 presigned URLs with local filesystem
storage so validators and miners can run on testnet without external S3 access.

Point both validator and miner at this server:
  --s3_auth_url http://localhost:8100
"""

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote

import bittensor as bt
import uvicorn
from common.constants import MIN_OD_TTL_MINUTES
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from vali_utils.local_api.storage import LocalApiStorage, project_local_api_data_dir

# Default DD list served when dynamic_desirability/total.json is absent.
_DEFAULT_DD_PATH = (
    Path(__file__).resolve().parents[2] / "dynamic_desirability" / "default.json"
)

_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$", re.IGNORECASE)


async def _extract_upload_body(request: Request) -> bytes:
    """Return raw file bytes from PUT body or S3-style multipart POST."""
    content_type = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" in content_type:
        form = await request.form()
        upload = form.get("file")
        if upload is None:
            for value in form.values():
                if hasattr(value, "read"):
                    upload = value
                    break
        if upload is not None and hasattr(upload, "read"):
            data = await upload.read()
            if data:
                return data
        raise HTTPException(400, "multipart upload missing file part")
    return await request.body()


def _file_media_type(key: str) -> str:
    if key.endswith(".json"):
        return "application/json"
    return "application/octet-stream"


def _build_ranged_file_response(
    content: bytes, media_type: str, range_header: Optional[str]
) -> Response:
    """Return full or partial file content (HTTP 206) for DuckDB/httpfs Range reads."""
    file_size = len(content)
    base_headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(file_size),
    }

    if not range_header:
        return Response(content=content, media_type=media_type, headers=base_headers)

    match = _RANGE_RE.match(range_header.strip())
    if not match:
        return Response(content=content, media_type=media_type, headers=base_headers)

    start_str, end_str = match.groups()
    if start_str:
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
    elif end_str:
        suffix_len = int(end_str)
        start = max(file_size - suffix_len, 0)
        end = file_size - 1
    else:
        start = 0
        end = file_size - 1

    end = min(end, file_size - 1)
    if start > end or start >= file_size:
        return Response(
            status_code=416,
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk = content[start : end + 1]
    return Response(
        content=chunk,
        status_code=206,
        media_type=media_type,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(len(chunk)),
        },
    )


class CreateOdJobRequest(BaseModel):
    """Request body for POST /on-demand/constellation/jobs."""

    job: Dict[str, Any]
    limit: int = Field(default=100, ge=1, le=1000)
    keyword_mode: str = "any"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    ttl_minutes: int = Field(default=30, ge=MIN_OD_TTL_MINUTES, le=1440)


class LocalApiServer:
    """FastAPI server exposing Macrocosmos-compatible endpoints over local storage."""

    def __init__(self, data_dir: str, host: str = "0.0.0.0", port: int = 8100):
        self.host = host
        self.port = port
        self.base_url = f"http://localhost:{port}"
        self.storage = LocalApiStorage(data_dir)
        self.app = self._create_app()
        self._thread: Optional[Thread] = None

    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title="Local Data Universe API",
            description="Testnet replacement for Macrocosmos S3/Data Universe API",
            version="1.0.0",
        )
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @app.get("/health")
        async def health():
            return {"status": "healthy", "storage": self.storage.get_stats()}

        # --------------------------------------------------------------
        # S3-compatible file listing and download
        # --------------------------------------------------------------

        @app.post("/get-miner-list")
        async def get_miner_list(request: Request):
            """Return a local list URL instead of an S3 presigned list URL."""
            body = await request.body()
            payload = json.loads(body) if body else {}
            miner_hotkey = payload.get("miner_hotkey", "")
            token = payload.get("continuation_token", "")
            if not miner_hotkey:
                raise HTTPException(400, "miner_hotkey required")

            list_url = (
                f"{self.base_url}/local-s3/list"
                f"?miner_hotkey={quote(miner_hotkey)}"
            )
            if token:
                list_url += f"&continuation_token={quote(str(token))}"
            return {"list_url": list_url}

        @app.get("/local-s3/list")
        async def local_s3_list(
            miner_hotkey: str,
            continuation_token: Optional[str] = None,
        ):
            """Serve S3 ListBucketResult XML for ValidatorS3Access."""
            xml = self.storage.build_s3_list_xml(miner_hotkey, continuation_token)
            return Response(content=xml, media_type="application/xml")

        @app.post("/get-file-presigned-urls")
        async def get_file_presigned_urls(request: Request):
            """Return local download URLs instead of S3 presigned URLs."""
            payload = json.loads(await request.body())
            miner_hotkey = payload.get("miner_hotkey", "")
            file_keys: List[str] = payload.get("file_keys", [])
            file_urls = {}
            for key in file_keys:
                file_urls[key] = {
                    "presigned_url": (
                        f"{self.base_url}/local-download"
                        f"?key={quote(key, safe='')}"
                    )
                }
            return {"file_urls": file_urls, "miner_hotkey": miner_hotkey}

        @app.head("/local-download")
        async def local_download_head(key: str):
            """Expose file size for clients that probe before Range reads."""
            content = self.storage.read_file(key)
            if content is None:
                raise HTTPException(404, f"File not found: {key}")
            return Response(
                status_code=200,
                media_type=_file_media_type(key),
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(len(content)),
                },
            )

        @app.get("/local-download")
        async def local_download(key: str, request: Request):
            """Serve a stored parquet or JSON file by S3-style key."""
            content = self.storage.read_file(key)
            if content is None:
                raise HTTPException(404, f"File not found: {key}")
            return _build_ranged_file_response(
                content,
                _file_media_type(key),
                request.headers.get("range"),
            )

        @app.post("/get-file-upload-url")
        async def get_file_upload_url(request: Request):
            """Return a local PUT upload URL for miner parquet uploads."""
            payload = json.loads(await request.body())
            job_id = payload.get("job_id", "")
            filename = payload.get("filename", "")
            if not job_id or not filename:
                raise HTTPException(400, "job_id and filename required")

            # Hotkey is embedded in Tao auth header; for local use we accept body override.
            miner_hotkey = payload.get("miner_hotkey", "unknown")
            signed_by = request.headers.get("X-Tao-Signed-By")
            if signed_by:
                miner_hotkey = signed_by

            s3_key = (
                f"data/hotkey={miner_hotkey}/job_id={job_id}/{filename}"
            )
            upload_url = (
                f"{self.base_url}/local-upload"
                f"?key={quote(s3_key, safe='')}"
            )
            return {
                "s3_key": s3_key,
                "url": upload_url,
                "fields": {},
                "expires_in_seconds": 3600,
                "method": "PUT",
            }

        @app.put("/local-upload")
        async def local_upload(key: str, request: Request):
            """Receive a raw PUT upload (JSON/parquet bytes)."""
            content = await _extract_upload_body(request)
            self.storage.save_upload(unquote(key), content)
            return Response(status_code=200)

        @app.post("/local-upload")
        async def local_upload_post(key: str, request: Request):
            """POST variant for S3/DO Spaces-style multipart uploads (mainnet path)."""
            content = await _extract_upload_body(request)
            self.storage.save_upload(unquote(key), content)
            return Response(status_code=204)

        # --------------------------------------------------------------
        # On-demand job endpoints
        # --------------------------------------------------------------

        @app.post("/on-demand/constellation/jobs")
        async def create_od_job(req: CreateOdJobRequest):
            """Create an on-demand job (used by dashboard scheduler / manual input)."""
            job_id = str(uuid.uuid4())
            inner = req.job.get("job", req.job)
            record = self.storage.create_od_job(
                {
                    "id": job_id,
                    "job": inner,
                    "limit": req.limit,
                    "keyword_mode": req.keyword_mode,
                    "start_date": req.start_date,
                    "end_date": req.end_date,
                    "ttl_minutes": req.ttl_minutes,
                }
            )
            return {"id": job_id, "job": record}

        @app.get("/on-demand/constellation/jobs")
        async def list_od_jobs():
            return {"jobs": self.storage.list_od_jobs()}

        @app.get("/on-demand/constellation/jobs/{job_id}")
        async def get_od_job(job_id: str):
            jobs = self.storage.list_od_jobs()
            for j in jobs:
                if j["id"] == job_id:
                    submissions = []
                    sub_root = (
                        self.storage.data_dir
                        / "on_demand"
                        / "submissions"
                        / job_id
                    )
                    if sub_root.exists():
                        for hotkey_dir in sub_root.iterdir():
                            if hotkey_dir.is_dir():
                                sub = self.storage.get_od_submission(
                                    job_id, hotkey_dir.name
                                )
                                if sub:
                                    sub["s3_presigned_url"] = (
                                        f"{self.base_url}/local-download"
                                        f"?key={quote(sub['s3_path'], safe='')}"
                                    )
                                    submissions.append(sub)
                    return {"job": j, "submissions": submissions}
            raise HTTPException(404, "Job not found")

        @app.post("/on-demand/miner/jobs/active")
        async def miner_list_active_jobs(request: Request):
            """Return active OD jobs for miners to pick up."""
            raw = await request.body()
            body = json.loads(raw) if raw else {}
            since_str = body.get("since")
            if since_str:
                since = datetime.fromisoformat(
                    since_str.replace("Z", "+00:00")
                )
            else:
                since = datetime.now(timezone.utc) - timedelta(minutes=30)

            created_since = None
            if body.get("created_since"):
                created_since = datetime.fromisoformat(
                    body["created_since"].replace("Z", "+00:00")
                )

            jobs = self.storage.get_active_od_jobs(
                since, created_since=created_since
            )
            return {"jobs": [_format_od_job(j) for j in jobs]}

        @app.post("/on-demand/miner/jobs/submit")
        async def miner_submit_job(request: Request):
            """Return a local upload URL for OD submission JSON."""
            payload = json.loads(await request.body())
            submission = payload.get("submission", payload)
            job_id = submission.get("job_id", "")
            if not job_id:
                raise HTTPException(400, "job_id required")

            miner_hotkey = request.headers.get("X-Tao-Signed-By", "unknown")
            s3_path = f"on_demand/submissions/{job_id}/{miner_hotkey}/data.json"
            upload_url = (
                f"{self.base_url}/local-upload"
                f"?key={quote(s3_path, safe='')}"
            )
            return {
                "presigned_post_upload_data": {
                    "url": upload_url,
                    "fields": {"Content-Type": "application/json"},
                    "method": "POST",
                }
            }

        @app.post("/on-demand/validator/jobs")
        async def validator_list_jobs(request: Request):
            """List expired jobs with all miner submissions for validation."""
            payload = json.loads(await request.body())
            expired_since = datetime.fromisoformat(
                payload["expired_since"].replace("Z", "+00:00")
            )
            expired_until = datetime.fromisoformat(
                payload["expired_until"].replace("Z", "+00:00")
            )
            limit = payload.get("limit", 10)
            created_since = None
            if payload.get("created_since"):
                created_since = datetime.fromisoformat(
                    payload["created_since"].replace("Z", "+00:00")
                )

            jobs = self.storage.get_expired_jobs(
                expired_since, expired_until, created_since=created_since
            )[:limit]
            result = []
            for job in jobs:
                submissions = _collect_submissions(self, job["id"])
                result.append(
                    {"job": _format_od_job(job), "submissions": submissions}
                )
            return {"jobs_with_submissions": result}

        @app.post("/on-demand/validator/miner-jobs")
        async def validator_list_miner_jobs(request: Request):
            """List OD jobs submitted by a specific miner."""
            payload = json.loads(await request.body())
            miner_hotkey = payload.get("miner_hotkey", "")
            expired_since = datetime.fromisoformat(
                payload["expired_since"].replace("Z", "+00:00")
            )
            expired_until = datetime.fromisoformat(
                payload["expired_until"].replace("Z", "+00:00")
            )
            limit = payload.get("limit", 500)
            created_since = None
            if payload.get("created_since"):
                created_since = datetime.fromisoformat(
                    payload["created_since"].replace("Z", "+00:00")
                )

            jobs = self.storage.get_expired_jobs(
                expired_since, expired_until, created_since=created_since
            )
            result = []
            for job in jobs:
                sub = self.storage.get_od_submission(job["id"], miner_hotkey)
                if sub:
                    sub["s3_presigned_url"] = (
                        f"{self.base_url}/local-download"
                        f"?key={quote(sub['s3_path'], safe='')}"
                    )
                    result.append(
                        {
                            "job": _format_od_job(job),
                            "submission": sub,
                        }
                    )
                if len(result) >= limit:
                    break
            return {"jobs": result}

        # --------------------------------------------------------------
        # Dynamic desirability
        # --------------------------------------------------------------

        @app.get("/dynamic-desirability/validator/get-latest-list")
        async def get_dd_list_validator():
            return _load_dd_list()

        @app.get("/dynamic-desirability/miner/get-latest-list")
        async def get_dd_list_miner():
            return _load_dd_list()

        return app

    def start(self):
        """Start the local API server in a background thread."""
        if self._thread and self._thread.is_alive():
            bt.logging.warning("Local API server already running")
            return

        def _run():
            uvicorn.run(self.app, host=self.host, port=self.port, log_level="warning")

        self._thread = Thread(target=_run, daemon=True, name="local-api-server")
        self._thread.start()
        bt.logging.success(
            f"Local Data Universe API started at {self.base_url}"
        )

    def stop(self):
        if self._thread:
            self._thread.join(timeout=3)


def _format_od_job(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert internal job record to OnDemandJob-compatible dict."""
    return {
        "id": record["id"],
        "created_at": record.get("created_at"),
        "expire_at": record.get("expire_at"),
        "job": record.get("job", {}),
        "start_date": record.get("start_date"),
        "end_date": record.get("end_date"),
        "limit": record.get("limit", 100),
        "keyword_mode": record.get("keyword_mode", "any"),
    }


def _collect_submissions(server: LocalApiServer, job_id: str) -> List[Dict]:
    """Gather all miner submissions for a job with local download URLs."""
    submissions = []
    sub_root = server.storage.data_dir / "on_demand" / "submissions" / job_id
    if not sub_root.exists():
        return submissions
    for hotkey_dir in sub_root.iterdir():
        if hotkey_dir.is_dir():
            sub = server.storage.get_od_submission(job_id, hotkey_dir.name)
            if sub:
                sub["s3_presigned_url"] = (
                    f"{server.base_url}/local-download"
                    f"?key={quote(sub['s3_path'], safe='')}"
                )
                submissions.append(sub)
    return submissions


def _load_dd_list() -> Dict[str, Any]:
    """Load dynamic desirability list from total.json or default.json."""
    root = Path(__file__).resolve().parents[2]
    for name in ("total.json", "default.json"):
        path = root / "dynamic_desirability" / name
        if path.exists():
            with open(path) as f:
                raw = json.load(f)
            entries = []
            for item in raw:
                params = item.get("params", item)
                entries.append(
                    {
                        "id": item.get("id", ""),
                        "platform": params.get("platform", ""),
                        "weight": item.get("weight", 1.0),
                        "label": params.get("label"),
                        "keyword": params.get("keyword"),
                        "post_start_datetime": params.get("post_start_datetime"),
                        "post_end_datetime": params.get("post_end_datetime"),
                    }
                )
            return {
                "version": "local-testnet",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "entries": entries,
            }
    return {"version": "local-testnet", "generated_at": datetime.now(timezone.utc).isoformat(), "entries": []}


def start_local_api(
    data_dir: Optional[str] = None,
    port: int = 8100,
) -> LocalApiServer:
    """Convenience launcher used by the testnet dashboard script."""
    if data_dir is None:
        data_dir = str(project_local_api_data_dir())
    server = LocalApiServer(data_dir=data_dir, port=port)
    server.start()
    return server
