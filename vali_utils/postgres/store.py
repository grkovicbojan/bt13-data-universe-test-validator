"""PostgreSQL tables for scrape cache, OD job templates, and validation reports."""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _default_scrape_cache_ttl_hours() -> int:
    try:
        return max(0, int(os.getenv("SCRAPE_CACHE_TTL_HOURS", "168")))
    except ValueError:
        return 168


class ValidatorPostgresStore:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._lock = threading.RLock()
        self._pool = pool.ThreadedConnectionPool(1, 6, database_url)
        self._scrape_cache_ttl_hours = _default_scrape_cache_ttl_hours()
        self._init_schema()
        logger.info("ValidatorPostgresStore connected")

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()

    @contextmanager
    def _connect(self):
        with self._lock:
            conn = self._pool.getconn()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                self._pool.putconn(conn)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scrape_cache (
                        url TEXT PRIMARY KEY,
                        platform TEXT NOT NULL,
                        content_json JSONB NOT NULL,
                        scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_scrape_cache_platform_scraped
                        ON scrape_cache (platform, scraped_at DESC);

                    CREATE TABLE IF NOT EXISTS od_job_templates (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL DEFAULT '',
                        platform TEXT NOT NULL DEFAULT '',
                        request_json JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_used_at TIMESTAMPTZ
                    );
                    CREATE INDEX IF NOT EXISTS idx_od_job_templates_created
                        ON od_job_templates (created_at DESC);

                    CREATE TABLE IF NOT EXISTS validation_comparisons (
                        id UUID PRIMARY KEY,
                        validation_type TEXT NOT NULL,
                        uid INTEGER NOT NULL,
                        hotkey TEXT NOT NULL,
                        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        passed BOOLEAN NOT NULL DEFAULT FALSE,
                        report_json JSONB NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_validation_comparisons_recorded
                        ON validation_comparisons (recorded_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_validation_comparisons_uid_recorded
                        ON validation_comparisons (uid, recorded_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_validation_comparisons_type_recorded
                        ON validation_comparisons (validation_type, recorded_at DESC);
                    """
                )

    # ------------------------------------------------------------------
    # Scrape cache
    # ------------------------------------------------------------------

    def get_scrape_cache(self, url: str, platform: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT content_json, scraped_at
                    FROM scrape_cache
                    WHERE url = %s AND platform = %s
                    """,
                    (url, platform),
                )
                row = cur.fetchone()
        if not row:
            return None
        scraped_at = row["scraped_at"]
        if scraped_at.tzinfo is None:
            scraped_at = scraped_at.replace(tzinfo=timezone.utc)
        if self._scrape_cache_ttl_hours > 0:
            age = _utcnow() - scraped_at.astimezone(timezone.utc)
            if age > timedelta(hours=self._scrape_cache_ttl_hours):
                return None
        content = row["content_json"]
        if isinstance(content, str):
            return json.loads(content)
        return dict(content)

    def put_scrape_cache(
        self, url: str, platform: str, content: Dict[str, Any]
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scrape_cache (url, platform, content_json, scraped_at)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (url) DO UPDATE SET
                        platform = EXCLUDED.platform,
                        content_json = EXCLUDED.content_json,
                        scraped_at = EXCLUDED.scraped_at
                    """,
                    (url, platform, json.dumps(content), _utcnow()),
                )

    # ------------------------------------------------------------------
    # OD job templates
    # ------------------------------------------------------------------

    def save_od_job_template(
        self,
        request: Dict[str, Any],
        *,
        name: Optional[str] = None,
    ) -> int:
        platform = str(request.get("platform") or "x").lower()
        if not name:
            keywords = request.get("keywords") or []
            if keywords:
                name = f"{platform}: {', '.join(str(k) for k in keywords[:3])}"
            elif request.get("subreddit"):
                name = f"reddit: r/{str(request.get('subreddit')).lstrip('r/')}"
            else:
                name = f"{platform} job"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO od_job_templates (name, platform, request_json)
                    VALUES (%s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (name, platform, json.dumps(request)),
                )
                row = cur.fetchone()
        return int(row[0])

    def list_od_job_templates(self, limit: int = 50) -> List[Dict[str, Any]]:
        limit = max(1, min(limit, 200))
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, platform, request_json, created_at, last_used_at
                    FROM od_job_templates
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            req = row["request_json"]
            if isinstance(req, str):
                req = json.loads(req)
            out.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "platform": row["platform"],
                    "request": req,
                    "created_at": row["created_at"].isoformat()
                    if row["created_at"]
                    else None,
                    "last_used_at": row["last_used_at"].isoformat()
                    if row["last_used_at"]
                    else None,
                }
            )
        return out

    def get_od_job_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, platform, request_json, created_at, last_used_at
                    FROM od_job_templates
                    WHERE id = %s
                    """,
                    (template_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        req = row["request_json"]
        if isinstance(req, str):
            req = json.loads(req)
        return {
            "id": row["id"],
            "name": row["name"],
            "platform": row["platform"],
            "request": req,
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "last_used_at": row["last_used_at"].isoformat()
            if row["last_used_at"]
            else None,
        }

    def touch_od_job_template(self, template_id: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE od_job_templates
                    SET last_used_at = %s
                    WHERE id = %s
                    """,
                    (_utcnow(), template_id),
                )

    def delete_od_job_template(self, template_id: int) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM od_job_templates WHERE id = %s RETURNING id",
                    (template_id,),
                )
                return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Validation comparisons
    # ------------------------------------------------------------------

    def insert_validation_comparison(self, report: Dict[str, Any]) -> str:
        report_id = str(report.get("id") or uuid.uuid4())
        recorded_at = report.get("timestamp")
        if recorded_at:
            try:
                recorded_dt = datetime.fromisoformat(
                    str(recorded_at).replace("Z", "+00:00")
                )
            except ValueError:
                recorded_dt = _utcnow()
        else:
            recorded_dt = _utcnow()
        if recorded_dt.tzinfo is None:
            recorded_dt = recorded_dt.replace(tzinfo=timezone.utc)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO validation_comparisons (
                        id, validation_type, uid, hotkey, recorded_at, passed, report_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        report_id,
                        str(report.get("validation_type") or "unknown"),
                        int(report.get("uid") or 0),
                        str(report.get("hotkey") or ""),
                        recorded_dt,
                        bool(report.get("passed", False)),
                        json.dumps(report),
                    ),
                )
        return report_id

    def list_validation_comparisons(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        uid: Optional[int] = None,
        validation_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        clauses = ["1=1"]
        params: List[Any] = []
        if uid is not None:
            clauses.append("uid = %s")
            params.append(uid)
        if validation_type:
            clauses.append("validation_type = %s")
            params.append(validation_type)
        where_sql = " AND ".join(clauses)

        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS total FROM validation_comparisons WHERE {where_sql}",
                    params,
                )
                total = int(cur.fetchone()["total"])
                cur.execute(
                    f"""
                    SELECT report_json
                    FROM validation_comparisons
                    WHERE {where_sql}
                    ORDER BY recorded_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    [*params, limit, offset],
                )
                rows = cur.fetchall()

        failures: List[Dict[str, Any]] = []
        for row in rows:
            payload = row["report_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            failures.append(dict(payload))
        return {"failures": failures, "total": total, "limit": limit, "offset": offset}

    def clear_validation_comparisons(self) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM validation_comparisons")
                return cur.rowcount
