import os
import gc
import copy
import asyncio
import datetime
import random
import traceback
import threading
import time

from common import constants
from common.data_v2 import ScorableMinerIndex
from common.metagraph_syncer import MetagraphSyncer
import common.utils as utils
import datetime as dt
import bittensor as bt
from common.data import (
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntity,
    DataSource,
)
from common.protocol import GetDataEntityBucket, GetMinerIndex
from rewards.data_value_calculator import DataValueCalculator
from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)
from storage.validator.s3_validator_storage import S3ValidationStorage

from vali_utils.miner_iterator import MinerIterator
from vali_utils import metrics, utils as vali_utils

from typing import Dict, List, Optional, Tuple
from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.s3_utils import validate_s3_miner_data, get_s3_validation_summary, S3ValidationResult
from vali_utils.s3_logging_utils import log_s3_validation_table

import httpx

from common.api_client import (
    DataUniverseApiClient,
    ListMinerJobsForValidationRequest,
    OnDemandJob,
    OnDemandJobSubmission,
    OnDemandMinerUpload,
)
from rewards.miner_scorer import MinerScorer
from vali_utils.on_demand.on_demand_validation import OnDemandValidator
from vali_utils.dashboard.events import get_event_bus
from vali_utils.dashboard.score_metrics import emit_miner_score_update
from vali_utils.dashboard.settings import get_settings_manager
from vali_utils.dashboard.validation_reports import (
    OdValidationResult,
    preview_entity,
    preview_entities,
    record_od_failure,
    record_p2p_failure,
    record_s3_failure,
    serialize_od_job,
)
from vali_utils.dashboard.validation_stats import (
    record_od_stats,
    record_p2p_stats,
    record_s3_stats,
)


class MinerEvaluator:
    """MinerEvaluator is responsible for evaluating miners and updating their scores."""

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_APIDOJO,
        DataSource.REDDIT: ScraperId.REDDIT_MC,
    }

    def __init__(self, config: bt.config, uid: int, metagraph_syncer: MetagraphSyncer, s3_reader: ValidatorS3Access):
        self.config = config
        self.uid = uid
        self.metagraph_syncer = metagraph_syncer
        self.metagraph = self.metagraph_syncer.get_metagraph(config.netuid)
        self.metagraph_syncer.register_listener(
            self._on_metagraph_updated, netuids=[config.netuid]
        )
        self.vpermit_rao_limit = self.config.vpermit_rao_limit
        self.wallet = bt.wallet(config=self.config)

        # Set up initial scoring weights for validation
        self.scorer = MinerScorer(self.metagraph.n, DataValueCalculator())

        # Setup dependencies.
        self.miner_iterator = MinerIterator(
            utils.get_miner_uids(self.metagraph, self.vpermit_rao_limit)
        )
        self.scraper_provider = ScraperProvider()
        self.storage = SqliteMemoryValidatorStorage()
        self.s3_storage = S3ValidationStorage(self.config.s3_results_path)
        self.s3_reader = s3_reader
        # OD validator — set by validator.py after construction.
        # Used for inline OD evaluation during eval_miner().
        self.on_demand_validator: Optional[OnDemandValidator] = None
        # Track last OD eval time per miner to query only new jobs each cycle.
        # Not persisted — on restart defaults to session start (see below).
        self._last_od_eval_at: Dict[str, dt.datetime] = {}
        # Only validate OD jobs created at or after this validator session start.
        self._od_session_started_at = dt.datetime.now(dt.timezone.utc)
        bt.logging.info(
            "OD validation session started at %s — only jobs created after this "
            "time will be validated",
            self._od_session_started_at.isoformat(),
        )
        # Cache API client construction params (derived once, reused every eval).
        self._api_base_url = self.config.s3_auth_url
        self._api_verify_ssl = "localhost" not in self._api_base_url

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.lock = threading.RLock()
        self.is_setup = False

    def _on_demand_client(self) -> DataUniverseApiClient:
        return DataUniverseApiClient(
            base_url=self._api_base_url,
            verify_ssl=self._api_verify_ssl,
            keypair=self.wallet.hotkey,
            timeout=60,
        )

    def get_scorer(self) -> MinerScorer:
        """Returns the scorer used by the evaluator."""
        return self.scorer

    def reset_scores(
        self,
        uids: Optional[List[int]] = None,
        clear_history: bool = True,
        clear_validation_reports: bool = True,
    ) -> List[int]:
        """Reset miner scores to initial values and persist scorer state.

        Args:
            uids: Miner UIDs to reset. None resets every neuron slot.
            clear_history: Clear dashboard chart history for affected UIDs.
            clear_validation_reports: Clear validation failure report buffer.

        Returns:
            List of UIDs that were reset.
        """
        from vali_utils.dashboard.score_metrics import get_score_history
        from vali_utils.dashboard.validation_reports import get_validation_reports
        from vali_utils.dashboard.validation_stats import get_validation_stats

        if uids is None:
            self.scorer.reset_all()
            reset_uids = list(range(len(self.metagraph.hotkeys)))
        else:
            reset_uids = sorted({int(uid) for uid in uids})
            for uid in reset_uids:
                if 0 <= uid < len(self.metagraph.hotkeys):
                    self.scorer.reset(uid)

        if clear_history:
            get_score_history().clear(None if uids is None else reset_uids)
        if clear_validation_reports:
            get_validation_reports().clear()
        stats_store = get_validation_stats()
        if uids is None:
            stats_store.clear()
        else:
            stats_store.remove_miners(reset_uids)

        self.save_state()
        bt.logging.info(
            f"Reset scores for {len(reset_uids)} miner(s): "
            f"{reset_uids[:10]}{'…' if len(reset_uids) > 10 else ''}"
        )
        return reset_uids

    def eval_miner_sync(self, uid: int) -> None:
        """Synchronous version of eval_miner."""
        asyncio.run(self.eval_miner(uid))

    # Maximum OD jobs to validate per miner per eval cycle.
    # Each validation downloads ~1MB + 1 scraper API call, so keep this bounded.
    OD_MAX_JOBS_TO_VALIDATE = 3
    # Number of entities to schema-check per downloaded submission.
    OD_SCHEMA_SAMPLE_SIZE = 5

    def _od_job_in_session(self, job: OnDemandJob) -> bool:
        """True if the job was created during this validator session."""
        created = job.created_at
        if created is None:
            return False
        if created.tzinfo is None:
            created = created.replace(tzinfo=dt.timezone.utc)
        return created >= self._od_session_started_at

    async def _evaluate_od(self, uid: int, hotkey: str) -> None:
        """Evaluate a miner's on-demand submissions by querying the API directly.

        Calls the per-miner jobs endpoint to get this miner's recent OD
        submissions with fresh presigned URLs, then validates a random
        sample and applies per-job rewards/penalties.
        """
        if self.on_demand_validator is None:
            return

        # Determine time window: since last eval, or session start on first run.
        now = dt.datetime.now(dt.timezone.utc)
        expired_since = self._last_od_eval_at.get(
            hotkey, self._od_session_started_at
        )

        try:
            async with self._on_demand_client() as client:
                resp = await client.validator_list_miner_jobs(
                    ListMinerJobsForValidationRequest(
                        miner_hotkey=hotkey,
                        expired_since=expired_since,
                        expired_until=now,
                        created_since=self._od_session_started_at,
                        limit=500,
                    )
                )
        except Exception as e:
            bt.logging.warning(f"UID:{uid} - HOTKEY:{hotkey}: OD API fetch failed: {e}")
            return

        self._last_od_eval_at[hotkey] = now

        jobs = [j for j in resp.jobs if self._od_job_in_session(j.job)]
        skipped_old = len(resp.jobs) - len(jobs)
        if skipped_old:
            bt.logging.debug(
                f"UID:{uid} - HOTKEY:{hotkey}: skipped {skipped_old} pre-session OD job(s)"
            )
        if not jobs:
            return

        # Single-pass partition into empty vs non-empty submissions
        empty, non_empty = [], []
        od_failure_summaries: List[Dict] = []
        for j in jobs:
            if (j.submission.s3_content_length or 0) > 0:
                non_empty.append(j)
            else:
                empty.append(j)

        for j in empty:
            self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
            record_od_failure(
                uid,
                hotkey,
                j.job,
                j.submission,
                {
                    "job_id": j.submission.job_id,
                    "failure_phase": "empty_submission",
                    "reason": "Miner submitted empty OD payload (0 bytes).",
                    "entity_count": 0,
                    "entity_previews": [],
                    "hints": ["Miner must upload JSON with data_entities before job expires."],
                },
            )
            od_failure_summaries.append(
                {
                    "job_id": j.submission.job_id,
                    "phase": "empty_submission",
                    "reason": "Empty submission",
                }
            )

        if not non_empty:
            if empty:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: OD — {len(empty)} empty submissions penalized, "
                    f"0 non-empty"
                )
                od_payload = {
                    "validated_pass": 0,
                    "validated_fail": len(empty),
                    "empty_penalized": len(empty),
                    "credibility_bumped": 0,
                    "failures": od_failure_summaries,
                    "jobs_total": len(empty),
                    "jobs_checked": len(empty),
                    "jobs_passed": 0,
                    "jobs_failed": len(empty),
                    "entities_total": 0,
                    "entities_checked": 0,
                    "entities_passed": 0,
                }
                get_event_bus().publish("eval_od_complete", uid, hotkey, od_payload)
                record_od_stats(
                    uid,
                    hotkey,
                    jobs_total=len(empty),
                    jobs_checked=len(empty),
                    jobs_passed=0,
                    jobs_failed=len(empty),
                    entities_total=0,
                    entities_checked=0,
                    entities_passed=0,
                    status="failed",
                )
                emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="od")
            return

        od_mode = getattr(
            get_settings_manager().get(), "od_validation_mode", "sample"
        )
        od_full = od_mode == "full"

        if od_full:
            to_validate = list(non_empty)
            not_sampled_count = 0
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: OD full validation — "
                f"validating all {len(non_empty)} non-empty jobs"
            )
        else:
            # Sample up to OD_MAX_JOBS_TO_VALIDATE for deep validation
            to_validate = random.sample(
                non_empty, min(self.OD_MAX_JOBS_TO_VALIDATE, len(non_empty))
            )
            not_sampled_count = len(non_empty) - len(to_validate)

        # Validate sampled jobs concurrently
        validation_results: List[OdValidationResult] = await asyncio.gather(*[
            self._validate_od_submission(
                uid, hotkey, j.job, j.submission, j.submission.job_id
            )
            for j in to_validate
        ])

        # Apply per-job rewards/penalties based on validation results
        validated_pass = 0
        validated_fail = 0
        validated_skipped = 0

        for j, outcome in zip(to_validate, validation_results):
            passed = outcome.passed
            entity_count = outcome.entity_count
            if passed is None:
                # Validator-side download failure (5xx/timeout) — neither reward nor
                # penalize; the miner is not at fault for our infrastructure. (companion to #805)
                validated_skipped += 1
                bt.logging.warning(
                    f"UID:{uid} - OD: job {j.submission.job_id} skipped — validator-side "
                    f"download failure, no reward/penalty"
                )
                continue

            speed_mult, vol_mult = (
                self.on_demand_validator.calculate_ondemand_reward_multipliers(
                    job_created_at=j.job.created_at,
                    submission_timestamp=j.submission.submitted_at,
                    returned_count=entity_count,
                    requested_limit=j.job.limit,
                )
            )

            if passed:
                self.scorer.apply_ondemand_reward(uid, speed_mult, vol_mult)
                validated_pass += 1
            else:
                self.scorer.apply_ondemand_penalty(uid, mult_factor=1.0)
                validated_fail += 1
                record_od_failure(uid, hotkey, j.job, j.submission, outcome.report)
                od_failure_summaries.append(
                    {
                        "job_id": j.submission.job_id,
                        "phase": outcome.report.get("failure_phase", ""),
                        "reason": outcome.report.get("reason", ""),
                    }
                )

        # Batch credibility bump for non-sampled but participating submissions
        if not_sampled_count > 0:
            self.scorer.apply_ondemand_credibility_bump(uid, count=not_sampled_count)

        od_jobs_total = len(jobs)
        od_jobs_checked = len(empty) + len(to_validate)
        od_jobs_failed = len(empty) + validated_fail
        od_entities_checked = sum(
            o.entity_count for o in validation_results if o.passed is not None
        )
        od_entities_passed = sum(
            o.entity_count for o in validation_results if o.passed is True
        )
        od_entities_failed = sum(
            o.entity_count for o in validation_results if o.passed is False
        )
        od_status = (
            "passed"
            if validated_fail == 0 and len(empty) == 0 and validated_skipped == 0
            else "failed"
            if validated_pass == 0 and not_sampled_count == 0
            else "partial"
        )

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: OD summary — "
            f"{len(non_empty)} non-empty (validated: {validated_pass} pass, {validated_fail} fail, "
            f"{not_sampled_count} credibility-bumped), {len(empty)} empty penalized"
        )
        od_payload = {
            "validated_pass": validated_pass,
            "validated_fail": validated_fail,
            "validated_skipped": validated_skipped,
            "empty_penalized": len(empty),
            "credibility_bumped": not_sampled_count,
            "failures": od_failure_summaries,
            "jobs_total": od_jobs_total,
            "jobs_checked": od_jobs_checked,
            "jobs_passed": validated_pass,
            "jobs_failed": od_jobs_failed,
            "jobs_skipped": validated_skipped,
            "entities_total": od_entities_checked,
            "entities_checked": od_entities_checked,
            "entities_passed": od_entities_passed,
            "entities_failed": od_entities_failed,
        }
        get_event_bus().publish("eval_od_complete", uid, hotkey, od_payload)
        record_od_stats(
            uid,
            hotkey,
            jobs_total=od_jobs_total,
            jobs_checked=od_jobs_checked,
            jobs_passed=validated_pass,
            jobs_failed=od_jobs_failed,
            jobs_skipped=validated_skipped,
            jobs_credibility_bumped=not_sampled_count,
            entities_total=od_entities_checked,
            entities_checked=od_entities_checked,
            entities_passed=od_entities_passed,
            entities_failed=od_entities_failed,
            status=od_status,
            detail={"non_empty_submissions": len(non_empty)},
        )
        emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="od")

    def _od_expected_summary(self, job: OnDemandJob) -> Dict:
        job_data = serialize_od_job(job)
        inner = job_data.get("job", {}) if isinstance(job_data.get("job"), dict) else {}
        return {
            "platform": inner.get("platform"),
            "keywords": inner.get("keywords"),
            "subreddit": inner.get("subreddit"),
            "usernames": inner.get("usernames"),
            "start_date": job_data.get("start_date"),
            "end_date": job_data.get("end_date"),
            "limit": job_data.get("limit"),
            "keyword_mode": job_data.get("keyword_mode"),
        }

    async def _validate_od_submission(
        self,
        uid: int,
        hotkey: str,
        job: OnDemandJob,
        submission: OnDemandJobSubmission,
        job_id: str,
    ) -> OdValidationResult:
        """Download and validate a single OD submission.

        Returns OdValidationResult — entity_count is used for volume_multiplier.
        passed=None signals a validator-side download failure (5xx/timeout):
        the caller leaves credibility unchanged (neither reward nor penalty). (#805)
        """
        expected = self._od_expected_summary(job)

        def _fail(
            phase: str,
            reason: str,
            entity_count: int = 0,
            entities: Optional[List[DataEntity]] = None,
            failed_entity: Optional[DataEntity] = None,
            hints: Optional[List[str]] = None,
            check_failures: Optional[List[Dict]] = None,
            validation_trail: Optional[List[Dict]] = None,
            entity_validation_results: Optional[List[Dict]] = None,
        ) -> OdValidationResult:
            previews = preview_entities(entities or [])
            if failed_entity and not any(p.get("uri") == failed_entity.uri for p in previews):
                previews = [preview_entity(failed_entity)] + previews
            trail = list(validation_trail or [])
            if not trail:
                trail = [{"phase": phase, "message": reason}]
            normalized_checks = check_failures or [
                {
                    "phase": item.get("phase", phase),
                    "detail": item.get("message", reason),
                    "validator_message": item.get("message", reason),
                }
                for item in trail
            ]
            return OdValidationResult(
                passed=False,
                entity_count=entity_count,
                report={
                    "job_id": job_id,
                    "failure_phase": phase,
                    "reason": reason,
                    "entity_count": entity_count,
                    "entity_previews": previews[:5],
                    "failed_entity": preview_entity(failed_entity) if failed_entity else None,
                    "expected": expected,
                    "check_failures": normalized_checks,
                    "validation_trail": trail,
                    "entity_validation_results": entity_validation_results or [],
                    "hints": hints or [],
                },
            )

        try:
            async with httpx.AsyncClient(timeout=30.0) as http:
                dl_resp = await http.get(submission.s3_presigned_url, follow_redirects=True)
                if dl_resp.status_code != 200:
                    bt.logging.warning(
                        f"UID:{uid} - OD validate: download failed ({dl_resp.status_code}) "
                        f"for job {job_id}"
                    )
                    if dl_resp.status_code >= 500:
                        return OdValidationResult(passed=None, entity_count=0)
                    return _fail(
                        "download",
                        f"Submission download failed with HTTP {dl_resp.status_code}.",
                        hints=["Check presigned URL upload completed before expiry."],
                    )

                try:
                    miner_upload = OnDemandMinerUpload.model_validate(dl_resp.json())
                except Exception as parse_err:
                    return _fail(
                        "parse",
                        f"Submission JSON could not be parsed: {parse_err}",
                        hints=["Upload must be valid JSON with data_entities array."],
                    )

            entities = miner_upload.data_entities

            if not entities:
                ctx = OnDemandValidator.build_validation_context(job)
                data_exists = await self.on_demand_validator.check_data_exists(ctx)
                if data_exists:
                    bt.logging.info(
                        f"UID:{uid} - OD validate: empty submission but data exists "
                        f"for job {job_id}"
                    )
                    return _fail(
                        "empty",
                        "Submission contained zero entities but matching data exists on platform.",
                        hints=[
                            "Miner should return posts matching job keywords/usernames/date range.",
                        ],
                    )
                bt.logging.info(
                    f"UID:{uid} - OD validate: empty submission, data doesn't exist "
                    f"for job {job_id} — acceptable"
                )
                return OdValidationResult(passed=True, entity_count=0)

            entity_count = len(entities)

            if job.limit and entity_count > job.limit:
                entities = entities[:job.limit]

            od_full = (
                getattr(get_settings_manager().get(), "od_validation_mode", "sample")
                == "full"
            )
            if od_full:
                schema_sample = list(entities)
            else:
                sample_size = min(self.OD_SCHEMA_SAMPLE_SIZE, len(entities))
                schema_sample = random.sample(entities, sample_size)

            ctx = OnDemandValidator.build_validation_context(job)
            if not self.on_demand_validator._validate_miner_data_format(
                ctx, schema_sample, uid
            ):
                bt.logging.warning(
                    f"UID:{uid} - OD validate: SCHEMA FAILED for job {job_id} "
                    f"(wrong XContent format)"
                )
                return _fail(
                    "schema",
                    "Entity schema/format validation failed (invalid XContent/RedditContent or duplicates).",
                    entity_count=entity_count,
                    entities=schema_sample,
                    hints=[
                        "Each entity needs uri + content; XContent/RedditContent must deserialize.",
                        "Duplicate post IDs in the same submission are rejected.",
                    ],
                )

            job_match_results: List[Dict] = []
            job_match_failures: List[Dict] = []
            first_job_match_failed: Optional[DataEntity] = None
            for entity in schema_sample:
                post_id = self.on_demand_validator._get_post_id(entity)
                job_match_ok = self.on_demand_validator._validate_request_fields(
                    ctx, entity, uid
                )
                job_match_msg = (
                    "OK"
                    if job_match_ok
                    else f"Entity {post_id} does not match job request fields."
                )
                job_match_results.append(
                    {
                        "uri": entity.uri,
                        "post_id": post_id,
                        "passed": job_match_ok,
                        "phase": "job_match",
                        "validator_message": job_match_msg,
                    }
                )
                if job_match_ok:
                    bt.logging.info(
                        f"UID:{uid} - OD validate: JOB MATCH PASSED for job {job_id}, "
                        f"post {post_id}"
                    )
                else:
                    bt.logging.warning(
                        f"UID:{uid} - OD validate: JOB MATCH FAILED for job {job_id}, "
                        f"post {post_id} (wrong username/keyword/date)"
                    )
                    job_match_failures.append(
                        {
                            "phase": "job_match",
                            "uri": entity.uri,
                            "detail": job_match_msg,
                            "validator_message": job_match_msg,
                            "content_comparison": {
                                "uri": entity.uri,
                                "miner_submission": preview_entity(entity),
                                "validator_message": job_match_msg,
                                "job_requirements": expected,
                            },
                        }
                    )
                    if first_job_match_failed is None:
                        first_job_match_failed = entity

            if job_match_failures:
                failed_count = len(job_match_failures)
                sampled_count = len(job_match_results)
                return _fail(
                    "job_match",
                    (
                        f"{failed_count}/{sampled_count} sampled entities failed "
                        "job field match."
                    ),
                    entity_count=entity_count,
                    entities=schema_sample,
                    failed_entity=first_job_match_failed,
                    hints=[
                        "Check username, keyword/subreddit, URL, and date range against the job.",
                    ],
                    check_failures=job_match_failures,
                    entity_validation_results=job_match_results,
                )

            scraper_targets = (
                schema_sample
                if od_full
                else [random.choice(schema_sample)]
            )
            entity_validation_results: List[Dict] = []
            scraper_check_failures: List[Dict] = []
            first_failed_entity: Optional[DataEntity] = None
            first_failure_phase = "scraper"
            first_failure_trail: List[Dict] = []

            for entity in scraper_targets:
                post_id = self.on_demand_validator._get_post_id(entity)
                entity_result = (
                    await self.on_demand_validator.validate_entity_with_messages(
                        ctx, entity, post_id, uid
                    )
                )
                passed = bool(entity_result.get("passed"))
                trail = list(entity_result.get("messages") or [])
                comparison = entity_result.get("comparison") or {}
                validator_msg = (
                    "OK"
                    if passed
                    else (
                        trail[-1]["message"]
                        if trail
                        else (
                            comparison.get("validator_message")
                            or f"Live scraper re-validation failed for entity {post_id}."
                        )
                    )
                )
                entity_validation_results.append(
                    {
                        "uri": entity.uri,
                        "post_id": post_id,
                        "passed": passed,
                        "phase": entity_result.get("phase", "scraper"),
                        "validator_message": validator_msg,
                        **(
                            {"content_comparison": comparison}
                            if comparison
                            else {}
                        ),
                    }
                )
                if passed:
                    bt.logging.info(
                        f"UID:{uid} - OD validate: ENTITY PASSED for job {job_id}, "
                        f"post {post_id}"
                    )
                    continue

                bt.logging.warning(
                    f"UID:{uid} - OD validate: ENTITY FAILED for job {job_id}, "
                    f"post {post_id}: {validator_msg}"
                )
                if first_failed_entity is None:
                    first_failed_entity = entity
                    first_failure_phase = entity_result.get("phase", "scraper")
                    first_failure_trail = trail
                for step in trail:
                    entry = {
                        "phase": step.get("phase", "scraper"),
                        "uri": entity.uri,
                        "detail": step.get("message", validator_msg),
                        "validator_message": step.get("message", validator_msg),
                    }
                    if step.get("phase") == "scraper" and comparison:
                        entry["content_comparison"] = comparison
                    scraper_check_failures.append(entry)

            failed_entity_count = sum(
                1 for r in entity_validation_results if not r.get("passed")
            )
            if failed_entity_count:
                return _fail(
                    first_failure_phase,
                    (
                        f"{failed_entity_count}/{len(entity_validation_results)} "
                        "sampled entities failed scraper validation."
                    ),
                    entity_count=entity_count,
                    entities=schema_sample,
                    failed_entity=first_failed_entity,
                    hints=[
                        "Validator re-fetched the URI; content did not match miner submission.",
                        "For local testnet, align scraper with REDDIT_DOM / DOM-based fetchers.",
                    ],
                    check_failures=scraper_check_failures,
                    validation_trail=first_failure_trail,
                    entity_validation_results=entity_validation_results,
                )

            bt.logging.info(
                f"UID:{uid} - OD validate: PASSED job {job_id} "
                f"({entity_count} entities, schema OK, job match OK, "
                f"scraper OK for {len(entity_validation_results)} sampled entities)"
            )
            for result in entity_validation_results:
                bt.logging.info(
                    f"UID:{uid} - OD validate: entity {result.get('post_id')}: PASSED"
                )
            return OdValidationResult(
                passed=True,
                entity_count=entity_count,
                report={"entity_validation_results": entity_validation_results},
            )

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            bt.logging.warning(
                f"UID:{uid} - OD validate: network error for job {job_id}: {e}"
            )
            return OdValidationResult(passed=None, entity_count=0)
        except Exception as e:
            bt.logging.warning(
                f"UID:{uid} - OD validate: error for job {job_id}: {e}"
            )
            return _fail(
                "error",
                f"Unexpected validation error: {e}",
                hints=["See validator logs for stack trace."],
            )

    async def eval_miner(self, uid: int) -> None:
        """Evaluates a miner and updates their score.

        Specifically:
            1. Gets the latest index from the miner
            2. Chooses a random data entity bucket to query
            3. Performs basic validation on the data entity bucket (right labels, matching size, etc.)
            4. Samples data from the data entity bucket and verifies the data is correct
            5. Passes the validation result to the scorer to update the miner's score.
        """
        t_start = time.perf_counter()

        axon_info = None
        hotkey = None
        with self.lock:
            hotkey = self.metagraph.hotkeys[uid]
        axon_info = self._get_miner_axon(uid)

        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Evaluating miner.")
        self._last_index_fetch_error = ""
        get_event_bus().publish("eval_started", uid, hotkey)

        # Apply any cached OD results before the main P2P/S3 evaluation
        await self._evaluate_od(uid, hotkey)

        dashboard_settings = get_settings_manager().get()
        if dashboard_settings.skip_p2p_validation:
            await self._complete_eval_without_p2p(
                uid, hotkey, t_start, dashboard_settings
            )
            return

        # Query the miner for the latest index.
        index = await self._update_and_get_miner_index(hotkey, uid, axon_info)
        if not index:
            # The miner hasn't provided an index yet, so we can't validate them. Count as a failed validation.
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed to get an index for miner. Counting as a failed validation."
            )
            index_err = getattr(self, "_last_index_fetch_error", "") or ""
            get_event_bus().publish(
                "eval_failed",
                uid,
                hotkey,
                {
                    "phase": "index",
                    "message": "No available miner index.",
                    "detail": index_err,
                },
            )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id="(no index)",
                reason="No available miner index.",
                failure_phase="index",
                hints=[index_err] if index_err else [],
            )
            record_p2p_stats(
                uid,
                hotkey,
                jobs_passed=0,
                jobs_failed=1,
                status="failed",
                detail={"phase": "index", "bucket": "(no index)"},
            )
            self.scorer.on_miner_evaluated(
                uid,
                None,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="No available miner index.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )
            emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="index_fail")

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='unavailable miner index').observe(time.perf_counter() - t_start)
            return

        s3_validation_result = await self._maybe_run_s3_validation(
            uid, hotkey, dashboard_settings
        )

        # From that index, find a data entity bucket to sample and get it from the miner.
        chosen_data_entity_bucket = vali_utils.choose_data_entity_bucket_to_query(index)
        if chosen_data_entity_bucket is None:
            bucket_count = len(index.scorable_data_entity_buckets or [])
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Miner index has no scorable P2P buckets "
                f"({bucket_count} buckets, 0 scorable bytes). Skipping P2P bucket query."
            )
            get_event_bus().publish(
                "eval_failed",
                uid,
                hotkey,
                {
                    "phase": "p2p",
                    "message": "No scorable buckets in miner index.",
                    "detail": f"buckets={bucket_count}",
                },
            )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id="(no scorable buckets)",
                reason="No scorable buckets in miner index.",
                failure_phase="index",
                hints=[
                    "P2P index buckets may have 0 scorable_bytes under current DD weights.",
                    "S3/OD validation can still contribute to score.",
                ],
            )
            record_p2p_stats(
                uid,
                hotkey,
                jobs_passed=0,
                jobs_failed=1,
                status="failed",
                detail={"phase": "no_scorable_buckets", "bucket_count": bucket_count},
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="No scorable buckets in miner index.",
                        content_size_bytes_validated=0,
                    )
                ],
            )
            emit_miner_score_update(
                uid, hotkey, self.scorer, self.metagraph, phase="p2p_fail"
            )
            if s3_validation_result:
                self._apply_s3_validation_result(uid, hotkey, s3_validation_result)
            snapshot = emit_miner_score_update(
                uid, hotkey, self.scorer, self.metagraph, phase="complete"
            )
            get_event_bus().publish("eval_complete", uid, hotkey, snapshot)
            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(
                hotkey=self.wallet.hotkey.ss58_address,
                miner_hotkey=hotkey,
                status="no scorable buckets",
            ).observe(time.perf_counter() - t_start)
            return

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Querying miner for Bucket ID: {chosen_data_entity_bucket.id}."
        )

        responses = None
        async with bt.dendrite(wallet=self.wallet) as dendrite:
            responses = await dendrite.forward(
                axons=[axon_info],
                synapse=GetDataEntityBucket(
                    data_entity_bucket_id=chosen_data_entity_bucket.id,
                    version=constants.PROTOCOL_VERSION,
                ),
                timeout=140,
            )
        
        data_entity_bucket = vali_utils.get_single_successful_response(
            responses, GetDataEntityBucket
        )

        # Treat a failed response the same way we treat a failed validation.
        # If we didn't, the miner could just not respond to queries for data entity buckets it doesn't have.
        if data_entity_bucket is None:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Miner returned an invalid/failed response for Bucket ID: {chosen_data_entity_bucket.id}."
            )
            get_event_bus().publish(
                "eval_failed",
                uid,
                hotkey,
                {"phase": "p2p", "message": "Invalid bucket response."},
            )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id=str(chosen_data_entity_bucket.id),
                reason="Response failed or is invalid.",
                failure_phase="response",
            )
            record_p2p_stats(
                uid,
                hotkey,
                jobs_passed=0,
                jobs_failed=1,
                status="failed",
                detail={
                    "phase": "response",
                    "bucket": str(chosen_data_entity_bucket.id),
                },
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Response failed or is invalid.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )
            emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="p2p_fail")

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='invalid response').observe(time.perf_counter() - t_start)
            return

        # Perform basic validation on the entities.
        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Performing basic validation on Bucket ID: {chosen_data_entity_bucket.id} containing "
            f"{chosen_data_entity_bucket.size_bytes} bytes across {len(data_entity_bucket.data_entities)} entities."
        )

        data_entities: List[DataEntity] = data_entity_bucket.data_entities
        (valid, reason) = vali_utils.are_entities_valid(
            data_entities, chosen_data_entity_bucket
        )
        if not valid:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed basic entity validation on Bucket ID: {chosen_data_entity_bucket.id} with reason: {reason}"
            )
            get_event_bus().publish(
                "eval_failed",
                uid,
                hotkey,
                {"phase": "p2p", "message": reason},
            )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id=str(chosen_data_entity_bucket.id),
                reason=reason,
                failure_phase="basic",
                entities=data_entities,
            )
            record_p2p_stats(
                uid,
                hotkey,
                jobs_passed=0,
                jobs_failed=1,
                entities_total=len(data_entities),
                entities_checked=len(data_entities),
                status="failed",
                detail={
                    "phase": "basic",
                    "bucket": str(chosen_data_entity_bucket.id),
                    "reason": reason,
                },
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason=reason,
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )
            emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="p2p_fail")

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='invalid data entity bucket').observe(time.perf_counter() - t_start)
            return

        # Perform uniqueness validation on the entity contents.
        # If we didn't, the miner could just return the same data over and over again.
        unique = vali_utils.are_entities_unique(data_entities)
        if not unique:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed enitity uniqueness checks on Bucket ID: {chosen_data_entity_bucket.id}."
            )
            get_event_bus().publish(
                "eval_failed",
                uid,
                hotkey,
                {"phase": "p2p", "message": "Duplicate entities found."},
            )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id=str(chosen_data_entity_bucket.id),
                reason="Duplicate entities found.",
                failure_phase="uniqueness",
                entities=data_entities,
            )
            record_p2p_stats(
                uid,
                hotkey,
                jobs_passed=0,
                jobs_failed=1,
                entities_total=len(data_entities),
                entities_checked=len(data_entities),
                status="failed",
                detail={
                    "phase": "uniqueness",
                    "bucket": str(chosen_data_entity_bucket.id),
                },
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Duplicate entities found.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )
            emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="p2p_fail")

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='duplicate entities').observe(time.perf_counter() - t_start)
            return

        # Basic validation and uniqueness passed. Now sample some entities for data correctness.
        p2p_mode = getattr(
            get_settings_manager().get(), "p2p_scraper_validation_mode", "sample"
        )
        entities_to_validate: List[DataEntity] = vali_utils.choose_entities_to_verify(
            data_entities, mode=p2p_mode
        )
        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: P2P scraper mode={p2p_mode}, "
            f"validating {len(entities_to_validate)}/{len(data_entities)} entities."
        )

        entity_uris = [entity.uri for entity in entities_to_validate]

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Basic validation on Bucket ID: {chosen_data_entity_bucket.id} passed. Validating uris: {entity_uris}."
        )

        scraper = self.scraper_provider.get(
            MinerEvaluator.PREFERRED_SCRAPERS[chosen_data_entity_bucket.id.source]
        )
        validation_results = await scraper.validate(entities_to_validate)

        bt.logging.success(
            f"UID:{uid} - HOTKEY:{hotkey}: Data validation on selected entities finished with results: {validation_results}"
        )

        scraper_failed = [r for r in validation_results if not r.is_valid]
        if scraper_failed:
            from vali_utils.on_demand.on_demand_validation import ValidationContext

            source_name = DataSource(chosen_data_entity_bucket.id.source).name
            p2p_ctx = ValidationContext(source=source_name)
            failure_entries = []
            for entity, result in zip(entities_to_validate, validation_results):
                if result.is_valid:
                    continue
                comparison = (
                    await self.on_demand_validator.build_scraper_failure_comparison(
                        p2p_ctx, entity
                    )
                )
                validator_msg = result.reason or "Scraper validation failed"
                failure_entries.append(
                    {
                        "phase": "scraper",
                        "uri": entity.uri,
                        "detail": validator_msg,
                        "validator_message": validator_msg,
                        "reason": validator_msg,
                        "miner_submission": comparison.get("miner_submission"),
                        "content_comparison": comparison,
                    }
                )
            record_p2p_failure(
                uid,
                hotkey,
                bucket_id=str(chosen_data_entity_bucket.id),
                reason=f"{len(scraper_failed)}/{len(validation_results)} sampled entities failed scraper validation.",
                failure_phase="scraper",
                entities=entities_to_validate,
                failure_entries=failure_entries,
            )

        self.scorer.on_miner_evaluated(uid, index, validation_results)
        snapshot = emit_miner_score_update(
            uid, hotkey, self.scorer, self.metagraph, phase="p2p"
        )

        p2p_passed = sum(1 for r in validation_results if r.is_valid)
        p2p_failed = len(validation_results) - p2p_passed
        p2p_bucket_passed = p2p_failed == 0
        p2p_payload = {
            "passed": p2p_passed,
            "total": len(validation_results),
            "bucket": str(chosen_data_entity_bucket.id),
            "jobs_total": 1,
            "jobs_passed": 1 if p2p_bucket_passed else 0,
            "jobs_failed": 0 if p2p_bucket_passed else 1,
            "entities_total": len(data_entities),
            "entities_checked": len(entities_to_validate),
            "entities_passed": p2p_passed,
            "entities_failed": p2p_failed,
            **{k: snapshot[k] for k in ("score", "credibility", "local_incentive")},
        }
        get_event_bus().publish("eval_p2p_complete", uid, hotkey, p2p_payload)
        record_p2p_stats(
            uid,
            hotkey,
            jobs_passed=1 if p2p_bucket_passed else 0,
            jobs_failed=0 if p2p_bucket_passed else 1,
            entities_total=len(data_entities),
            entities_checked=len(entities_to_validate),
            entities_passed=p2p_passed,
            entities_failed=p2p_failed,
            status="passed" if p2p_bucket_passed else "partial" if p2p_passed else "failed",
            detail={
                "bucket": str(chosen_data_entity_bucket.id),
                "scraper_mode": p2p_mode,
            },
        )

        # Force garbage collection to free miner index objects (can be 350K+ buckets per miner)
        del index
        gc.collect()

        metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='ok').observe(time.perf_counter() - t_start)

        if s3_validation_result:
            self._apply_s3_validation_result(uid, hotkey, s3_validation_result)

        snapshot = emit_miner_score_update(
            uid, hotkey, self.scorer, self.metagraph, phase="complete"
        )
        get_event_bus().publish(
            "eval_complete",
            uid,
            hotkey,
            snapshot,
        )

    async def _maybe_run_s3_validation(
        self, uid: int, hotkey: str, dashboard_settings
    ) -> Optional[S3ValidationResult]:
        """Run S3/parquet validation when enabled and due."""
        current_block = int(self.metagraph.block)
        s3_validation_info = self.s3_storage.get_validation_info(hotkey)
        s3_interval_minutes = max(
            1, int(getattr(dashboard_settings, "s3_validation_interval_minutes", 120) or 120)
        )
        # Bittensor blocks are ~12 seconds apart on testnet/mainnet.
        s3_interval_blocks = max(1, int(s3_interval_minutes * 60 / 12))

        if (
            not dashboard_settings.skip_s3_validation
            and (
                not s3_validation_info
                or (current_block - s3_validation_info["block"]) > s3_interval_blocks
            )
        ):
            return await self._perform_s3_validation(uid, hotkey, current_block)
        return None

    async def _complete_eval_without_p2p(
        self, uid: int, hotkey: str, t_start: float, dashboard_settings
    ) -> None:
        """Finish an eval cycle without P2P index/bucket/scraper checks."""
        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: P2P validation skipped (dashboard setting)."
        )
        s3_validation_result = await self._maybe_run_s3_validation(
            uid, hotkey, dashboard_settings
        )
        snapshot = emit_miner_score_update(
            uid, hotkey, self.scorer, self.metagraph, phase="p2p_skipped"
        )
        p2p_payload = {
            "skipped": True,
            "passed": 0,
            "total": 0,
            "jobs_total": 0,
            "jobs_passed": 0,
            "jobs_failed": 0,
            "entities_total": 0,
            "entities_checked": 0,
            "entities_passed": 0,
            "entities_failed": 0,
            "message": "P2P validation skipped via dashboard",
        }
        for key in ("score", "credibility", "local_incentive"):
            if key in snapshot:
                p2p_payload[key] = snapshot[key]
        get_event_bus().publish("eval_p2p_complete", uid, hotkey, p2p_payload)
        if s3_validation_result:
            self._apply_s3_validation_result(uid, hotkey, s3_validation_result)
        snapshot = emit_miner_score_update(
            uid, hotkey, self.scorer, self.metagraph, phase="complete"
        )
        get_event_bus().publish("eval_complete", uid, hotkey, snapshot)
        metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(
            hotkey=self.wallet.hotkey.ss58_address,
            miner_hotkey=hotkey,
            status="p2p skipped",
        ).observe(time.perf_counter() - t_start)

    def _apply_s3_validation_result(
        self, uid: int, hotkey: str, s3_validation_result: S3ValidationResult
    ) -> None:
        """Publish S3 eval events and update scorer after validation."""
        if not s3_validation_result.is_valid:
            record_s3_failure(uid, hotkey, s3_validation_result)
        record_s3_stats(uid, hotkey, s3_validation_result)
        s3_payload = {
            "is_valid": s3_validation_result.is_valid,
            "validation_pct": s3_validation_result.validation_percentage,
            "reason": s3_validation_result.reason,
            "effective_size_mb": s3_validation_result.effective_size_bytes / (1024 * 1024),
            "issues": list(s3_validation_result.validation_issues or [])[:5],
            "jobs_total": s3_validation_result.total_active_jobs,
            "jobs_checked": s3_validation_result.recent_files_count,
            "jobs_passed": s3_validation_result.total_active_jobs
            if s3_validation_result.is_valid
            else 0,
            "entities_total": s3_validation_result.entities_validated,
            "entities_checked": s3_validation_result.entities_validated,
            "entities_passed": s3_validation_result.entities_passed_scraper,
        }
        get_event_bus().publish("eval_s3_complete", uid, hotkey, s3_payload)
        if s3_validation_result.is_valid:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Miner {uid} passed S3 validation. "
                f"Validation: {s3_validation_result.validation_percentage:.1f}%, "
                f"Jobs: {s3_validation_result.total_active_jobs}, Files: {s3_validation_result.recent_files_count}, "
                f"Coverage: {s3_validation_result.job_coverage_rate:.1f}%, "
                f"Effective size: {s3_validation_result.effective_size_bytes/(1024*1024):.1f}MB, "
                f"Job match: {s3_validation_result.job_match_rate:.1f}%"
            )
        else:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Miner {uid} did not pass S3 validation. "
                f"Reason: {s3_validation_result.reason}"
            )
        self.scorer.update_s3_effective_size(
            uid=uid,
            effective_size=s3_validation_result.effective_size_bytes,
            validation_passed=s3_validation_result.is_valid,
        )
        emit_miner_score_update(uid, hotkey, self.scorer, self.metagraph, phase="s3")

    async def _perform_s3_validation(
        self, uid: int, hotkey: str, current_block: int
    ) -> Optional[S3ValidationResult]:
        """
        Performs S3 validation using DuckDB-based sampled validation.

        Returns:
            An S3ValidationResult with validation details or None if no S3 data is found.
        """
        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Starting comprehensive S3 validation")

        try:
            # Use S3 auth URL from config
            s3_auth_url = self.config.s3_auth_url
            
            s3_mode = getattr(
                get_settings_manager().get(), "s3_validation_mode", "sample"
            )
            s3_full = s3_mode == "full"
            s3_validation_result = await validate_s3_miner_data(
                self.wallet,
                s3_auth_url,
                hotkey,
                config=self.config,
                s3_reader=self.s3_reader,
                sample_percent=100.0 if s3_full else 10.0,
                full_validation=s3_full,
            )
            if s3_full:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: S3 full validation enabled "
                    f"(all active job files)"
                )
            
            # Log results with rich table
            summary = get_s3_validation_summary(s3_validation_result)
            bt.logging.info(f"{hotkey}: {summary}")

            # Display rich table with detailed metrics
            try:
                log_s3_validation_table(
                    result=s3_validation_result,
                    uid=uid,
                    hotkey=hotkey,
                    pagination_stats=None  # Could add pagination stats if available
                )
            except Exception as e:
                bt.logging.debug(f"Error displaying S3 validation table: {e}")

            if not s3_validation_result.is_valid and s3_validation_result.validation_issues:
                bt.logging.debug(f"{hotkey}: S3 validation issues: {', '.join(s3_validation_result.validation_issues[:3])}")

        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in S3 validation: {str(e)}")
            s3_validation_result = S3ValidationResult(
                is_valid=False,
                validation_percentage=0.0,
                total_active_jobs=0,
                expected_jobs_count=0,
                recent_jobs_analyzed=0,
                recent_files_count=0,
                total_size_bytes=0,
                has_duplicates=False,
                duplicate_percentage=0.0,
                entities_validated=0,
                entities_passed_scraper=0,
                scraper_success_rate=0.0,
                entities_checked_for_job_match=0,
                entities_matched_job=0,
                job_match_rate=0.0,
                validation_issues=[f"Validation error: {str(e)}"],
                reason=f"S3 validation failed: {str(e)}",
                sample_validation_results=[],
                sample_job_mismatches=[]
            )

        # Update S3 validation storage
        if s3_validation_result:
            self.s3_storage.update_validation_info(hotkey, s3_validation_result.total_active_jobs, current_block)

        return s3_validation_result

    async def run_next_eval_batch(self) -> int:
        """Asynchronously runs the next batch of miner evaluations and returns the number of seconds to wait until the next batch.

        Args:
            block (int): The block at which we started this evaluation.
        """
        settings_mgr = get_settings_manager()
        settings = settings_mgr.get()
        force_eval = settings_mgr.consume_trigger_eval()

        if settings.evaluation_paused and not force_eval:
            return 30.0

        # Grab a snapshot of the metagraph
        metagraph = None
        with self.lock:
            metagraph = copy.deepcopy(self.metagraph)

        # When target miners are configured, evaluate only those UIDs.
        if settings.target_miner_uids:
            uids_to_eval = set(settings.target_miner_uids)
            if not uids_to_eval:
                return 30.0
            try:
                self.metagraph_syncer.force_sync(self.config.netuid)
            except Exception as e:
                bt.logging.warning(f"Metagraph force-sync failed before eval: {e}")
        else:
            # Check if the next miner is due an update.
            next_uid = self.miner_iterator.peek()
            hotkey = metagraph.hotkeys[next_uid]
            last_evaluated = self.storage.read_miner_last_updated(hotkey)
            now = dt.datetime.utcnow()
            due_update = (
                force_eval
                or last_evaluated is None
                or (now - last_evaluated) >= constants.MIN_EVALUATION_PERIOD
            )

            if not due_update:
                return (
                    last_evaluated + constants.MIN_EVALUATION_PERIOD - now
                ).total_seconds()

            miners_to_eval = settings.eval_batch_size
            uids_to_eval = {
                next(self.miner_iterator) for _ in range(miners_to_eval)
            }

        t_start = time.perf_counter()

        bt.logging.info(
            f"Running validation on the following batch of uids: {uids_to_eval}."
        )
        threads = [
            threading.Thread(target=self.eval_miner_sync, args=(uid,))
            for uid in uids_to_eval
        ]
        for thread in threads:
            thread.start()

        bt.logging.trace(f"Waiting for {len(threads)} miner evals to finish.")
        end = datetime.datetime.now() + datetime.timedelta(seconds=300)
        for t in threads:
            # Compute the timeout, so that all threads are waited for a total of 5 minutes.
            timeout = max(0, (end - datetime.datetime.now()).total_seconds())
            t.join(timeout=timeout)

        duration = time.perf_counter() - t_start
        metrics.MINER_EVALUATOR_EVAL_BATCH_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address).observe(duration)

        bt.logging.trace(f"Finished waiting for {len(threads)} miner eval.")

        if settings.target_miner_uids:
            interval = max(60, int(settings.target_eval_interval_seconds or 300))
            return float(interval)

        # Run the next evaluation batch immediately.
        return 0

    def _get_miner_axon(self, uid: int) -> bt.AxonInfo:
        """Return miner axon info, applying dashboard overrides when configured."""
        with self.lock:
            axon = copy.deepcopy(self.metagraph.axons[uid])

        settings = get_settings_manager().get()
        override = (settings.miner_axon_overrides or {}).get(str(uid))
        if override and ":" in override:
            host, port_str = override.rsplit(":", 1)
            axon.ip = host
            axon.port = int(port_str)
            bt.logging.info(
                f"UID:{uid} - Using dashboard axon override: {host}:{port_str}"
            )
        else:
            bt.logging.debug(
                f"UID:{uid} - Evaluating via metagraph axon {axon.ip}:{axon.port}"
            )
        return axon

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.trace("Saving evaluator state.")

        if not os.path.exists(self.config.neuron.full_path):
            os.makedirs(self.config.neuron.full_path)

        # Save the state of the validator to file.
        self.scorer.save_state(
            os.path.join(self.config.neuron.full_path, MinerEvaluator.SCORER_FILENAME)
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading evaluator state.")

        with self.lock:
            # Load the state of the validator from file.
            filepath = os.path.join(
                self.config.neuron.full_path, MinerEvaluator.SCORER_FILENAME
            )
            if not os.path.exists(filepath):
                bt.logging.warning("No scorer state file found. Starting from scratch.")
                return

            try:
                self.scorer.load_state(filepath)
                bt.logging.success(f"Loaded scorer state from: {filepath}.")
            except Exception as e:
                bt.logging.warning(
                    f"Failed to load scorer state. Reason: {e}. Starting from scratch."
                )

            # Resize the scorer in case the loaded state is old and missing newly added neurons.
            self.scorer.resize(len(self.metagraph.hotkeys))

    async def _update_and_get_miner_index(
        self, hotkey: str, uid: int, miner_axon: bt.AxonInfo
    ) -> Optional[ScorableMinerIndex]:
        """Updates the index for the specified miner, and returns the latest known index or None if the miner hasn't yet provided an index."""

        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Getting MinerIndex from miner.")

        try:
            responses: List[GetMinerIndex] = None
            async with bt.dendrite(wallet=self.wallet) as dendrite:
                responses = await dendrite.forward(
                    axons=[miner_axon],
                    synapse=GetMinerIndex(version=constants.PROTOCOL_VERSION),
                    timeout=120,
                )

            response = vali_utils.get_single_successful_response(
                responses, GetMinerIndex
            )
            if not response:
                fail_detail = ""
                if responses and responses[0] and responses[0].dendrite:
                    fail_detail = (
                        f"{responses[0].dendrite.status_code}: "
                        f"{responses[0].dendrite.status_message}"
                    )
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner failed to respond with an index"
                    + (f" ({fail_detail})" if fail_detail else "")
                    + ". Using last known index if present."
                )
                self._last_index_fetch_error = fail_detail
                # Miner failed to update the index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            # Validate the index.
            miner_index = None
            try:
                miner_index = vali_utils.get_miner_index_from_response(response)
            except ValueError as e:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner returned an invalid index. Reason: {e}. Using last known index if present."
                )
                # Miner returned an invalid index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            assert miner_index is not None, "Miner index should not be None."

            # Miner replied with a valid index. Store it and return it.
            miner_credibility = self.scorer.get_miner_credibility(uid)
            bt.logging.success(
                f"UID:{uid} - HOTKEY:{hotkey}: Got new compressed miner index of {CompressedMinerIndex.size_bytes(miner_index)} bytes "
                f"across {CompressedMinerIndex.bucket_count(miner_index)} buckets."
            )
            self.storage.upsert_compressed_miner_index(
                miner_index, hotkey, miner_credibility
            )

            return self.storage.read_miner_index(hotkey)
        except Exception:
            bt.logging.error(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed to update and get miner index.\n{traceback.format_exc()}"
            )
            return None

    def _on_metagraph_updated(self, metagraph: bt.metagraph, netuid: int):
        """Handles an update to a metagraph."""
        bt.logging.info(
            f"Evaluator processing an update to metagraph on subnet {netuid}."
        )

        with self.lock:
            bt.logging.info(
                "Evaluator: Metagraph updated, re-syncing hotkeys, and moving averages."
            )
            # Zero out all hotkeys that have been replaced.
            old_hotkeys = self.metagraph.hotkeys
            for uid, hotkey in enumerate(old_hotkeys):
                if hotkey != metagraph.hotkeys[uid] or (
                    not utils.is_miner(uid, metagraph, self.vpermit_rao_limit)
                    and not utils.is_validator(uid, metagraph, self.vpermit_rao_limit)
                ):
                    bt.logging.info(
                        f"Hotkey {hotkey} w/ UID {uid} has been unregistered or does not qualify to mine/validate."
                    )
                    self.scorer.reset(uid)  # hotkey has been replaced
                    self._last_od_eval_at.pop(hotkey, None)
                    try:
                        self.storage.delete_miner(hotkey)
                    except Exception:
                        bt.logging.error(
                            f"{hotkey} Failed to delete miner index.",
                            traceback.format_exc(),
                        )
            # Update the iterator. It will keep its current position if possible.
            self.miner_iterator.set_miner_uids(
                #utils.get_miner_uids(self.metagraph, self.vpermit_rao_limit) # uses cached/stale self.metagraph --> iterator may miss new miners and keep removed ones.
                utils.get_miner_uids(metagraph, self.vpermit_rao_limit) # use fresh metagraph --> iterator gets latest eligible UIDs immediately
            )

            # Check to see if the metagraph has changed size.
            # If so, we need to add new hotkeys and moving averages.
            if len(self.metagraph.hotkeys) < len(metagraph.hotkeys):
                self.scorer.resize(len(metagraph.hotkeys))

            self.metagraph = copy.deepcopy(metagraph)

    def exit(self):
        self.should_exit = True
