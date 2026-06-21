"""
Miner score snapshots and history for the validator dashboard.

Computes composite score components, local incentive share (normalized weight),
and optional on-chain metagraph incentive.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

import torch

from rewards.miner_scorer import MinerScorer
from vali_utils.dashboard.events import get_event_bus


def build_miner_score_row(
    uid: int,
    hotkey: str,
    scorer: MinerScorer,
    metagraph: Any,
    capped_scores: Optional[torch.Tensor] = None,
    relax_service_cap: bool = False,
) -> Dict[str, Any]:
    """Build a full dashboard metrics row for one miner."""
    with scorer.lock:
        components = scorer._compute_weight_components_unlocked(
            uid, relax_service_cap=relax_service_cap
        )
        raw_score = float(scorer.scores[uid].item())
        row = {
            "uid": uid,
            "hotkey": hotkey,
            "score": raw_score,
            "credibility": float(scorer.miner_credibility[uid].item()),
            "s3_boost": float(scorer.s3_boosts[uid].item()),
            "s3_credibility": float(scorer.s3_credibility[uid].item()),
            "od_boost": float(scorer.ondemand_boosts[uid].item()),
            "od_credibility": float(scorer.ondemand_credibility[uid].item()),
            "scorable_bytes": float(scorer.scorable_bytes[uid].item()),
            "capped_score": components["capped_score"],
            "p2p_component": components["p2p_component"],
            "s3_component": components["s3_component"],
            "od_component": components["od_component"],
        }

    if capped_scores is None:
        capped_scores = scorer.get_scores_for_weights(
            relax_service_cap=relax_service_cap
        )

    total = float(capped_scores.sum().item())
    capped_val = float(capped_scores[uid].item())
    row["local_incentive"] = capped_val / total if total > 0 else 0.0
    row["local_weight"] = row["local_incentive"]

    try:
        row["chain_incentive"] = float(metagraph.I[uid].item())
    except Exception:
        row["chain_incentive"] = 0.0

    row["timestamp"] = datetime.now(timezone.utc).isoformat()
    return row


class ScoreHistoryStore:
    """Thread-safe per-UID score history for dashboard charts."""

    MAX_POINTS_PER_UID = 120

    def __init__(self):
        self._lock = threading.RLock()
        self._history: Dict[int, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.MAX_POINTS_PER_UID)
        )

    def record(self, uid: int, snapshot: Dict[str, Any]) -> None:
        point = {
            "timestamp": snapshot.get("timestamp")
            or datetime.now(timezone.utc).isoformat(),
            "score": snapshot.get("score", 0),
            "capped_score": snapshot.get("capped_score", 0),
            "local_incentive": snapshot.get("local_incentive", 0),
            "chain_incentive": snapshot.get("chain_incentive", 0),
            "credibility": snapshot.get("credibility", 0),
            "s3_boost": snapshot.get("s3_boost", 0),
            "od_boost": snapshot.get("od_boost", 0),
            "scorable_bytes": snapshot.get("scorable_bytes", 0),
            "p2p_component": snapshot.get("p2p_component", 0),
            "s3_component": snapshot.get("s3_component", 0),
            "od_component": snapshot.get("od_component", 0),
        }
        with self._lock:
            self._history[uid].append(point)

    def get(self, uid: int, limit: int = 60) -> List[Dict[str, Any]]:
        with self._lock:
            points = list(self._history.get(uid, []))
        return points[-limit:]

    def get_multi(self, uids: List[int], limit: int = 60) -> Dict[int, List[Dict[str, Any]]]:
        return {uid: self.get(uid, limit) for uid in uids}

    def clear(self, uids: Optional[List[int]] = None) -> None:
        """Remove stored chart history (all miners or selected UIDs)."""
        with self._lock:
            if uids is None:
                self._history.clear()
            else:
                for uid in uids:
                    self._history.pop(uid, None)


_history_store: Optional[ScoreHistoryStore] = None


def get_score_history() -> ScoreHistoryStore:
    global _history_store
    if _history_store is None:
        _history_store = ScoreHistoryStore()
    return _history_store


def emit_miner_score_update(
    uid: int,
    hotkey: str,
    scorer: MinerScorer,
    metagraph: Any,
    phase: str = "",
    relax_service_cap: Optional[bool] = None,
) -> Dict[str, Any]:
    """Publish a score_updated SSE event and record history."""
    if relax_service_cap is None:
        from vali_utils.dashboard.settings import get_settings_manager

        relax_service_cap = get_settings_manager().get().relax_weight_caps
    capped_scores = scorer.get_scores_for_weights(relax_service_cap=relax_service_cap)
    snapshot = build_miner_score_row(
        uid,
        hotkey,
        scorer,
        metagraph,
        capped_scores,
        relax_service_cap=relax_service_cap,
    )
    if phase:
        snapshot["phase"] = phase
    get_score_history().record(uid, snapshot)
    get_event_bus().publish("score_updated", uid, hotkey, snapshot)
    return snapshot
