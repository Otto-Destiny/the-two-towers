from __future__ import annotations

import base64
import hashlib
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from watchtower.config import Config
from watchtower.vector_store import VectorStore


def _stable_slot(value: str, width: int) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % width


def _moment(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _unit(values: list[float] | np.ndarray) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float32)
    magnitude = float(np.linalg.norm(vector))
    if magnitude == 0:
        return vector
    return vector / magnitude


def _cosine(left: list[float] | np.ndarray, right: list[float] | np.ndarray) -> float:
    lhs = _unit(left)
    rhs = _unit(right)
    if lhs.shape != rhs.shape or lhs.size == 0:
        return 0.0
    return float(np.dot(lhs, rhs))


def _robust(values: list[float]) -> list[float]:
    if not values:
        return []
    if len(values) == 1:
        return [float(values[0])]
    center = float(np.median(values))
    spread = float(np.median(np.abs(np.asarray(values) - center))) * 1.4826
    if spread <= 1e-6:
        return [0.0 for _ in values]
    return [(value - center) / spread for value in values]


def clip_quality(views: float, likes: float, shares: float, comments: float, saves: float) -> tuple[float, float]:
    positive = max(likes, 0.0) + 2.6 * max(shares, 0.0) + 1.7 * max(saves, 0.0) + 0.6 * max(comments, 0.0)
    alpha = 1.0 + positive
    beta = 1.0 + max(views - positive, 0.0)
    mean = alpha / (alpha + beta)
    variance = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1.0))
    return mean, math.sqrt(max(variance, 0.0))


def encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def decode_cursor(raw_cursor: str | None, lane: str) -> dict[str, Any]:
    if not raw_cursor:
        return {"lane": lane, "page": 0, "seen_refs": []}
    try:
        payload = json.loads(base64.urlsafe_b64decode(raw_cursor.encode("utf-8")).decode("utf-8"))
        return {
            "lane": str(payload.get("lane", lane)),
            "page": int(payload.get("page", 0)),
            "seen_refs": [str(item) for item in payload.get("seen_refs", [])],
        }
    except Exception:
        return {"lane": lane, "page": 0, "seen_refs": []}


@dataclass(slots=True)
class RankedClip:
    clip_ref: str
    score: float
    maker_ref: str
    topic: str
    territory: str
    payload: dict[str, Any]
    breakdown: dict[str, float]


class FeedEngine:
    def __init__(self, config: Config, vector_store: VectorStore | None = None) -> None:
        self.config = config
        self.vector_store = vector_store
        self.snapshot_path = Path(config.snapshot_path)
        self.catalog: dict[str, dict[str, Any]] = {}
        self.built_at = ""

    def load(self) -> None:
        if not self.snapshot_path.exists():
            self.catalog = {}
            self.built_at = ""
            return
        raw = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        self.built_at = str(raw.get("built_at", ""))
        self.catalog = {item["clip_ref"]: item for item in raw.get("entries", [])}

    def rebuild(self, clips: list[dict[str, Any]]) -> dict[str, Any]:
        entries = []
        for clip in clips:
            signature = self._clip_signature(clip)
            quality, uncertainty = clip_quality(
                views=float(clip.get("Views", 0.0) or 0.0),
                likes=float(clip.get("Likes", 0.0) or 0.0),
                shares=float(clip.get("Shares", 0.0) or 0.0),
                comments=float(clip.get("Comments", 0.0) or 0.0),
                saves=float(clip.get("Saves", 0.0) or 0.0),
            )
            freshness = self._freshness(clip.get("createdAt"))
            entries.append(
                {
                    "clip_ref": clip["clipRef"],
                    "maker_ref": clip.get("makerRef", ""),
                    "topic": clip.get("topic", ""),
                    "territory": clip.get("territory", ""),
                    "language": clip.get("language", ""),
                    "headline": clip.get("headline", ""),
                    "created_at": _moment(clip.get("createdAt")).isoformat(),
                    "quality": quality,
                    "uncertainty": uncertainty,
                    "freshness": freshness,
                    "signature": signature.tolist(),
                    "payload": self._json_clip(clip),
                }
            )
        payload = {"built_at": datetime.now(timezone.utc).isoformat(), "entries": entries}
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        self.load()
        summary = {"built_at": self.built_at, "clip_count": len(entries)}
        if self.vector_store is not None:
            summary["vector"] = self.vector_store.replace(entries)
        return summary

    def status(self) -> dict[str, Any]:
        return {"clip_count": len(self.catalog), "built_at": self.built_at}

    def viewer_summary(self, viewer_state: dict[str, Any]) -> dict[str, Any]:
        profile = self._viewer_profile(viewer_state)
        top_topics = sorted(profile["topic_weights"].items(), key=lambda item: item[1], reverse=True)[:5]
        top_makers = sorted(profile["maker_weights"].items(), key=lambda item: item[1], reverse=True)[:5]
        return {
            "viewer_ref": viewer_state["viewer_ref"],
            "topics": dict(top_topics),
            "makers": dict(top_makers),
            "territories": dict(sorted(profile["territory_weights"].items(), key=lambda item: item[1], reverse=True)[:3]),
        }

    def for_you(self, viewer_state: dict[str, Any], batch_size: int, omit_refs: list[str], cursor: str | None) -> dict[str, Any]:
        state = decode_cursor(cursor, "for-you")
        excluded = list(dict.fromkeys(list(omit_refs) + list(state["seen_refs"]) + [event["clip_ref"] for event in viewer_state["watch_events"]]))
        profile = self._viewer_profile(viewer_state)
        query_vector = profile["vector"]
        if not self._has_signal(query_vector):
            query_vector = self._trend_vector(viewer_state.get("territory", ""))
        pool = self._ann_pool(query_vector, batch_size, viewer_state.get("territory", ""), excluded)
        if not pool:
            pool = self._local_pool(excluded, viewer_state.get("territory", ""))
        ranked = self._rank_lane(profile, pool, "for_you", batch_size)
        return self._response("for-you", state, ranked, batch_size)

    def following(self, viewer_state: dict[str, Any], clips: list[dict[str, Any]], batch_size: int, omit_refs: list[str], cursor: str | None) -> dict[str, Any]:
        state = decode_cursor(cursor, "following")
        excluded = list(dict.fromkeys(list(omit_refs) + list(state["seen_refs"])))
        profile = self._viewer_profile(viewer_state)
        pool = []
        for clip in clips:
            if clip["clipRef"] in excluded:
                continue
            pool.append(self._entry_from_clip(clip))
        ranked = self._rank_lane(profile, pool, "following", batch_size)
        return self._response("following", state, ranked, batch_size)

    def trending(self, viewer_state: dict[str, Any], batch_size: int, omit_refs: list[str], cursor: str | None) -> dict[str, Any]:
        state = decode_cursor(cursor, "trending")
        excluded = list(dict.fromkeys(list(omit_refs) + list(state["seen_refs"])))
        profile = self._viewer_profile(viewer_state)
        trend_vector = self._trend_vector(viewer_state.get("territory", ""))
        pool = self._ann_pool(trend_vector, batch_size, viewer_state.get("territory", ""), excluded)
        if not pool:
            pool = [entry for entry in self.catalog.values() if entry["clip_ref"] not in set(excluded)]
        if not pool:
            pool = list(self.catalog.values())
        ranked = self._rank_lane(profile, pool, "trending", batch_size)
        return self._response("trending", state, ranked, batch_size)

    def related(self, anchor_ref: str, batch_size: int) -> dict[str, Any]:
        anchor = self.catalog.get(anchor_ref)
        if not anchor:
            return {"items": [], "continuationMark": None, "more": False}
        if self.vector_store is not None and self.vector_store.has_entries():
            hits = self.vector_store.related(
                anchor["signature"],
                limit=max(batch_size * 4, batch_size),
                territory=anchor.get("territory", ""),
                omit_refs=[anchor_ref],
            )
            scored = []
            for hit in hits:
                entry = self.catalog.get(hit.clip_ref)
                if not entry:
                    continue
                similarity = max(float(hit.score), _cosine(anchor["signature"], entry["signature"]))
                score = 0.75 * similarity + 0.15 * entry["quality"] + 0.10 * entry["freshness"]
                scored.append(
                    (
                        score,
                        entry,
                        {
                            "similarity": similarity,
                            "quality": entry["quality"],
                            "freshness": entry["freshness"],
                        },
                    )
                )
            if scored:
                scored.sort(key=lambda item: item[0], reverse=True)
                items = [
                    RankedClip(
                        clip_ref=entry["clip_ref"],
                        score=score,
                        maker_ref=entry["maker_ref"],
                        topic=entry["topic"],
                        territory=entry["territory"],
                        payload=dict(entry["payload"]),
                        breakdown={key: float(value) for key, value in detail.items()} | {"final": float(score)},
                    )
                    for score, entry, detail in scored[:batch_size]
                ]
                return {"items": items, "continuationMark": None, "more": len(scored) > batch_size}
        scored = []
        for entry in self.catalog.values():
            if entry["clip_ref"] == anchor_ref:
                continue
            similarity = _cosine(anchor["signature"], entry["signature"])
            score = 0.7 * similarity + 0.2 * entry["quality"] + 0.1 * entry["freshness"]
            scored.append((score, entry, {"similarity": similarity, "quality": entry["quality"], "freshness": entry["freshness"]}))
        scored.sort(key=lambda item: item[0], reverse=True)
        items = [
            RankedClip(
                clip_ref=entry["clip_ref"],
                score=score,
                maker_ref=entry["maker_ref"],
                topic=entry["topic"],
                territory=entry["territory"],
                payload=dict(entry["payload"]),
                breakdown={key: float(value) for key, value in detail.items()} | {"final": float(score)},
            )
            for score, entry, detail in scored[:batch_size]
        ]
        return {"items": items, "continuationMark": None, "more": len(scored) > batch_size}

    def _response(self, lane: str, state: dict[str, Any], ranked: list[RankedClip], batch_size: int) -> dict[str, Any]:
        seen_refs = (state["seen_refs"] + [item.clip_ref for item in ranked])[-self.config.max_cursor_seen :]
        continuation = encode_cursor({"lane": lane, "page": state["page"] + 1, "seen_refs": seen_refs})
        return {"items": ranked[:batch_size], "continuationMark": continuation, "more": len(ranked) >= batch_size}

    def _rank_lane(self, profile: dict[str, Any], pool: list[dict[str, Any]], lane: str, batch_size: int) -> list[RankedClip]:
        if not pool:
            return []
        raw = []
        for entry in pool[: self.config.max_candidates]:
            affinity = _cosine(profile["vector"], entry["signature"])
            topic = profile["topic_weights"].get(entry["topic"], 0.0)
            maker = profile["maker_weights"].get(entry["maker_ref"], 0.0)
            territory = profile["territory_weights"].get(entry["territory"], 0.0)
            exploration = entry["uncertainty"] * (1.0 - maker) * max(self.config.exploration_scale, 0.0)
            following_seed = float(entry.get("_following_seed", 0.0))
            raw.append(
                {
                    "entry": entry,
                    "affinity": affinity,
                    "topic": topic,
                    "maker": maker + following_seed,
                    "territory": territory,
                    "quality": entry["quality"],
                    "freshness": entry["freshness"],
                    "exploration": exploration,
                }
            )
        components = {name: _robust([item[name] for item in raw]) for name in ("affinity", "topic", "maker", "territory", "quality", "freshness", "exploration")}
        weights = {
            "for_you": {"affinity": 0.34, "topic": 0.18, "maker": 0.11, "territory": 0.05, "quality": 0.17, "freshness": 0.07, "exploration": 0.08},
            "following": {"affinity": 0.10, "topic": 0.10, "maker": 0.38, "territory": 0.05, "quality": 0.17, "freshness": 0.12, "exploration": 0.08},
            "trending": {"affinity": 0.08, "topic": 0.06, "maker": 0.06, "territory": 0.08, "quality": 0.37, "freshness": 0.20, "exploration": 0.15},
        }[lane]
        candidates = []
        for index, item in enumerate(raw):
            base = sum(weights[name] * components[name][index] for name in weights)
            breakdown = {name: float(item[name]) for name in weights}
            breakdown["blend"] = float(base)
            candidates.append((base, item["entry"], breakdown))
        candidates.sort(key=lambda item: item[0], reverse=True)
        return self._rerank(candidates, batch_size, lane)

    def _rerank(self, candidates: list[tuple[float, dict[str, Any], dict[str, float]]], batch_size: int, lane: str) -> list[RankedClip]:
        chosen: list[tuple[float, dict[str, Any], dict[str, float]]] = []
        remaining = list(candidates)
        diversity = self.config.diversity_scale * (1.0 if lane == "for_you" else 0.75 if lane == "following" else 0.55)
        while remaining and len(chosen) < batch_size:
            best_index = 0
            best_score = float("-inf")
            maker_counts = Counter(item[1]["maker_ref"] for item in chosen)
            topic_counts = Counter(item[1]["topic"] for item in chosen)
            for index, (base, entry, detail) in enumerate(remaining):
                redundancy = max((_cosine(entry["signature"], other[1]["signature"]) for other in chosen), default=0.0)
                penalty = diversity * redundancy + 0.08 * maker_counts[entry["maker_ref"]] + 0.04 * topic_counts[entry["topic"]]
                value = base - penalty
                if value > best_score:
                    best_score = value
                    best_index = index
            base, entry, detail = remaining.pop(best_index)
            detail["diversity_penalty"] = float(base - best_score)
            detail["final"] = float(best_score)
            chosen.append((base, entry, detail))
        return [
            RankedClip(
                clip_ref=entry["clip_ref"],
                score=float(detail["final"]),
                maker_ref=entry["maker_ref"],
                topic=entry["topic"],
                territory=entry["territory"],
                payload=dict(entry["payload"]),
                breakdown=detail,
            )
            for _, entry, detail in chosen
        ]

    def _viewer_profile(self, viewer_state: dict[str, Any]) -> dict[str, Any]:
        vector = np.zeros(self.config.signature_width, dtype=np.float32)
        topic_weights: Counter[str] = Counter()
        maker_weights: Counter[str] = Counter()
        territory_weights: Counter[str] = Counter()

        for index, event in enumerate(reversed(viewer_state.get("watch_events", [])[-80:]), start=1):
            clip = self.catalog.get(event["clip_ref"])
            if not clip:
                continue
            weight = (0.4 + min(max(float(event.get("watch_ratio", 0.0)), 0.0), 1.5)) / math.sqrt(index)
            vector += np.asarray(clip["signature"], dtype=np.float32) * weight
            topic_weights[clip["topic"]] += weight
            maker_weights[clip["maker_ref"]] += weight
            territory_weights[clip["territory"]] += weight

        for index, clip_ref in enumerate(reversed(viewer_state.get("liked_refs", [])[-80:]), start=1):
            clip = self.catalog.get(clip_ref)
            if not clip:
                continue
            weight = 1.7 / math.sqrt(index)
            vector += np.asarray(clip["signature"], dtype=np.float32) * weight
            topic_weights[clip["topic"]] += weight
            maker_weights[clip["maker_ref"]] += weight
            territory_weights[clip["territory"]] += weight

        for index, maker_ref in enumerate(reversed(viewer_state.get("following_refs", [])[-160:]), start=1):
            maker_weights[maker_ref] += 1.2 / math.sqrt(index)

        if viewer_state.get("territory"):
            territory_weights[viewer_state["territory"]] += 1.0

        return {
            "vector": _unit(vector),
            "topic_weights": self._normalize_counter(topic_weights),
            "maker_weights": self._normalize_counter(maker_weights),
            "territory_weights": self._normalize_counter(territory_weights),
        }

    def _local_pool(self, excluded: list[str], territory: str) -> list[dict[str, Any]]:
        pool = [entry for entry in self.catalog.values() if entry["clip_ref"] not in set(excluded)]
        if territory:
            localized = [entry for entry in pool if entry["territory"] in ("", territory)]
            pool = localized or pool
        return pool

    def _ann_pool(self, query_vector: np.ndarray, batch_size: int, territory: str, excluded: list[str]) -> list[dict[str, Any]]:
        if self.vector_store is None or not self.vector_store.has_entries() or not self._has_signal(query_vector):
            return []
        hits = self.vector_store.search(
            query_vector,
            limit=max(self.config.max_candidates, batch_size * 12),
            territory=territory,
            omit_refs=excluded,
        )
        pool = []
        for hit in hits:
            entry = self.catalog.get(hit.clip_ref)
            if entry:
                pool.append(entry)
        return pool

    def _trend_vector(self, territory: str) -> np.ndarray:
        vector = np.zeros(self.config.signature_width, dtype=np.float32)
        pool = list(self.catalog.values())
        if territory:
            localized = [entry for entry in pool if entry["territory"] in ("", territory)]
            pool = localized or pool
        pool.sort(key=lambda entry: 0.72 * entry["quality"] + 0.28 * entry["freshness"], reverse=True)
        for index, entry in enumerate(pool[:64], start=1):
            weight = (0.72 * entry["quality"] + 0.28 * entry["freshness"]) / math.sqrt(index)
            vector += np.asarray(entry["signature"], dtype=np.float32) * weight
        return _unit(vector)

    def _has_signal(self, vector: np.ndarray) -> bool:
        return float(np.linalg.norm(vector)) > 1e-6

    def _normalize_counter(self, counter: Counter[str]) -> dict[str, float]:
        total = float(sum(counter.values()))
        if total <= 0:
            return {}
        return {key: value / total for key, value in counter.items() if key}

    def _clip_signature(self, clip: dict[str, Any]) -> np.ndarray:
        width = self.config.signature_width
        vector = np.zeros(width, dtype=np.float32)
        tokens: list[tuple[str, float]] = [
            (str(clip.get("makerRef", "")), 0.9),
            (str(clip.get("topic", "")), 1.3),
            (str(clip.get("territory", "")), 0.7),
            (str(clip.get("language", "")), 0.4),
        ]
        for tag in clip.get("hashtags", [])[:8]:
            tokens.append((str(tag), 0.45))
        for token in str(clip.get("headline", "")).lower().split():
            tokens.append((token, 0.15))
        for token, weight in tokens:
            if not token:
                continue
            vector[_stable_slot(token, width)] += float(weight)
        return _unit(vector)

    def _freshness(self, value: Any) -> float:
        age_hours = max((datetime.now(timezone.utc) - _moment(value)).total_seconds() / 3600.0, 0.0)
        return math.exp(-age_hours / max(self.config.freshness_half_life_hours, 1.0))

    def _entry_from_clip(self, clip: dict[str, Any]) -> dict[str, Any]:
        existing = self.catalog.get(clip["clipRef"])
        if existing:
            seeded = dict(existing)
            if "_following_seed" in clip:
                seeded["_following_seed"] = float(clip["_following_seed"])
            return seeded
        quality, uncertainty = clip_quality(clip["Views"], clip["Likes"], clip["Shares"], clip["Comments"], clip["Saves"])
        return {
            "clip_ref": clip["clipRef"],
            "maker_ref": clip["makerRef"],
            "topic": clip["topic"],
            "territory": clip["territory"],
            "language": clip["language"],
            "headline": clip["headline"],
            "created_at": _moment(clip["createdAt"]).isoformat(),
            "quality": quality,
            "uncertainty": uncertainty,
            "freshness": self._freshness(clip["createdAt"]),
            "signature": self._clip_signature(clip).tolist(),
            "payload": self._json_clip(clip),
            "_following_seed": float(clip.get("_following_seed", 0.0)),
        }

    def _json_clip(self, clip: dict[str, Any]) -> dict[str, Any]:
        payload = dict(clip)
        if isinstance(payload.get("createdAt"), datetime):
            payload["createdAt"] = payload["createdAt"].isoformat()
        return payload
