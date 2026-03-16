from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from watchtower.config import Config


class SessionLedger:
    def __init__(self) -> None:
        self._served: dict[str, list[str]] = defaultdict(list)
        self._signals: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def served_refs(self, viewer_ref: str) -> list[str]:
        return list(self._served.get(viewer_ref, []))

    def remember(self, viewer_ref: str, clip_refs: list[str]) -> None:
        memory = self._served[viewer_ref]
        memory.extend(list(clip_refs))
        self._served[viewer_ref] = memory[-5000:]

    def record(self, viewer_ref: str, payload: dict[str, Any]) -> None:
        rows = self._signals[viewer_ref]
        rows.append(payload)
        self._signals[viewer_ref] = rows[-500:]

    def health(self) -> dict[str, Any]:
        return {"mode": "memory", "served_viewers": len(self._served), "signal_viewers": len(self._signals)}


class Repository:
    def __init__(self, config: Config, seed_bundle: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.config = config
        self.seed_bundle = seed_bundle or {}
        self._client = None
        self._db = None
        self._mode = "memory"
        self._memory = {name: [dict(row) for row in rows] for name, rows in self.seed_bundle.items()}

    async def connect(self) -> None:
        if self.seed_bundle or not self.config.mongo_dsn:
            self._mode = "memory"
            return
        try:
            from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        except ImportError:
            self._mode = "memory"
            return
        self._client = AsyncIOMotorClient(self.config.mongo_dsn, serverSelectionTimeoutMS=5000)
        self._db = self._client[self.config.mongo_database]
        self._mode = "mongo"

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()

    async def health(self) -> dict[str, Any]:
        if self._mode == "mongo" and self._db is not None:
            await self._db.command("ping")
            return {"mode": "mongo", "healthy": True}
        return {"mode": "memory", "healthy": True}

    def _rows(self, collection_name: str) -> list[dict[str, Any]]:
        return self._memory.setdefault(collection_name, [])

    def _moment(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str) and value:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return datetime.now(timezone.utc)

    def _is_public(self, row: dict[str, Any]) -> bool:
        visibility = str(row.get(self.config.fields.visibility, "public") or "public").lower()
        return visibility in {"public", "publish", "published", ""}

    def _clip_ref(self, row: dict[str, Any]) -> str:
        return str(row.get(self.config.fields.clip_ref, row.get("_id", "")))

    def _viewer_ref(self, row: dict[str, Any]) -> str:
        return str(row.get(self.config.fields.viewer_ref, row.get("_id", "")))

    def _normalized_clip(self, row: dict[str, Any]) -> dict[str, Any]:
        clip_ref = self._clip_ref(row)
        maker_ref = str(row.get(self.config.fields.maker_ref, ""))
        return {
            **row,
            "_id": clip_ref,
            "clipRef": clip_ref,
            "makerRef": maker_ref,
            "topic": str(row.get(self.config.fields.topic, "") or ""),
            "territory": str(row.get(self.config.fields.territory, "") or ""),
            "language": str(row.get(self.config.fields.language, "") or ""),
            "headline": str(row.get(self.config.fields.headline, "") or ""),
            "hashtags": list(row.get(self.config.fields.hashtags, []) or []),
            "createdAt": self._moment(row.get(self.config.fields.created_at)),
            "Views": float(row.get(self.config.fields.views, 0) or 0),
            "Likes": float(row.get(self.config.fields.likes, 0) or 0),
            "Shares": float(row.get(self.config.fields.shares, 0) or 0),
            "Comments": float(row.get(self.config.fields.comments, 0) or 0),
            "Saves": float(row.get(self.config.fields.saves, 0) or 0),
        }

    async def public_clips(self, limit: int | None = None) -> list[dict[str, Any]]:
        if self._mode == "mongo" and self._db is not None:
            cursor = self._db[self.config.collections.clips].find({self.config.fields.visibility: {"$ne": "private"}})
            if limit:
                cursor = cursor.limit(limit)
            return [self._normalized_clip(row) async for row in cursor]
        rows = [self._normalized_clip(row) for row in self._rows(self.config.collections.clips) if self._is_public(row)]
        return rows[:limit] if limit else rows

    async def clips_by_refs(self, clip_refs: list[str]) -> dict[str, dict[str, Any]]:
        clip_set = set(clip_refs)
        rows = await self.public_clips()
        return {row["clipRef"]: row for row in rows if row["clipRef"] in clip_set}

    async def viewer_state(self, viewer_ref: str, territory_hint: str | None = None) -> dict[str, Any]:
        if self._mode == "mongo" and self._db is not None:
            viewers = self._db[self.config.collections.viewers]
            profile = await viewers.find_one({self.config.fields.viewer_ref: viewer_ref}) or {}
            watches = [
                {
                    "clip_ref": str(row.get(self.config.fields.watch_clip, "")),
                    "watch_ratio": self._watch_ratio(row),
                }
                async for row in self._db[self.config.collections.watches].find({self.config.fields.watch_actor: viewer_ref}).limit(120)
            ]
            likes = [
                str(row.get(self.config.fields.like_clip, ""))
                async for row in self._db[self.config.collections.likes].find({self.config.fields.like_actor: viewer_ref}).limit(120)
            ]
            follows = [
                str(row.get(self.config.fields.follow_subject, ""))
                async for row in self._db[self.config.collections.follows].find(
                    {self.config.fields.follow_actor: viewer_ref, self.config.fields.follow_state: "follow"}
                ).limit(240)
            ]
        else:
            profile = {}
            for row in self._rows(self.config.collections.viewers):
                if self._viewer_ref(row) == viewer_ref:
                    profile = dict(row)
                    break
            watches = [
                {"clip_ref": str(row.get(self.config.fields.watch_clip, "")), "watch_ratio": self._watch_ratio(row)}
                for row in self._rows(self.config.collections.watches)
                if str(row.get(self.config.fields.watch_actor, "")) == viewer_ref
            ][-120:]
            likes = [
                str(row.get(self.config.fields.like_clip, ""))
                for row in self._rows(self.config.collections.likes)
                if str(row.get(self.config.fields.like_actor, "")) == viewer_ref
            ][-120:]
            follows = [
                str(row.get(self.config.fields.follow_subject, ""))
                for row in self._rows(self.config.collections.follows)
                if str(row.get(self.config.fields.follow_actor, "")) == viewer_ref and str(row.get(self.config.fields.follow_state, "follow")).lower() == "follow"
            ][-240:]
        return {
            "viewer_ref": viewer_ref,
            "territory": territory_hint or str(profile.get(self.config.fields.territory, "") or ""),
            "language": str(profile.get(self.config.fields.language, "") or ""),
            "watch_events": watches,
            "liked_refs": likes,
            "following_refs": follows,
            "profile": profile,
        }

    async def following_pool(self, following_refs: list[str], omit_refs: list[str], limit: int) -> list[dict[str, Any]]:
        if not following_refs:
            return []
        rows = await self.public_clips()
        omit = set(omit_refs)
        following_set = set(following_refs)
        primary = []
        for row in rows:
            if row["clipRef"] in omit:
                continue
            if row["makerRef"] in following_set:
                seeded = dict(row)
                seeded["_following_seed"] = 1.0
                primary.append(seeded)
        liked_refs = {
            str(row.get(self.config.fields.like_clip, ""))
            for row in self._rows(self.config.collections.likes)
            if str(row.get(self.config.fields.like_actor, "")) in following_set
        }
        lookup = {row["clipRef"]: row for row in rows}
        for clip_ref in liked_refs:
            if clip_ref in omit or clip_ref not in lookup:
                continue
            seeded = dict(lookup[clip_ref])
            seeded["_following_seed"] = 0.6
            primary.append(seeded)
        primary.sort(key=lambda row: row["createdAt"], reverse=True)
        deduped: dict[str, dict[str, Any]] = {}
        for row in primary:
            deduped.setdefault(row["clipRef"], row)
        return list(deduped.values())[:limit]

    async def viewer_refs(self, limit: int | None = None) -> list[str]:
        if self._mode == "mongo" and self._db is not None:
            cursor = self._db[self.config.collections.viewers].find({}, {self.config.fields.viewer_ref: 1})
            if limit:
                cursor = cursor.limit(limit)
            return [str(row.get(self.config.fields.viewer_ref, "")) async for row in cursor if row.get(self.config.fields.viewer_ref)]
        refs = [self._viewer_ref(row) for row in self._rows(self.config.collections.viewers)]
        refs = [ref for ref in refs if ref]
        return refs[:limit] if limit else refs

    async def record_served(self, viewer_ref: str, clip_refs: list[str], lane: str, trace_ref: str) -> None:
        payload = {
            "viewer_ref": viewer_ref,
            "clip_refs": list(clip_refs),
            "lane": lane,
            "trace_ref": trace_ref,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if self._mode == "mongo" and self._db is not None:
            await self._db[self.config.collections.served_logs].insert_one(payload)
            return
        self._rows(self.config.collections.served_logs).append(payload)

    async def record_signal(self, payload: dict[str, Any]) -> None:
        if self._mode == "mongo" and self._db is not None:
            await self._db[self.config.collections.signal_logs].insert_one(payload)
            return
        self._rows(self.config.collections.signal_logs).append(dict(payload))

    def _watch_ratio(self, row: dict[str, Any]) -> float:
        watched = float(row.get(self.config.fields.watch_millis, 0) or 0)
        total = float(row.get(self.config.fields.clip_millis, 0) or 0)
        if watched <= 0 or total <= 0:
            return 0.0
        ratio = watched / total
        return max(0.0, min(ratio, 5.0))

