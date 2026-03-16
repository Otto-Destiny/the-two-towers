from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from watchtower.config import Config


def _unit(values: list[float] | np.ndarray) -> np.ndarray:
    vector = np.asarray(values, dtype=np.float32)
    magnitude = float(np.linalg.norm(vector))
    if magnitude == 0.0:
        return vector
    return vector / magnitude


def _chunked(rows: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


@dataclass(slots=True)
class VectorHit:
    clip_ref: str
    score: float
    payload: dict[str, Any]


class VectorStore:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._client: Any = None
        self._mode = "memory"
        self._degraded = False
        self._last_error = ""
        self._records: dict[str, dict[str, Any]] = {}

    def connect(self) -> None:
        self._client = None
        self._mode = "memory"
        self._degraded = False
        self._last_error = ""
        if not self.config.vector_url:
            return
        try:
            from qdrant_client import QdrantClient

            self._client = QdrantClient(url=self.config.vector_url, timeout=5.0)
            self._ensure_collection()
            self._mode = "qdrant"
        except Exception as exc:
            self._client = None
            self._mode = "memory"
            self._degraded = True
            self._last_error = str(exc)

    def close(self) -> None:
        if self._client is not None and hasattr(self._client, "close"):
            self._client.close()

    def health(self) -> dict[str, Any]:
        healthy = self._mode == "qdrant" or not self.config.vector_url
        return {
            "mode": self._mode,
            "healthy": healthy and not self._degraded,
            "degraded": self._degraded,
            "collection": self.config.vector_collection,
            "dimension": self.config.vector_dimension,
            "indexed_clips": self.count(),
            "endpoint": self.config.vector_url,
            "last_error": self._last_error,
        }

    def count(self) -> int:
        if self._mode == "qdrant" and self._client is not None:
            try:
                result = self._client.count(collection_name=self.config.vector_collection, exact=True)
                return int(getattr(result, "count", 0))
            except Exception:
                return 0
        return len(self._records)

    def has_entries(self) -> bool:
        return self.count() > 0

    def replace(self, entries: Iterable[dict[str, Any]]) -> dict[str, Any]:
        rows = [dict(entry) for entry in entries]
        if self._mode == "qdrant" and self._client is not None:
            try:
                self._recreate_collection()
                self._replace_remote(rows)
            except Exception as exc:
                self._degraded = True
                self._last_error = str(exc)
                self._mode = "memory"
                self._client = None
                self._replace_memory(rows)
        else:
            self._replace_memory(rows)
        return self.health()

    def search(
        self,
        query_vector: list[float] | np.ndarray,
        limit: int,
        territory: str = "",
        omit_refs: list[str] | None = None,
    ) -> list[VectorHit]:
        if limit <= 0:
            return []
        fitted = self._fit_vector(query_vector)
        if float(np.linalg.norm(fitted)) == 0.0:
            return []
        omit = set(omit_refs or [])
        if self._mode == "qdrant" and self._client is not None:
            try:
                results = self._search_remote(fitted, limit, territory, omit)
                if results:
                    return results[:limit]
            except Exception as exc:
                self._degraded = True
                self._last_error = str(exc)
                self._mode = "memory"
                self._client = None
        return self._search_memory(fitted, limit, territory, omit)

    def related(
        self,
        anchor_vector: list[float] | np.ndarray,
        limit: int,
        territory: str = "",
        omit_refs: list[str] | None = None,
    ) -> list[VectorHit]:
        return self.search(anchor_vector, limit, territory=territory, omit_refs=omit_refs)

    def _fit_vector(self, values: list[float] | np.ndarray) -> np.ndarray:
        vector = np.asarray(values, dtype=np.float32)
        target = self.config.vector_dimension
        if vector.size < target:
            vector = np.pad(vector, (0, target - vector.size))
        elif vector.size > target:
            vector = vector[:target]
        return _unit(vector)

    def _replace_memory(self, rows: list[dict[str, Any]]) -> None:
        self._records = {
            row["clip_ref"]: {"vector": self._fit_vector(row["signature"]), "payload": self._payload(row)}
            for row in rows
        }

    def _search_memory(
        self,
        query_vector: np.ndarray,
        limit: int,
        territory: str,
        omit_refs: set[str],
    ) -> list[VectorHit]:
        hits: list[VectorHit] = []
        for clip_ref, record in self._records.items():
            if clip_ref in omit_refs:
                continue
            payload = record["payload"]
            clip_territory = str(payload.get("territory", "") or "")
            if territory and clip_territory not in {"", territory}:
                continue
            score = float(np.dot(query_vector, record["vector"]))
            hits.append(VectorHit(clip_ref=clip_ref, score=score, payload=dict(payload)))
        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[:limit]

    def _search_remote(
        self,
        query_vector: np.ndarray,
        limit: int,
        territory: str,
        omit_refs: set[str],
    ) -> list[VectorHit]:
        query_filter = self._remote_filter(territory, omit_refs)
        points = None
        if hasattr(self._client, "query_points"):
            response = self._client.query_points(
                collection_name=self.config.vector_collection,
                query=query_vector.tolist(),
                limit=max(limit, 1),
                with_payload=True,
                with_vectors=False,
                query_filter=query_filter,
            )
            points = getattr(response, "points", response)
        else:
            points = self._client.search(
                collection_name=self.config.vector_collection,
                query_vector=query_vector.tolist(),
                limit=max(limit, 1),
                with_payload=True,
                query_filter=query_filter,
            )
        hits: list[VectorHit] = []
        for point in points or []:
            payload = dict(getattr(point, "payload", {}) or {})
            clip_ref = str(getattr(point, "id", "") or payload.get("clip_ref", ""))
            if not clip_ref or clip_ref in omit_refs:
                continue
            clip_territory = str(payload.get("territory", "") or "")
            if territory and clip_territory not in {"", territory}:
                continue
            hits.append(
                VectorHit(
                    clip_ref=clip_ref,
                    score=float(getattr(point, "score", 0.0)),
                    payload=payload,
                )
            )
        return hits[:limit]

    def _payload(self, entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "clip_ref": entry["clip_ref"],
            "maker_ref": entry.get("maker_ref", ""),
            "topic": entry.get("topic", ""),
            "territory": entry.get("territory", ""),
            "quality": float(entry.get("quality", 0.0)),
            "freshness": float(entry.get("freshness", 0.0)),
        }

    def _ensure_collection(self) -> None:
        from qdrant_client.http import models

        exists = False
        if hasattr(self._client, "collection_exists"):
            exists = bool(self._client.collection_exists(collection_name=self.config.vector_collection))
        else:
            try:
                self._client.get_collection(collection_name=self.config.vector_collection)
                exists = True
            except Exception:
                exists = False
        if exists:
            return
        self._client.create_collection(
            collection_name=self.config.vector_collection,
            vectors_config=models.VectorParams(
                size=self.config.vector_dimension,
                distance=models.Distance.COSINE,
            ),
        )

    def _recreate_collection(self) -> None:
        from qdrant_client.http import models

        try:
            self._client.delete_collection(collection_name=self.config.vector_collection)
        except Exception:
            pass
        self._client.create_collection(
            collection_name=self.config.vector_collection,
            vectors_config=models.VectorParams(
                size=self.config.vector_dimension,
                distance=models.Distance.COSINE,
            ),
        )

    def _replace_remote(self, rows: list[dict[str, Any]]) -> None:
        from qdrant_client.http import models

        points = [
            models.PointStruct(
                id=row["clip_ref"],
                vector=self._fit_vector(row["signature"]).tolist(),
                payload=self._payload(row),
            )
            for row in rows
        ]
        for batch in _chunked(points, 256):
            self._client.upsert(
                collection_name=self.config.vector_collection,
                points=batch,
                wait=True,
            )

    def _remote_filter(self, territory: str, omit_refs: set[str]) -> Any:
        from qdrant_client.http import models

        kwargs: dict[str, Any] = {}
        if territory:
            kwargs["should"] = [
                models.FieldCondition(key="territory", match=models.MatchValue(value=territory)),
                models.FieldCondition(key="territory", match=models.MatchValue(value="")),
            ]
        if omit_refs:
            kwargs["must_not"] = [models.HasIdCondition(has_id=list(omit_refs))]
        if not kwargs:
            return None
        return models.Filter(**kwargs)
