from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from watchtower.config import load_config
from watchtower.engine import FeedEngine, RankedClip
from watchtower.storage import Repository, SessionLedger
from watchtower.vector_store import VectorStore


class CurationRequest(BaseModel):
    viewerRef: str
    batchSize: int = Field(default=20, ge=1, le=100)
    continuationMark: str | None = None
    omitClipRefs: list[str] = Field(default_factory=list)
    journeyRef: str | None = None
    territoryHint: str | None = None


class RelatedRequest(BaseModel):
    anchorClipRef: str
    batchSize: int = Field(default=12, ge=1, le=50)


class EngagementSignal(BaseModel):
    viewerRef: str
    clipRef: str
    signalKind: str
    watchMillis: int | None = Field(default=None, ge=0)
    watchRatio: float | None = Field(default=None, ge=0.0, le=5.0)
    journeyRef: str | None = None
    occurredAt: datetime | None = None


def create_app(seed_bundle: dict[str, list[dict]] | None = None) -> FastAPI:
    config = load_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        repository = Repository(config, seed_bundle=seed_bundle)
        ledger = SessionLedger()
        vector_store = VectorStore(config)
        await repository.connect()
        vector_store.connect()
        engine = FeedEngine(config, vector_store=vector_store)
        engine.load()
        if not engine.catalog or not vector_store.has_entries():
            engine.rebuild(await repository.public_clips())
        app.state.config = config
        app.state.repository = repository
        app.state.ledger = ledger
        app.state.engine = engine
        app.state.vector_store = vector_store
        yield
        vector_store.close()
        await repository.close()

    app = FastAPI(title="the-two-towers", lifespan=lifespan)

    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "the-two-towers", "alive": "/status/alive", "ready": "/status/ready", "rebuild": "/status/rebuild"}

    @app.post("/curation/for-you")
    async def for_you(body: CurationRequest) -> dict:
        return await _lane_response(app, "for_you", body)

    @app.post("/curation/following")
    async def following(body: CurationRequest) -> dict:
        return await _lane_response(app, "following", body)

    @app.post("/curation/trending")
    async def trending(body: CurationRequest) -> dict:
        return await _lane_response(app, "trending", body)

    @app.post("/curation/related")
    async def related(body: RelatedRequest) -> dict:
        trace_ref = f"related-{uuid.uuid4().hex[:12]}"
        result = app.state.engine.related(body.anchorClipRef, body.batchSize)
        return _shape_response(result["items"], result["continuationMark"], result["more"], trace_ref)

    @app.post("/signals/engagement")
    async def signal(body: EngagementSignal) -> dict:
        trace_ref = f"signal-{uuid.uuid4().hex[:12]}"
        payload = {
            "viewer_ref": body.viewerRef,
            "clip_ref": body.clipRef,
            "signal_kind": body.signalKind,
            "watch_millis": body.watchMillis,
            "watch_ratio": body.watchRatio,
            "journey_ref": body.journeyRef,
            "occurred_at": (body.occurredAt or datetime.now(timezone.utc)).isoformat(),
            "trace_ref": trace_ref,
        }
        app.state.ledger.record(body.viewerRef, payload)
        if body.signalKind in {"view", "like", "complete", "save", "skip"}:
            app.state.ledger.remember(body.viewerRef, [body.clipRef])
        await app.state.repository.record_signal(payload)
        return {"status": "ok", "accepted": True, "traceRef": trace_ref}

    @app.get("/status/alive")
    async def alive() -> dict:
        return {"status": "ok", "checkedAt": datetime.now(timezone.utc).isoformat()}

    @app.get("/status/ready")
    async def ready() -> dict:
        return {
            "status": "ok",
            "checkedAt": datetime.now(timezone.utc).isoformat(),
            "repository": await app.state.repository.health(),
            "ledger": app.state.ledger.health(),
            "engine": app.state.engine.status(),
            "vector": app.state.vector_store.health(),
        }

    @app.post("/status/rebuild")
    async def rebuild() -> dict:
        summary = app.state.engine.rebuild(await app.state.repository.public_clips())
        return {"status": "ok", "checkedAt": datetime.now(timezone.utc).isoformat(), "engine": summary}

    return app


async def _lane_response(app: FastAPI, lane: str, body: CurationRequest) -> dict:
    trace_ref = f"{lane}-{uuid.uuid4().hex[:12]}"
    repository: Repository = app.state.repository
    ledger: SessionLedger = app.state.ledger
    engine: FeedEngine = app.state.engine

    viewer_state = await repository.viewer_state(body.viewerRef, territory_hint=body.territoryHint)
    viewer_state["served_refs"] = ledger.served_refs(body.viewerRef)
    omit_refs = list(dict.fromkeys(body.omitClipRefs + viewer_state["served_refs"]))

    if lane == "for_you":
        result = engine.for_you(viewer_state, body.batchSize, omit_refs, body.continuationMark)
    elif lane == "following":
        pool = await repository.following_pool(viewer_state["following_refs"], omit_refs, limit=max(body.batchSize * 5, body.batchSize))
        result = engine.following(viewer_state, pool, body.batchSize, omit_refs, body.continuationMark)
    else:
        result = engine.trending(viewer_state, body.batchSize, omit_refs, body.continuationMark)

    clip_refs = [item.clip_ref for item in result["items"]]
    ledger.remember(body.viewerRef, clip_refs)
    await repository.record_served(body.viewerRef, clip_refs, lane, trace_ref)
    return _shape_response(result["items"], result["continuationMark"], result["more"], trace_ref)


def _shape_response(items: list[RankedClip], continuation_mark: str | None, more: bool, trace_ref: str) -> dict:
    return {
        "status": "ok",
        "items": [
            {
                "clipRef": item.clip_ref,
                "makerRef": item.maker_ref,
                "topic": item.topic,
                "territory": item.territory,
                "clip": item.payload,
                "scoring": item.breakdown,
            }
            for item in items
        ],
        "continuationMark": continuation_mark,
        "more": more,
        "traceRef": trace_ref,
    }


app = create_app()


def run() -> None:
    config = load_config()
    uvicorn.run("watchtower.app:app", host=config.bind_host, port=config.http_port, reload=False)


if __name__ == "__main__":
    run()
