from __future__ import annotations

from watchtower.config import load_config
from watchtower.engine import FeedEngine, clip_quality, decode_cursor
from watchtower.vector_store import VectorStore


def test_clip_quality_rewards_stronger_positive_feedback():
    weak = clip_quality(views=1000, likes=10, shares=1, comments=1, saves=0)
    strong = clip_quality(views=1000, likes=100, shares=20, comments=10, saves=5)
    assert strong[0] > weak[0]
    assert strong[1] > 0


def test_engine_related_prefers_signature_overlap(seed_bundle):
    vector_store = VectorStore(load_config())
    vector_store.connect()
    engine = FeedEngine(load_config(), vector_store=vector_store)
    clips = []
    for row in seed_bundle["videos"]:
        clips.append(
            {
                **row,
                "clipRef": row["_id"],
                "makerRef": row["UserID"],
                "topic": row["Category"],
                "territory": row["Country"],
                "language": row["Language"],
                "headline": row["Description"],
                "hashtags": [],
                "createdAt": row["createdAt"],
            }
        )
    engine.rebuild(clips)
    result = engine.related("clip-1", 2)
    assert result["items"]
    assert result["items"][0].clip_ref == "clip-3"
    assert vector_store.health()["indexed_clips"] == 3


def test_clip_signatures_are_stable(seed_bundle):
    config = load_config()
    first = FeedEngine(config)
    second = FeedEngine(config)
    clip = {
        **seed_bundle["videos"][0],
        "clipRef": seed_bundle["videos"][0]["_id"],
        "makerRef": seed_bundle["videos"][0]["UserID"],
        "topic": seed_bundle["videos"][0]["Category"],
        "territory": seed_bundle["videos"][0]["Country"],
        "language": seed_bundle["videos"][0]["Language"],
        "headline": seed_bundle["videos"][0]["Description"],
        "hashtags": [],
        "createdAt": seed_bundle["videos"][0]["createdAt"],
    }
    assert first._clip_signature(clip).tolist() == second._clip_signature(clip).tolist()


def test_cursor_round_trip():
    cursor = decode_cursor(None, "for-you")
    assert cursor["page"] == 0
