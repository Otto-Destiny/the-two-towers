from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


@dataclass(slots=True)
class Collections:
    viewers: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_VIEWERS", "userprofiles"))
    clips: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_CLIPS", "videos"))
    watches: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_WATCHES", "watchvideointeractions"))
    likes: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_LIKES", "likevideointeractions"))
    follows: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_FOLLOWS", "userfollowers"))
    served_logs: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_SERVED_LOGS", "ttt_served_logs"))
    signal_logs: str = field(default_factory=lambda: _env("TTT_MONGO_COLLECTION_SIGNAL_LOGS", "ttt_signal_logs"))


@dataclass(slots=True)
class Fields:
    viewer_ref: str = field(default_factory=lambda: _env("TTT_FIELD_VIEWER_REF", "UserID"))
    clip_ref: str = field(default_factory=lambda: _env("TTT_FIELD_CLIP_REF", "_id"))
    maker_ref: str = field(default_factory=lambda: _env("TTT_FIELD_MAKER_REF", "UserID"))
    topic: str = field(default_factory=lambda: _env("TTT_FIELD_TOPIC", "Category"))
    territory: str = field(default_factory=lambda: _env("TTT_FIELD_TERRITORY", "Country"))
    language: str = field(default_factory=lambda: _env("TTT_FIELD_LANGUAGE", "Language"))
    headline: str = field(default_factory=lambda: _env("TTT_FIELD_HEADLINE", "Description"))
    hashtags: str = field(default_factory=lambda: _env("TTT_FIELD_HASHTAGS", "HashTags"))
    created_at: str = field(default_factory=lambda: _env("TTT_FIELD_CREATED_AT", "createdAt"))
    views: str = field(default_factory=lambda: _env("TTT_FIELD_VIEWS", "Views"))
    likes: str = field(default_factory=lambda: _env("TTT_FIELD_LIKES", "Likes"))
    shares: str = field(default_factory=lambda: _env("TTT_FIELD_SHARES", "Shares"))
    comments: str = field(default_factory=lambda: _env("TTT_FIELD_COMMENTS", "Comments"))
    saves: str = field(default_factory=lambda: _env("TTT_FIELD_SAVES", "Saves"))
    visibility: str = field(default_factory=lambda: _env("TTT_FIELD_VISIBILITY", "Visibility"))
    watch_actor: str = field(default_factory=lambda: _env("TTT_FIELD_WATCH_ACTOR", "UserID"))
    watch_clip: str = field(default_factory=lambda: _env("TTT_FIELD_WATCH_CLIP", "ItemID"))
    watch_millis: str = field(default_factory=lambda: _env("TTT_FIELD_WATCH_MILLIS", "WatchedDuration"))
    clip_millis: str = field(default_factory=lambda: _env("TTT_FIELD_CLIP_MILLIS", "VideoDuration"))
    like_actor: str = field(default_factory=lambda: _env("TTT_FIELD_LIKE_ACTOR", "UserID"))
    like_clip: str = field(default_factory=lambda: _env("TTT_FIELD_LIKE_CLIP", "ItemID"))
    follow_actor: str = field(default_factory=lambda: _env("TTT_FIELD_FOLLOW_ACTOR", "FollowerID"))
    follow_subject: str = field(default_factory=lambda: _env("TTT_FIELD_FOLLOW_SUBJECT", "UserID"))
    follow_state: str = field(default_factory=lambda: _env("TTT_FIELD_FOLLOW_STATE", "Status"))


@dataclass(slots=True)
class Config:
    bind_host: str = field(default_factory=lambda: _env("TTT_BIND_HOST", "0.0.0.0"))
    http_port: int = field(default_factory=lambda: _int("TTT_HTTP_PORT", 8080))
    mongo_dsn: str = field(default_factory=lambda: _env("TTT_MONGO_DSN"))
    mongo_database: str = field(default_factory=lambda: _env("TTT_MONGO_DATABASE", "sample_media"))
    snapshot_path: Path = field(default_factory=lambda: Path(_env("TTT_SNAPSHOT_PATH", "./snapshot/catalog.json")))
    affinity_path: Path = field(default_factory=lambda: Path(_env("TTT_AFFINITY_PATH", "./snapshot/affinities.json")))
    signature_width: int = field(default_factory=lambda: _int("TTT_SIGNATURE_WIDTH", 28))
    vector_url: str = field(default_factory=lambda: _env("TTT_VECTOR_URL"))
    vector_collection: str = field(default_factory=lambda: _env("TTT_VECTOR_COLLECTION", "ttt-clips"))
    vector_dimension: int = field(default_factory=lambda: _int("TTT_VECTOR_DIMENSION", _int("TTT_SIGNATURE_WIDTH", 28)))
    max_candidates: int = field(default_factory=lambda: _int("TTT_MAX_CANDIDATES", 1200))
    max_cursor_seen: int = field(default_factory=lambda: _int("TTT_MAX_CURSOR_SEEN", 500))
    freshness_half_life_hours: float = field(default_factory=lambda: _float("TTT_FRESHNESS_HALF_LIFE_HOURS", 18.0))
    exploration_scale: float = field(default_factory=lambda: _float("TTT_EXPLORATION_SCALE", 0.12))
    diversity_scale: float = field(default_factory=lambda: _float("TTT_DIVERSITY_SCALE", 0.22))
    collections: Collections = field(default_factory=Collections)
    fields: Fields = field(default_factory=Fields)

    def __post_init__(self) -> None:
        if self.vector_dimension <= 0:
            self.vector_dimension = max(self.signature_width, 1)
        if self.signature_width <= 0:
            self.signature_width = max(self.vector_dimension, 1)


@lru_cache(maxsize=1)
def load_config() -> Config:
    config = Config()
    config.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    config.affinity_path.parent.mkdir(parents=True, exist_ok=True)
    return config


def reset_config() -> Config:
    load_config.cache_clear()
    return load_config()
