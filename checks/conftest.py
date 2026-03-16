from __future__ import annotations

import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _fresh_settings_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("TTT_SNAPSHOT_PATH", str(tmp_path / "snapshot" / "catalog.json"))
    monkeypatch.setenv("TTT_AFFINITY_PATH", str(tmp_path / "snapshot" / "affinities.json"))
    monkeypatch.setenv("TTT_MONGO_DSN", "")
    monkeypatch.setenv("TTT_SIGNATURE_WIDTH", "20")
    monkeypatch.setenv("TTT_VECTOR_URL", "")
    monkeypatch.setenv("TTT_VECTOR_COLLECTION", "ttt-test-clips")
    monkeypatch.setenv("TTT_VECTOR_DIMENSION", "20")
    from watchtower.config import reset_config

    reset_config()
    yield
    reset_config()


@pytest.fixture
def seed_bundle():
    return {
        "userprofiles": [
            {"UserID": "viewer-1", "Country": "NG", "Language": "en"},
            {"UserID": "maker-1", "Country": "NG", "Language": "en"},
            {"UserID": "maker-2", "Country": "NG", "Language": "en"},
        ],
        "videos": [
            {
                "_id": "clip-1",
                "UserID": "maker-1",
                "Category": "Comedy",
                "Country": "NG",
                "Language": "en",
                "Description": "first clip",
                "createdAt": "2026-03-15T10:00:00+00:00",
                "Views": 1000,
                "Likes": 120,
                "Shares": 15,
                "Comments": 12,
                "Saves": 5,
                "Visibility": "public",
            },
            {
                "_id": "clip-2",
                "UserID": "maker-2",
                "Category": "Sports",
                "Country": "NG",
                "Language": "en",
                "Description": "second clip",
                "createdAt": "2026-03-15T08:00:00+00:00",
                "Views": 900,
                "Likes": 110,
                "Shares": 22,
                "Comments": 8,
                "Saves": 3,
                "Visibility": "public",
            },
            {
                "_id": "clip-3",
                "UserID": "maker-1",
                "Category": "Comedy",
                "Country": "NG",
                "Language": "en",
                "Description": "third comedy clip",
                "createdAt": "2026-03-15T09:30:00+00:00",
                "Views": 500,
                "Likes": 75,
                "Shares": 4,
                "Comments": 4,
                "Saves": 1,
                "Visibility": "public",
            },
        ],
        "watchvideointeractions": [
            {"UserID": "viewer-1", "ItemID": "clip-1", "WatchedDuration": 9000, "VideoDuration": 10000},
            {"UserID": "viewer-1", "ItemID": "clip-2", "WatchedDuration": 4000, "VideoDuration": 10000},
        ],
        "likevideointeractions": [
            {"UserID": "viewer-1", "ItemID": "clip-1"},
            {"UserID": "maker-1", "ItemID": "clip-2"},
        ],
        "userfollowers": [
            {"FollowerID": "viewer-1", "UserID": "maker-1", "Status": "follow"},
        ],
    }
