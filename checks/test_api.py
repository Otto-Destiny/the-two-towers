from __future__ import annotations

from fastapi.testclient import TestClient

from watchtower.app import create_app


def test_endpoints(seed_bundle):
    with TestClient(create_app(seed_bundle=seed_bundle)) as client:
        for_you = client.post("/curation/for-you", json={"viewerRef": "viewer-1", "batchSize": 2})
        assert for_you.status_code == 200
        for_you_body = for_you.json()
        assert for_you_body["status"] == "ok"
        assert for_you_body["continuationMark"]
        assert for_you_body["items"]

        following = client.post("/curation/following", json={"viewerRef": "viewer-1", "batchSize": 2})
        assert following.status_code == 200
        following_body = following.json()
        assert following_body["items"]
        assert any(item["makerRef"] == "maker-1" for item in following_body["items"])

        trending = client.post("/curation/trending", json={"viewerRef": "viewer-1", "batchSize": 2})
        assert trending.status_code == 200
        assert trending.json()["items"]

        related = client.post("/curation/related", json={"anchorClipRef": "clip-1", "batchSize": 2})
        assert related.status_code == 200
        assert related.json()["items"][0]["clipRef"] == "clip-3"

        signal = client.post(
            "/signals/engagement",
            json={"viewerRef": "viewer-1", "clipRef": "clip-2", "signalKind": "view", "watchMillis": 3000, "watchRatio": 0.3},
        )
        assert signal.status_code == 200
        assert signal.json()["accepted"] is True

        ready = client.get("/status/ready")
        assert ready.status_code == 200
        assert ready.json()["engine"]["clip_count"] == 3
        assert ready.json()["vector"]["indexed_clips"] == 3

        rebuild = client.post("/status/rebuild")
        assert rebuild.status_code == 200
        assert rebuild.json()["engine"]["clip_count"] == 3
        assert rebuild.json()["engine"]["vector"]["indexed_clips"] == 3
