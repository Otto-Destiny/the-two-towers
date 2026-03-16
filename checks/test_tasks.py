from __future__ import annotations

import asyncio
import json
from pathlib import Path

from watchtower.tasks import rebuild_snapshot, write_affinities


def test_tasks_write_snapshot_and_affinities(seed_bundle):
    rebuild = asyncio.run(rebuild_snapshot(seed_bundle=seed_bundle))
    assert rebuild["clip_count"] == 3
    assert rebuild["vector"]["indexed_clips"] == 3

    affinity = asyncio.run(write_affinities(seed_bundle=seed_bundle))
    assert affinity["viewer_count"] >= 1
    payload = json.loads(Path(affinity["path"]).read_text(encoding="utf-8"))
    assert payload
    assert payload[0]["viewer_ref"] == "viewer-1"
