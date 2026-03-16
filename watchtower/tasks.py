from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from watchtower.config import load_config
from watchtower.engine import FeedEngine
from watchtower.storage import Repository
from watchtower.vector_store import VectorStore


async def rebuild_snapshot(seed_bundle: dict[str, list[dict]] | None = None, limit: int | None = None) -> dict[str, Any]:
    config = load_config()
    repository = Repository(config, seed_bundle=seed_bundle)
    vector_store = VectorStore(config)
    await repository.connect()
    vector_store.connect()
    engine = FeedEngine(config, vector_store=vector_store)
    clips = await repository.public_clips(limit=limit)
    summary = engine.rebuild(clips)
    vector_store.close()
    await repository.close()
    return summary


async def write_affinities(seed_bundle: dict[str, list[dict]] | None = None, limit: int | None = None) -> dict[str, Any]:
    config = load_config()
    repository = Repository(config, seed_bundle=seed_bundle)
    await repository.connect()
    engine = FeedEngine(config)
    engine.load()
    if not engine.catalog:
        engine.rebuild(await repository.public_clips())
    rows = []
    viewer_refs = await repository.viewer_refs(limit=limit)
    for viewer_ref in viewer_refs:
        rows.append(engine.viewer_summary(await repository.viewer_state(viewer_ref)))
    config.affinity_path.parent.mkdir(parents=True, exist_ok=True)
    config.affinity_path.write_text(json.dumps(rows, separators=(",", ":")), encoding="utf-8")
    await repository.close()
    return {"viewer_count": len(rows), "path": str(config.affinity_path)}


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    rebuild_parser = subparsers.add_parser("rebuild")
    rebuild_parser.add_argument("--limit", type=int, default=None)
    affinity_parser = subparsers.add_parser("affinities")
    affinity_parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    if args.command == "rebuild":
        print(asyncio.run(rebuild_snapshot(limit=args.limit)))
        return
    print(asyncio.run(write_affinities(limit=args.limit)))


if __name__ == "__main__":
    main()
