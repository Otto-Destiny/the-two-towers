# Architecture

This service keeps the online path small while still covering the full recommendation loop: data access, candidate retrieval, ranking, feedback capture, and index refresh.

The design centers on three choices:

1. The request path is single-process. The API, retrieval logic, and ranker live in one runtime.
2. Retrieval is vector-first. Qdrant holds clip signatures for ANN lookup, while the local snapshot keeps the metadata and calibrated priors needed for ranking.
3. Ranking is deterministic. Instead of a learned ranker, the system blends viewer affinity, engagement priors, freshness, and diversity penalties in a transparent scoring pass.

The runtime has four parts:

- `watchtower.storage.Repository` reads clips, viewer history, follows, and writes simple logs.
- `watchtower.storage.SessionLedger` keeps short-lived served history and captured feedback in memory.
- `watchtower.vector_store.VectorStore` owns Qdrant integration and falls back to an in-process index when a vector endpoint is not configured.
- `watchtower.engine.FeedEngine` builds signatures, derives viewer taste profiles, pulls ANN candidates, and applies the final ranking pass.
- `watchtower.app` exposes the HTTP surface and the rebuild endpoint.

`watchtower.tasks` refreshes the snapshot, rebuilds the vector collection, and writes viewer affinity summaries for inspection.
