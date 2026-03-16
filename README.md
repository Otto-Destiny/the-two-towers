<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.11" />
  <img src="https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Qdrant-Vector%20DB-DC244C?style=for-the-badge&logo=qdrant&logoColor=white" alt="Qdrant" />
  <img src="https://img.shields.io/badge/MongoDB-Data-47A248?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Docker-Containers-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Kubernetes-Deploy-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white" alt="Kubernetes" />
  <img src="https://img.shields.io/badge/Pytest-Checks-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest" />
  <img src="https://img.shields.io/badge/License-MIT-black?style=for-the-badge" alt="MIT License" />
</p>

# the-two-towers

`the-two-towers` is a vector-backed recommendation service for short-form video feeds. It exposes a small HTTP API for feed generation and engagement capture, uses Qdrant for ANN retrieval, and applies a transparent ranking pass that blends viewer affinity, engagement priors, freshness, and diversity control. The project is inspired by the retrieval and ranking ideas behind TikTok-style recommendation systems, and by *The Two Towers* from *The Lord of the Rings* movie, the towers of Sauron and Saruman; hence the name. This is my own small implementation of the well known industry two-tower recommendation engine.

The project is intentionally compact: one FastAPI service, one retrieval adapter, one ranking engine, one data access layer, and a small set of deployment artifacts that make the full path easy to inspect.

## What It Does

- Serves `for-you`, `following`, `trending`, and `related` recommendation lanes.
- Stores clip signatures in a vector collection and queries that index for candidate retrieval.
- Builds viewer taste profiles from watch history, likes, and follows.
- Reranks ANN candidates with deterministic scoring so the ranking path is easy to reason about.
- Captures served logs and engagement events.
- Rebuilds the local snapshot and vector collection through a maintenance entrypoint.
- Ships with Docker, Compose, Kubernetes manifests, CI, tests, and sample run artifacts.

## Architecture

The online path is intentionally direct:

```text
client
  -> FastAPI app
      -> Repository
           reads viewers, clips, watches, likes, follows
           writes served logs and signal logs
      -> FeedEngine
           builds viewer profile
           queries VectorStore for ANN candidates
           scores and reranks candidates
      -> response
```

There are four main runtime components:

1. `watchtower.app`
   Owns the HTTP interface, startup lifecycle, and request orchestration.
2. `watchtower.storage.Repository`
   Reads source records from MongoDB when configured, or seeded in-memory data during tests.
3. `watchtower.vector_store.VectorStore`
   Owns Qdrant integration and an in-process fallback index for test and bootstrap scenarios.
4. `watchtower.engine.FeedEngine`
   Builds clip signatures, derives viewer profiles, retrieves candidates, and performs final ranking.

The maintenance entrypoint in `watchtower.tasks` refreshes the snapshot catalog, rebuilds the vector collection, and writes viewer affinity summaries for inspection.

## Request Flow

### For You

1. The API loads the viewer profile, recent watch events, likes, and follows.
2. The engine converts those signals into a weighted viewer vector and normalized preference weights.
3. The vector store queries Qdrant for nearest-neighbor clip candidates.
4. The engine scores the candidate set using:
   - vector affinity
   - topic match
   - maker affinity
   - territory match
   - clip quality prior
   - freshness
   - exploration allowance for uncertain clips
5. A diversity-aware rerank pass reduces near-duplicates and over-concentration from one maker or topic.
6. The API returns ranked items plus a continuation mark and records the served set.

### Following

1. The repository builds a pool from clips posted by followed makers.
2. It expands that pool with clips liked by followed accounts.
3. The same final scoring pass reranks the resulting pool with stronger maker weight.

### Trending

1. The engine builds a trend vector from the strongest recent clips in the active territory.
2. That vector retrieves ANN candidates from the vector store.
3. The final scorer emphasizes engagement quality and freshness over personal affinity.

### Related

1. The engine reads the anchor clip signature.
2. The vector store retrieves nearest neighbors for that signature.
3. The engine applies a small final pass that mixes similarity, quality, and freshness.

## Retrieval and Ranking Model

### Clip Signatures

Each clip is converted into a dense signature using stable hashed features from:

- maker
- topic
- territory
- language
- hashtags
- headline tokens

The signature builder is deterministic. The same clip data maps to the same vector across runs.

### Viewer Profiles

Viewer profiles are derived from:

- watch events, weighted by watch ratio and recency
- likes, with higher weight than partial watches
- follows, which strengthen maker affinity even without direct content interaction

The result is a normalized viewer vector plus topic, maker, and territory preference maps.

### Final Score

The final scorer blends these signals:

- `affinity`: cosine similarity between the viewer vector and the clip signature
- `topic`: normalized viewer preference for the clip topic
- `maker`: normalized affinity for the clip maker, plus a follow seed where relevant
- `territory`: viewer alignment with the clip territory
- `quality`: a Bayesian-style prior over engagement counts
- `freshness`: exponential decay based on clip age
- `exploration`: a small bonus for uncertain clips that the viewer has not strongly saturated on

After the blend step, a rerank pass subtracts:

- signature redundancy against already-selected clips
- repeated maker concentration
- repeated topic concentration

The returned payload exposes the score breakdown so the ranking path is inspectable.

## API Surface

### Feed Endpoints

- `POST /curation/for-you`
- `POST /curation/following`
- `POST /curation/trending`
- `POST /curation/related`

Example request:

```json
{
  "viewerRef": "viewer-1",
  "batchSize": 20,
  "continuationMark": null,
  "omitClipRefs": [],
  "journeyRef": "session-001",
  "territoryHint": "NG"
}
```

Example response shape:

```json
{
  "status": "ok",
  "items": [],
  "continuationMark": "opaque-cursor",
  "more": true,
  "traceRef": "for_you-abc123"
}
```

### Signal Endpoint

- `POST /signals/engagement`

Captures viewer engagement such as watch, like, complete, save, or skip.

### Health and Maintenance

- `GET /status/alive`
- `GET /status/ready`
- `POST /status/rebuild`

`/status/ready` reports repository state, in-memory session ledger state, engine snapshot status, and vector index status.

## Data Model and Schema Mapping

The service is built to adapt to existing MongoDB collections through configuration rather than hardcoded schema assumptions.

Configurable areas include:

- collection names for viewers, clips, watches, likes, follows, served logs, and signal logs
- field names for viewer ids, clip ids, makers, territory, language, timestamps, and engagement counts
- vector collection name and dimension

That mapping lives in `watchtower.config` and is driven by `TTT_*` environment variables.

## Running the Project

### Local Python Run

```powershell
pip install -e .[dev,data]
python -m watchtower.app
```

### Rebuild the Snapshot and Vector Index

```powershell
python -m watchtower.tasks rebuild
```

### Write Viewer Affinity Summaries

```powershell
python -m watchtower.tasks affinities
```

## Running with Docker and Qdrant

`compose.yaml` starts the app and a local Qdrant instance:

```powershell
docker compose up --build
```

Key points:

- the app binds on port `8080`
- Qdrant binds on port `6333`
- snapshot artifacts are mounted from `./snapshot`

## Deployment Artifacts

The repository includes a small deployment surface instead of a large infrastructure tree:

- `Dockerfile`
  Builds the application image.
- `compose.yaml`
  Runs the app with Qdrant locally.
- `deploy/namespace.yaml`
  Creates the namespace.
- `deploy/app.yaml`
  Defines the API deployment, service, config map, and secret placeholder.
- `deploy/qdrant.yaml`
  Defines the Qdrant service and stateful set.
- `deploy/rebuild-job.yaml`
  Schedules index rebuilds as a Kubernetes `CronJob`.
- `.github/workflows/ci.yml`
  Runs tests and builds the image.

## Configuration

The main environment variables are:

- `TTT_BIND_HOST`
- `TTT_HTTP_PORT`
- `TTT_MONGO_DSN`
- `TTT_MONGO_DATABASE`
- `TTT_VECTOR_URL`
- `TTT_VECTOR_COLLECTION`
- `TTT_VECTOR_DIMENSION`
- `TTT_SNAPSHOT_PATH`
- `TTT_AFFINITY_PATH`
- `TTT_MAX_CANDIDATES`
- `TTT_FRESHNESS_HALF_LIFE_HOURS`
- `TTT_EXPLORATION_SCALE`
- `TTT_DIVERSITY_SCALE`

See [`public.env.sample`](./public.env.sample) for the full list.

## Project Layout

```text
watchtower/
  app.py
  config.py
  engine.py
  storage.py
  tasks.py
  vector_store.py
checks/
deploy/
demo-output/
guides/
```

## Verification

The project ships with checks for:

- API behavior
- ranking behavior
- task output
- secret-pattern hygiene
- required public repository assets

Run them with:

```powershell
python -m pytest checks
```

The `demo-output/` folder contains sanitized artifacts captured from a real local verification run.

## License

This project is released under the MIT License. See [`LICENSE`](./LICENSE).

## Author

Destiny Otto
AI/ML Engineer
