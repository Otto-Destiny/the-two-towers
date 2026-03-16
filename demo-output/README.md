# Demo Output

These files are sanitized run artifacts captured from a local verification pass on 2026-03-16.

- Source: the current `the-two-towers` app running against the same small in-memory dataset used in the checks.
- Purpose: show the actual response shape, rebuild output, and vector index state from a real run.
- Note: these captures were produced without external infrastructure. The deployed path still uses Qdrant through `TTT_VECTOR_URL`, `compose.yaml`, and the manifests under `deploy/`.

Verification commands used during the same pass:

```powershell
python -m pytest checks
python -m watchtower.tasks rebuild
```
