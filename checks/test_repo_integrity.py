from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_required_public_project_assets_exist():
    required = [
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "LICENSE",
        PROJECT_ROOT / "Dockerfile",
        PROJECT_ROOT / "compose.yaml",
        PROJECT_ROOT / "deploy" / "app.yaml",
        PROJECT_ROOT / "deploy" / "qdrant.yaml",
        PROJECT_ROOT / "deploy" / "rebuild-job.yaml",
        PROJECT_ROOT / "demo-output" / "README.md",
    ]
    for path in required:
        assert path.exists(), f"missing project asset: {path}"


def test_no_private_credentials_in_repository():
    forbidden = [
        re.compile(r"mongodb\+srv://[^:\s]+:[^@\s]+@", re.IGNORECASE),
        re.compile(r"mongodb://[^:\s]+:[^@\s]+@", re.IGNORECASE),
        re.compile(r"AKIA[0-9A-Z]{16}"),
        re.compile(r"ghp_[A-Za-z0-9]{20,}"),
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    ]
    skip = {PROJECT_ROOT / "checks" / "test_repo_integrity.py"}
    for path in PROJECT_ROOT.rglob("*"):
        if not path.is_file() or path in skip:
            continue
        if path.suffix == ".pyc":
            continue
        if ".pytest_cache" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in forbidden:
            assert not pattern.search(text), f"{pattern.pattern} found in {path}"
