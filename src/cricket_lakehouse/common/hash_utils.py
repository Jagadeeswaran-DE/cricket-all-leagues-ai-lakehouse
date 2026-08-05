from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return sha256_bytes(payload)


def stable_match_id(payload: dict[str, Any], source_file: str | None = None) -> tuple[str, str]:
    """Return a deterministic id and the rule used to create it."""
    stem = Path(source_file or "").stem
    if stem and stem.isdigit():
        return stem, "filename_stem"
    existing = payload.get("meta", {}).get("data_version")
    info = payload.get("info", {})
    attrs = {
        "dates": info.get("dates"),
        "event": info.get("event"),
        "teams": sorted(info.get("teams", [])),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "data_version": existing,
    }
    return f"fallback_{canonical_json_hash(attrs)[:32]}", "canonical_sha256"
