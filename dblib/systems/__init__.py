"""Discover packaged database metadata without importing connectors."""

from __future__ import annotations

import json
import re
from importlib.resources import files
from pathlib import Path

SYSTEM_KEY_RE = re.compile(r"[a-z][a-z0-9_]*")
ENV_NAME_RE = re.compile(r"[A-Z_][A-Z0-9_]*")
IDENTITY_FIELDS = ("system", "proprietary", "hosted", "tuned", "tags")
SYSTEM_FIELDS = (*IDENTITY_FIELDS, "requirements", "credentials")


def parse_system(raw: str | bytes, source: Path) -> dict:
    try:
        data = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{source}: invalid JSON: {error}") from None
    if not isinstance(data, dict):
        raise ValueError(f"{source}: metadata must be a JSON object")
    if set(data) != set(SYSTEM_FIELDS):
        raise ValueError(f"{source}: metadata fields must be {list(SYSTEM_FIELDS)}")
    for field in ("system", "requirements"):
        value = data[field]
        if not isinstance(value, str) or not value.strip() or not value.isprintable():
            raise ValueError(f"{source}: {field} must be non-empty printable text")
    for field in ("proprietary", "hosted", "tuned"):
        if data[field] not in ("yes", "no"):
            raise ValueError(f"{source}: {field} must be 'yes' or 'no'")
    tags = data["tags"]
    if (not isinstance(tags, list) or not tags
            or any(not isinstance(tag, str) or not tag.strip() or not tag.isprintable()
                   for tag in tags)):
        raise ValueError(f"{source}: tags must be a non-empty list of printable text")
    credentials = data["credentials"]
    if (not isinstance(credentials, list)
            or any(not isinstance(name, str) or not ENV_NAME_RE.fullmatch(name)
                   for name in credentials)
            or len(credentials) != len(set(credentials))):
        raise ValueError(f"{source}: credentials must be unique environment variable names")
    if data["hosted"] == "no" and credentials:
        raise ValueError(f"{source}: a self-hosted system cannot require service credentials")
    return data


def system_dir(name: str) -> Path:
    if not SYSTEM_KEY_RE.fullmatch(name):
        raise ValueError(f"invalid system key {name!r}")
    return Path(str(files(__package__).joinpath(name)))


def load_system(name: str) -> dict:
    directory = system_dir(name)
    path = directory / "system.json"
    try:
        data = parse_system(path.read_bytes(), path)
    except OSError as error:
        raise ValueError(f"{path}: cannot read metadata: {error}") from None
    required = {"README.md"}
    implementation = {"adapter.py", "connector.py"}
    present = {file for file in implementation if (directory / file).is_file()}
    if present:
        required.update(implementation)
    if data["hosted"] == "no":
        required.update(implementation)
        required.update(("start", "check", "stop"))
    missing = sorted(file for file in required if not (directory / file).is_file())
    if missing:
        raise ValueError(f"{directory}: missing required files: {', '.join(missing)}")
    return data


def system_catalog() -> dict[str, dict]:
    root = Path(str(files(__package__)))
    names = sorted(path.name for path in root.iterdir()
                   if path.is_dir() and not path.name.startswith("_")
                   and (path / "system.json").is_file())
    return {name: load_system(name) for name in names}


def system_identity(meta: dict) -> dict:
    return {field: meta[field] for field in IDENTITY_FIELDS}
