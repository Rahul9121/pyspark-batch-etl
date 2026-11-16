"""Lightweight config loader (no PyYAML dependency)."""
from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _parse_scalar(raw: str):
    v = raw.strip().strip('"').strip("'")
    if v.lower() in ("true", "false"):
        return v.lower() == "true"
    if re.fullmatch(r"-?\d+", v):
        return int(v)
    if re.fullmatch(r"-?\d+\.\d+", v):
        return float(v)
    return v


def load_yaml(path: Path) -> dict:
    """Minimal YAML subset parser for this project's config shape."""
    text = path.read_text(encoding="utf-8")
    root: dict = {}
    stack: list[tuple[int, dict | list]] = [(-1, root)]
    pending_key = None
    pending_indent = 0

    for line in text.splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            item = _parse_scalar(stripped[2:])
            if isinstance(parent, list):
                parent.append(item)
            elif pending_key is not None and isinstance(parent, dict):
                lst: list = []
                parent[pending_key] = lst
                stack.append((pending_indent, lst))
                lst.append(item)
                pending_key = None
            continue

        if ":" in stripped:
            key, _, rest = stripped.partition(":")
            key = key.strip()
            rest = rest.strip()
            if rest == "":
                new_map: dict = {}
                if isinstance(parent, dict):
                    parent[key] = new_map
                stack.append((indent, new_map))
                pending_key = key
                pending_indent = indent
            else:
                if isinstance(parent, dict):
                    parent[key] = _parse_scalar(rest)
                pending_key = None
    return root


def load_config(path: str | Path | None = None) -> dict:
    cfg_path = Path(path) if path else ROOT / "config" / "pipeline.yaml"
    cfg = load_yaml(cfg_path)
    cfg["_root"] = str(ROOT)
    # env overrides for CI
    if os.environ.get("PGHOST"):
        cfg.setdefault("postgres", {})["host"] = os.environ["PGHOST"]
    if os.environ.get("PGPORT"):
        cfg.setdefault("postgres", {})["port"] = int(os.environ["PGPORT"])
    if os.environ.get("PGDATABASE"):
        cfg.setdefault("postgres", {})["database"] = os.environ["PGDATABASE"]
    if os.environ.get("PGUSER"):
        cfg.setdefault("postgres", {})["user"] = os.environ["PGUSER"]
    if os.environ.get("PGPASSWORD"):
        cfg.setdefault("postgres", {})["password"] = os.environ["PGPASSWORD"]
    pg = cfg.get("postgres", {})
    host = pg.get("host", "localhost")
    port = pg.get("port", 5432)
    db = pg.get("database", "marketdata")
    cfg.setdefault("postgres", {})["jdbc_url"] = f"jdbc:postgresql://{host}:{port}/{db}"
    return cfg


def abspath(cfg: dict, rel: str) -> str:
    return str(Path(cfg["_root"]) / rel)
