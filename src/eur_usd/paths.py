from __future__ import annotations

from pathlib import Path


def get_project_root() -> Path:
    """
    Returns the repo root by walking upwards until we find a `data/` directory.
    """
    cur = Path(__file__).resolve()
    for parent in [cur.parent, *cur.parents]:
        if (parent / "data").exists():
            return parent
    # Fallback (shouldn't happen in this project).
    return Path(__file__).resolve().parents[3]


def data_dir() -> Path:
    return get_project_root() / "data"


def dirty_ticks_dir() -> Path:
    return data_dir() / "dirty" / "ticks" / "raw_source"


def per_second_dir() -> Path:
    return data_dir() / "interim" / "per_second"


def events_dir() -> Path:
    return data_dir() / "processed" / "events"

