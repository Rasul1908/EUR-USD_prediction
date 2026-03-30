from __future__ import annotations

from pathlib import Path
from typing import Any


def ticks_to_per_second(*, input_dir: Path, output_dir: Path, dry_run: bool = True) -> dict[str, Any]:
    """
    Placeholder for the actual tick -> per-second transformation.
    """
    if dry_run:
        return {
            "dry_run": True,
            "input_dir_exists": input_dir.exists(),
            "output_dir_exists": output_dir.exists(),
        }

    raise NotImplementedError(
        "ticks_to_per_second is not implemented yet. Add your raw tick loader + per-second resampling."
    )


def per_second_to_events(*, input_dir: Path, output_dir: Path, dry_run: bool = True) -> dict[str, Any]:
    """
    Placeholder for the actual per-second -> event transformation.
    """
    if dry_run:
        return {
            "dry_run": True,
            "input_dir_exists": input_dir.exists(),
            "output_dir_exists": output_dir.exists(),
        }

    raise NotImplementedError(
        "per_second_to_events is not implemented yet. Add your event builder logic."
    )

