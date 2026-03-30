from __future__ import annotations

from pathlib import Path
from typing import Any

from eur_usd.paths import dirty_ticks_dir, per_second_dir, events_dir
from eur_usd.pipeline.transform import per_second_to_events, ticks_to_per_second


def run_pipeline(*, dry_run: bool = True, verbose: bool = True) -> dict[str, Any]:
    """
    Wiring-only starter pipeline.

    When `dry_run=True`, it checks folder existence and returns a summary dict.
    """
    input_dir: Path = dirty_ticks_dir()
    out_per_second: Path = per_second_dir()
    out_events: Path = events_dir()

    if verbose:
        print("=== EUR/USD Pipeline (starter) ===")
        print(f"Dirty ticks: {input_dir}")
        print(f"Per-second output: {out_per_second}")
        print(f"Events output: {out_events}")
        print(f"Mode: {'dry-run' if dry_run else 'process'}")

    if dry_run:
        # Ensure output dirs exist even in dry-run mode.
        out_per_second.mkdir(parents=True, exist_ok=True)
        out_events.mkdir(parents=True, exist_ok=True)

        return {
            "dry_run": True,
            "ticks_to_per_second": ticks_to_per_second(
                input_dir=input_dir, output_dir=out_per_second, dry_run=True
            ),
            "per_second_to_events": per_second_to_events(
                input_dir=out_per_second, output_dir=out_events, dry_run=True
            ),
        }

    # Real processing mode: transform functions currently raise NotImplementedError.
    if verbose:
        print("Starting ticks -> per_second ...")
    ticks_to_per_second(input_dir=input_dir, output_dir=out_per_second, dry_run=False)

    if verbose:
        print("Starting per_second -> events ...")
    per_second_to_events(input_dir=out_per_second, output_dir=out_events, dry_run=False)

    return {"dry_run": False}

