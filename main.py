from __future__ import annotations

import argparse
from pathlib import Path
import sys


def _ensure_src_on_path() -> None:
    """
    Make `src/` importable when running `python main.py` from the repo root.
    """
    repo_root = Path(__file__).resolve().parent
    src_dir = repo_root / "src"
    sys.path.insert(0, str(src_dir))


def main() -> int:
    parser = argparse.ArgumentParser(description="EUR/USD pipeline runner")
    parser.add_argument(
        "--run",
        action="store_true",
        help="Actually perform processing (default is dry-run / wiring only).",
    )
    args = parser.parse_args()

    _ensure_src_on_path()
    from eur_usd.pipeline.run import run_pipeline  # noqa: E402

    run_pipeline(dry_run=not args.run)
    return 0

    


if __name__ == "__main__":
    raise SystemExit(main())


