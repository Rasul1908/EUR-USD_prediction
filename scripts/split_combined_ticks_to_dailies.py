"""
Recreate daily tick Parquets from one combined file (sorted by timestamp_utc).

Writes EURUSD_YYYY-MM-DD_ticks_utc.parquet into the same folder as the combined file
(or --out-dir). Existing files for the same UTC day are replaced.

Usage (repo root):
  python scripts/split_combined_ticks_to_dailies.py
  python scripts/split_combined_ticks_to_dailies.py --combined path/to/EURUSD_ticks_utc_combined_*.parquet
  python scripts/split_combined_ticks_to_dailies.py --out-dir path/to/dukascopy
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project; data/ and src/ must exist.")


def default_combined_path(out_dir: Path) -> Path:
    matches = sorted(out_dir.glob("EURUSD_ticks_utc_combined_*.parquet"))
    if not matches:
        raise FileNotFoundError(
            f"No EURUSD_ticks_utc_combined_*.parquet under {out_dir}. "
            "Pass --combined explicitly."
        )
    if len(matches) > 1:
        raise RuntimeError(
            "Multiple combined files found; pass --combined:\n" + "\n".join(str(p) for p in matches)
        )
    return matches[0]


def split_combined(combined: Path, out_dir: Path, batch_rows: int) -> tuple[int, list[Path]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pf = pq.ParquetFile(combined)
    writer: pq.ParquetWriter | None = None
    current_day: str | None = None
    schema: pa.Schema | None = None
    total = 0
    output_paths: list[Path] = []

    for batch in pf.iter_batches(batch_size=batch_rows):
        pdf = batch.to_pandas()
        pdf["_day"] = pd.to_datetime(pdf["timestamp_utc"], utc=True).dt.strftime("%Y-%m-%d")
        for day, grp in pdf.groupby("_day", sort=False):
            grp = grp.drop(columns=["_day"])
            tbl = pa.Table.from_pandas(grp, preserve_index=False)
            if schema is None:
                schema = tbl.schema
            elif tbl.schema != schema:
                tbl = tbl.cast(schema)
            if day != current_day:
                if writer is not None:
                    writer.close()
                    writer = None
                current_day = day
                out_path = out_dir / f"EURUSD_{day}_ticks_utc.parquet"
                if out_path.exists():
                    out_path.unlink()
                if not output_paths or output_paths[-1] != out_path:
                    output_paths.append(out_path)
                writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
            assert writer is not None
            writer.write_table(tbl)
            total += len(grp)

    if writer is not None:
        writer.close()

    print(f"Source : {combined}")
    print(f"Output : {out_dir}")
    print(f"Rows   : {total:,}  daily files written : {len(output_paths)}")
    return total, output_paths


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument(
        "--combined",
        type=Path,
        default=None,
        help="Path to EURUSD_ticks_utc_combined_*.parquet (default: sole match under dukascopy/)",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory for daily Parquets (default: same folder as combined)",
    )
    ap.add_argument("--batch-rows", type=int, default=500_000, help="Rows per read batch")
    args = ap.parse_args()

    root = repo_root()
    default_out = root / "data" / "dirty" / "ticks" / "raw_source" / "dukascopy"
    combined = args.combined or default_combined_path(default_out)
    combined = combined.resolve()
    if not combined.is_file():
        raise FileNotFoundError(combined)

    out_dir = (args.out_dir or combined.parent).resolve()

    n_expect = pq.ParquetFile(combined).metadata.num_rows
    total, paths = split_combined(combined, out_dir, args.batch_rows)
    written = sum(pq.ParquetFile(p).metadata.num_rows for p in paths)
    if total != n_expect or written != n_expect:
        raise RuntimeError(
            f"Row count mismatch: combined={n_expect:,} streamed={total:,} dailies_sum={written:,}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
