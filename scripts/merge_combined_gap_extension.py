"""
Merge without splitting the whole history into dailies:

  1) Stream the **main** combined Parquet (must be sorted by `timestamp_utc`).
  2) When you cross **2021-06-12 00:00 UTC**, insert the **gap day** Parquet (e.g. 2021-06-11).
  3) Continue streaming the rest of **main**.
  4) **Append** extension daily Parquets (e.g. 2025-11-05 .. 2026-03-27) in filename date order.

One output file; one full scan of `main` (plus small reads for gap + extension files).

Run from repo root:
  python scripts/merge_combined_gap_extension.py \\
    --main data/dirty/ticks/raw_source/dukascopy/EURUSD_ticks_utc_combined_....parquet \\
    --gap data/dirty/ticks/raw_source/dukascopy/EURUSD_2021-06-11_ticks_utc.parquet \\
    --extension-from 2025-11-05

Omit --gap if you have no middle insert (append-only extension).

Adjust --cutoff if your gap day is not 2021-06-11 (cutoff = first instant *after* the gap UTC day).
"""
from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project; data/ and src/ must exist.")


DAILY_NAME = re.compile(r"^EURUSD_(\d{4}-\d{2}-\d{2})_ticks_utc\.parquet$")


def extension_paths(out_dir: Path, extension_from: str) -> list[Path]:
    """Daily Parquets in out_dir with date >= extension_from (UTC calendar date in filename)."""
    start = extension_from
    paths: list[Path] = []
    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        m = DAILY_NAME.match(p.name)
        if not m:
            continue
        d = m.group(1)
        if d >= start:
            paths.append(p)
    return sorted(paths, key=lambda x: DAILY_NAME.match(x.name).group(1))


def table_from_df(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def write_gap_if_needed(
    writer: pq.ParquetWriter,
    gap_tbl: pa.Table | None,
    gap_inserted: list[bool],
) -> None:
    if gap_tbl is None or gap_inserted[0]:
        return
    writer.write_table(gap_tbl)
    gap_inserted[0] = True


def stream_main_with_gap(
    main_path: Path,
    writer: pq.ParquetWriter,
    schema: pa.Schema,
    cutoff: pd.Timestamp,
    gap_tbl: pa.Table | None,
    batch_rows: int,
) -> int:
    """Return row count written from main (excluding gap)."""
    pf = pq.ParquetFile(main_path)
    gap_inserted = [gap_tbl is None]  # True => skip gap logic
    n_main = 0

    for batch in pf.iter_batches(batch_size=batch_rows):
        df = batch.to_pandas()
        if df.empty:
            continue
        ts = pd.to_datetime(df["timestamp_utc"], utc=True)
        n_main += len(df)

        if ts.max() < cutoff:
            writer.write_table(table_from_df(df, schema))
            continue

        if ts.min() >= cutoff:
            write_gap_if_needed(writer, gap_tbl, gap_inserted)
            writer.write_table(table_from_df(df, schema))
            continue

        before = df.loc[ts < cutoff]
        after = df.loc[ts >= cutoff]
        if len(before):
            writer.write_table(table_from_df(before, schema))
        write_gap_if_needed(writer, gap_tbl, gap_inserted)
        if len(after):
            writer.write_table(table_from_df(after, schema))

    if gap_tbl is not None and not gap_inserted[0]:
        # Main never reached cutoff (unlikely for a multi-year file)
        writer.write_table(gap_tbl)

    return n_main


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--main", type=Path, required=True, help="Existing combined Parquet (sorted by time).")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output combined Parquet (default: next to main, suffix _merged).",
    )
    ap.add_argument(
        "--gap",
        type=Path,
        default=None,
        help="Gap day Parquet to insert before first main row with ts >= cutoff (e.g. 2021-06-11).",
    )
    ap.add_argument(
        "--cutoff",
        default="2021-06-12",
        help="UTC date: insert gap before timestamps on or after this day (default: 2021-06-12).",
    )
    ap.add_argument(
        "--extension-from",
        default="2025-11-05",
        help="Append all EURUSD_YYYY-MM-DD_ticks_utc.parquet in OUT_DIR with date >= this (YYYY-MM-DD).",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory containing extension dailies (default: same folder as --main).",
    )
    ap.add_argument("--batch-rows", type=int, default=800_000)
    ap.add_argument(
        "--no-extension",
        action="store_true",
        help="Do not append any daily files from OUT_DIR (gap insert + main only).",
    )
    args = ap.parse_args()

    main_path = args.main.resolve()
    if not main_path.is_file():
        raise FileNotFoundError(main_path)

    out_dir = (args.out_dir or main_path.parent).resolve()
    out_path = (args.out or main_path.with_name(main_path.stem + "_merged.parquet")).resolve()

    cutoff = pd.Timestamp(args.cutoff, tz="UTC")

    pf0 = pq.ParquetFile(main_path)
    schema = pf0.schema_arrow

    gap_tbl: pa.Table | None = None
    if args.gap:
        gap_path = args.gap.resolve()
        if not gap_path.is_file():
            raise FileNotFoundError(gap_path)
        gap_tbl = pq.read_table(gap_path)
        if gap_tbl.schema != schema:
            gap_tbl = gap_tbl.cast(schema)

    n_gap = gap_tbl.num_rows if gap_tbl is not None else 0
    n_ext = 0

    writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
    try:
        n_main = stream_main_with_gap(
            main_path, writer, schema, cutoff, gap_tbl, args.batch_rows
        )

        if not args.no_extension:
            ext_files = extension_paths(out_dir, args.extension_from)
            for ep in ext_files:
                t = pq.read_table(ep)
                if t.schema != schema:
                    t = t.cast(schema)
                writer.write_table(t)
                n_ext += t.num_rows
    finally:
        writer.close()

    total = pq.ParquetFile(out_path).metadata.num_rows
    print(f"Main rows scanned : {n_main:,}")
    print(f"Gap rows          : {n_gap:,}")
    print(f"Extension rows    : {n_ext:,}")
    print(f"Output rows       : {total:,}")
    print(f"Wrote             : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
