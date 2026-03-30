"""
Three-piece merge (your layout: one big combined + one 2021 daily + many 2025–26 dailies):

  1) Merge all daily Parquets with date >= --extension-from into one temp file (2025–26 block).
  2) Stream --base combined file; before the first row with timestamp >= --gap-cutoff, insert --gap.
  3) Append the 2025–26 block (already time-ordered after the base tail).

No full split of the base file into dailies.

Run from repo root:
  python scripts/merge_ticks_base_gap_extension.py \\
    --base data/dirty/ticks/raw_source/dukascopy/EURUSD_ticks_utc_combined_....parquet \\
    --gap data/dirty/ticks/raw_source/dukascopy/EURUSD_2021-06-11_ticks_utc.parquet \\
    --out-dir data/dirty/ticks/raw_source/dukascopy \\
    --out data/dirty/ticks/raw_source/dukascopy/EURUSD_ticks_utc_combined_FULL.parquet

Adjust --extension-from so it is the first calendar day of your 2025–26 dailies (no overlap with base).
"""
from __future__ import annotations

import argparse
import re
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DAILY_PATTERN = re.compile(r"^EURUSD_(\d{4}-\d{2}-\d{2})_ticks_utc\.parquet$")
COMBINED_PATTERN = re.compile(r"^EURUSD_ticks_utc_combined_.+\.parquet$")


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project; data/ and src/ must exist.")


def list_extension_dailies(out_dir: Path, extension_from: str) -> list[Path]:
    paths: list[Path] = []
    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        if COMBINED_PATTERN.match(p.name):
            continue
        m = DAILY_PATTERN.match(p.name)
        if not m:
            continue
        if m.group(1) >= extension_from:
            paths.append(p)
    return sorted(paths, key=lambda x: DAILY_PATTERN.match(x.name).group(1))


def _table_from_pandas(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    try:
        tbl = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        tbl = pa.Table.from_pandas(df, preserve_index=False)
    if tbl.schema != schema:
        tbl = tbl.cast(schema)
    return tbl


def merge_dailies_to_temp(
    paths: list[Path],
    tmp_path: Path,
    base_schema: pa.Schema | None,
) -> tuple[pa.Schema, int]:
    """Write one Parquet; return (schema, row_count)."""
    writer: pq.ParquetWriter | None = None
    n = 0
    for p in paths:
        tbl = pq.read_table(p)
        if base_schema is not None and tbl.schema != base_schema:
            tbl = tbl.cast(base_schema)
        if writer is None:
            writer = pq.ParquetWriter(str(tmp_path), tbl.schema, compression="snappy")
        writer.write_table(tbl)
        n += tbl.num_rows
    if writer is None:
        raise RuntimeError("No extension daily files to merge.")
    writer.close()
    return pq.ParquetFile(tmp_path).schema_arrow, n


def stream_base_insert_gap_then_append_extension(
    base_path: Path,
    gap_path: Path,
    extension_merged_path: Path,
    out_path: Path,
    cutoff: pd.Timestamp,
    batch_rows: int,
) -> tuple[int, int, int]:
    """
    Returns (rows_base_written, rows_gap, rows_extension_appended).
    Rows counted from gap table and extension file metadata; base from loop.
    """
    gap_tbl = pq.read_table(gap_path)
    pf_base = pq.ParquetFile(base_path)
    schema = pf_base.schema_arrow
    if gap_tbl.schema != schema:
        gap_tbl = gap_tbl.cast(schema)

    gap_inserted = [False]
    n_base = 0

    def write_gap_if_needed(writer: pq.ParquetWriter) -> None:
        if gap_inserted[0]:
            return
        writer.write_table(gap_tbl)
        gap_inserted[0] = True

    if out_path.exists():
        out_path.unlink()

    writer: pq.ParquetWriter | None = None

    for batch in pf_base.iter_batches(batch_size=batch_rows):
        df = batch.to_pandas()
        if df.empty:
            continue
        ts = pd.to_datetime(df["timestamp_utc"], utc=True)
        n_base += len(df)

        if ts.max() < cutoff:
            tbl = _table_from_pandas(df, schema)
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
            writer.write_table(tbl)
            continue

        if ts.min() >= cutoff:
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
            write_gap_if_needed(writer)
            tbl = _table_from_pandas(df, schema)
            writer.write_table(tbl)
            continue

        before = df.loc[ts < cutoff]
        after = df.loc[ts >= cutoff]
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), schema, compression="snappy")
        if len(before):
            writer.write_table(_table_from_pandas(before, schema))
        write_gap_if_needed(writer)
        if len(after):
            writer.write_table(_table_from_pandas(after, schema))

    if writer is None:
        raise RuntimeError("Base file produced no rows (empty?).")

    if not gap_inserted[0]:
        writer.write_table(gap_tbl)

    n_ext = 0
    pf_ext = pq.ParquetFile(extension_merged_path)
    if pf_ext.schema_arrow != schema:
        for batch in pf_ext.iter_batches(batch_size=batch_rows):
            t = pa.Table.from_batches([batch]).cast(schema)
            writer.write_table(t)
            n_ext += t.num_rows
    else:
        for batch in pf_ext.iter_batches(batch_size=batch_rows):
            writer.write_table(pa.Table.from_batches([batch]))
            n_ext += batch.num_rows

    writer.close()

    return n_base, gap_tbl.num_rows, n_ext


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--base", type=Path, required=True, help="Existing combined Parquet (history through late 2025).")
    ap.add_argument("--gap", type=Path, required=True, help="Single daily Parquet to insert (e.g. 2021-06-11).")
    ap.add_argument(
        "--gap-cutoff",
        default="2021-06-12",
        help="Insert gap before timestamps on/after this UTC date (default: 2021-06-12).",
    )
    ap.add_argument(
        "--extension-from",
        default="2025-11-05",
        help="Merge every EURUSD_YYYY-MM-DD_ticks_utc.parquet with YYYY-MM-DD >= this into the tail block.",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Folder containing extension dailies (default: parent of --base).",
    )
    ap.add_argument("--out", type=Path, required=True, help="Final combined Parquet path.")
    ap.add_argument("--batch-rows", type=int, default=800_000)
    ap.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temp extension-merged Parquet next to --out (named .__ext_merged_tmp.parquet).",
    )
    args = ap.parse_args()

    base_path = args.base.resolve()
    gap_path = args.gap.resolve()
    out_dir = (args.out_dir or base_path.parent).resolve()
    out_path = args.out.resolve()

    if not base_path.is_file():
        raise FileNotFoundError(base_path)
    if not gap_path.is_file():
        raise FileNotFoundError(gap_path)

    cutoff = pd.Timestamp(args.gap_cutoff, tz="UTC")

    ext_files = list_extension_dailies(out_dir, args.extension_from)
    if not ext_files:
        raise SystemExit(
            f"No extension dailies found in {out_dir} with date >= {args.extension_from}. "
            "Lower --extension-from or add files."
        )

    first_ext = DAILY_PATTERN.match(ext_files[0].name).group(1)
    last_ext = DAILY_PATTERN.match(ext_files[-1].name).group(1)
    print(f"Extension dailies: {len(ext_files)} files ({first_ext} .. {last_ext})")

    base_schema = pq.ParquetFile(base_path).schema_arrow
    tmp_dir: Path | None = None
    if args.keep_temp:
        tmp_ext = out_path.parent / ".__ext_merged_tmp.parquet"
        if tmp_ext.exists():
            tmp_ext.unlink()
    else:
        tmp_dir = Path(tempfile.mkdtemp(dir=str(out_path.parent)))
        tmp_ext = tmp_dir / "ext_merged.parquet"

    try:
        _, ext_rows = merge_dailies_to_temp(ext_files, tmp_ext, base_schema)
        print(f"Temp extension merge: {tmp_ext}  rows={ext_rows:,}")

        n_b, n_g, n_e = stream_base_insert_gap_then_append_extension(
            base_path,
            gap_path,
            tmp_ext,
            out_path,
            cutoff,
            args.batch_rows,
        )
    finally:
        if not args.keep_temp and tmp_dir is not None:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    total = pq.ParquetFile(out_path).metadata.num_rows
    print(f"Base rows (scanned): {n_b:,}")
    print(f"Gap rows:            {n_g:,}")
    print(f"Extension rows:      {n_e:,}")
    print(f"Output rows:         {total:,}")
    if total != n_b + n_g + n_e:
        print(
            f"[WARN] n_base+n_gap+n_ext = {n_b + n_g + n_e:,} != file rows {total:,} "
            "(can happen if base overlapped extension dates — remove overlap.)"
        )
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
