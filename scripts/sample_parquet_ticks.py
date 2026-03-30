"""
Print a small sample of rows from a tick Parquet (combined or daily).

Head / tail avoid loading the whole file (row-group / batch reads). Random sampling
loads the full file into memory — avoid on multi-GB Parquets.

Examples (repo root):
  python scripts/sample_parquet_ticks.py
  python scripts/sample_parquet_ticks.py --path data/dirty/ticks/raw_source/dukascopy/EURUSD_ticks_utc_combined_from_all_dailies.parquet
  python scripts/sample_parquet_ticks.py -n 12 --tail
  python scripts/sample_parquet_ticks.py -n 10 --random-seed 42
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project.")


def default_combined(out_dir: Path) -> Path:
    matches = sorted(out_dir.glob("EURUSD_ticks_utc_combined_*.parquet"))
    if not matches:
        raise FileNotFoundError(f"No EURUSD_ticks_utc_combined_*.parquet under {out_dir}")
    return matches[-1]


def read_head(path: Path, n: int) -> pd.DataFrame:
    pf = pq.ParquetFile(path)
    parts: list[pd.DataFrame] = []
    got = 0
    for batch in pf.iter_batches(batch_size=min(262_144, max(n, 8192))):
        parts.append(batch.to_pandas())
        got += len(parts[-1])
        if got >= n:
            break
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).head(n)


def read_tail(path: Path, n: int) -> pd.DataFrame:
    pf = pq.ParquetFile(path)
    chunks: list[pd.DataFrame] = []
    got = 0
    for rg in range(pf.num_row_groups - 1, -1, -1):
        chunks.append(pf.read_row_group(rg).to_pandas())
        got += len(chunks[-1])
        if got >= n:
            break
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(list(reversed(chunks)), ignore_index=True)
    return df.iloc[-n:]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--path", type=Path, default=None, help="Parquet file (default: last sorted name in dukascopy/)")
    ap.add_argument("-n", "--num-rows", type=int, default=12, help="How many rows to show")
    ap.add_argument("--tail", action="store_true", help="Last N rows (reads from end row groups)")
    ap.add_argument(
        "--random-seed",
        type=int,
        default=None,
        help="Random sample of N rows (loads entire file — use only on small Parquets)",
    )
    args = ap.parse_args()

    root = repo_root()
    duk = root / "data" / "dirty" / "ticks" / "raw_source" / "dukascopy"
    path = (args.path or default_combined(duk)).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)

    total = pq.ParquetFile(path).metadata.num_rows
    print(f"File : {path}")
    print(f"Rows : {total:,}\n")

    if args.random_seed is not None:
        sz_gb = path.stat().st_size / (1024**3)
        if sz_gb > 0.5:
            print(f"[WARN] File is ~{sz_gb:.2f} GiB; random mode loads all rows into RAM.\n")
        df = pd.read_parquet(path)
        k = min(args.num_rows, len(df))
        sample = df.sample(n=k, random_state=args.random_seed)
        print(sample.to_string())
        return 0

    if args.tail:
        df = read_tail(path, args.num_rows)
    else:
        df = read_head(path, args.num_rows)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 220)
    print(df.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
