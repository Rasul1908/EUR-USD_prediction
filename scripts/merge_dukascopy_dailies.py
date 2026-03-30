"""
Merge all `EURUSD_YYYY-MM-DD_ticks_utc.parquet` files in the Dukascopy folder into one
combined Parquet, and rebuild `manifest.csv` + `validation_report.csv` from the data on disk
(same checks as `notebooks/ingest_dukascopy.ipynb`).

`missing_hours` in the manifest is inferred from which UTC hour buckets have no ticks
(not from Dukascopy HTTP), which matches stored data for auditing.

After scanning on-disk dailies, the manifest is **filled to every calendar day** from the
first to last daily filename so it aligns with `us_calendar.parquet` (e.g. Saturdays with
no Parquet file still appear as `rows=0`, `is_weekend=True`, `status=ok`). Row-sum checks
use only `rows` (zeros for missing files), so they still match the combined Parquet tick count.

Run from repo root:
  python scripts/merge_dukascopy_dailies.py
  python scripts/merge_dukascopy_dailies.py --out-dir data/dirty/ticks/raw_source/dukascopy
  python scripts/merge_dukascopy_dailies.py --output path/to/EURUSD_ticks_utc_combined.parquet
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Match ingest_dukascopy.ipynb validation thresholds
PRICE_MIN = 0.5
PRICE_MAX = 2.5
SPREAD_MAX_OK = 0.005
SPREAD_MAX_ERR = 0.020

DAILY_PATTERN = re.compile(r"^EURUSD_(\d{4}-\d{2}-\d{2})_ticks_utc\.parquet$")
COMBINED_PATTERN = re.compile(r"^EURUSD_ticks_utc_combined_.+\.parquet$")


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project; data/ and src/ must exist.")


def atomic_write_csv(df: pd.DataFrame, path: Path) -> Path:
    """Write CSV via a temp file + os.replace. If target is locked (Windows), write *_rebuilt.csv instead."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    try:
        os.replace(tmp, path)
        return path
    except OSError:
        alt = path.with_name(path.stem + "_rebuilt" + path.suffix)
        try:
            if alt.exists():
                alt.unlink()
            os.replace(tmp, alt)
            print(f"[WARN] Could not replace {path.name} (file open?). Wrote {alt.name}")
            return alt
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)


def list_daily_files(out_dir: Path) -> list[Path]:
    files = []
    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        if COMBINED_PATTERN.match(p.name):
            continue
        m = DAILY_PATTERN.match(p.name)
        if m:
            files.append(p)
    return sorted(files, key=lambda x: DAILY_PATTERN.match(x.name).group(1))


def infer_missing_hours(df: pd.DataFrame, day_str: str) -> list[int]:
    """UTC hours [0..23] with no tick in [day 00:00, next day 00:00)."""
    day_start = pd.Timestamp(day_str, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)
    if len(df) == 0:
        return list(range(24))
    ts = pd.to_datetime(df["timestamp_utc"], utc=True)
    missing: list[int] = []
    for h in range(24):
        h0 = day_start + pd.Timedelta(hours=h)
        h1 = h0 + pd.Timedelta(hours=1)
        if not ((ts >= h0) & (ts < h1)).any():
            missing.append(h)
    return missing


def validate_day(
    df: pd.DataFrame,
    day_str: str,
    missing_hours: list[int],
    cal: pd.DataFrame,
) -> tuple[str, list[str], list[dict]]:
    issues: list[dict] = []
    warn_codes: list[str] = []

    day_ts = pd.Timestamp(day_str)
    cal_row = cal.loc[day_ts] if day_ts in cal.index else None

    is_weekend = bool(cal_row["is_weekend"]) if cal_row is not None else None
    is_us_holiday = bool(cal_row["is_us_holiday"]) if cal_row is not None else None
    is_expected = bool(cal_row["is_expected_data"]) if cal_row is not None else None
    day_of_week = int(cal_row["day_of_week"]) if cal_row is not None else None
    is_friday = day_of_week == 4

    friday_eod_hours = {21, 22, 23}
    missing_set = set(missing_hours)
    missing_hours_expected = (
        bool(is_weekend)
        or bool(is_us_holiday)
        or (is_friday and missing_set.issubset(friday_eod_hours))
    )

    def add(issue: str, detail: str, severity: str) -> None:
        warn_codes.append(issue)
        issues.append({"date": day_str, "issue": issue, "detail": detail, "severity": severity})

    rows = len(df)

    if rows == 0 and is_expected and not is_weekend and not is_us_holiday:
        add(
            "empty_expected_day",
            f"0 rows on expected-data day (weekend={is_weekend}, holiday={is_us_holiday})",
            "error",
        )

    if len(missing_hours) > 0 and not missing_hours_expected:
        sev = "warn" if len(missing_hours) <= 6 else "error"
        add("missing_hours", f"{len(missing_hours)} hours missing: {missing_hours}", sev)

    if rows == 0:
        if any(i["severity"] == "error" for i in issues):
            status = "error"
        elif issues:
            status = "warn"
        else:
            status = "ok"
        return status, warn_codes, issues

    day_start = pd.Timestamp(day_str, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)

    min_ts = df["timestamp_utc"].min()
    max_ts = df["timestamp_utc"].max()
    if min_ts < day_start or max_ts >= day_end:
        add("ts_out_of_day", f"min={min_ts}, max={max_ts}, expected [{day_start}, {day_end})", "error")

    outside = ((df["timestamp_utc"] < day_start) | (df["timestamp_utc"] >= day_end)).sum()
    if outside > 0:
        add("timestamps_outside_day", f"{outside} rows outside [{day_start}, {day_end})", "error")

    if not df["timestamp_utc"].is_monotonic_increasing:
        add("unsorted_timestamps", "timestamp_utc is not non-decreasing", "error")

    dup_ts = int(df["timestamp_utc"].duplicated().sum())
    if dup_ts > 100:
        add("high_dup_ts_count", f"dup_ts_count={dup_ts}", "warn")

    for col in ["bid", "ask"]:
        bad = (~df[col].apply(lambda x: pd.notna(x) and x not in (float("inf"), float("-inf")))).sum()
        if bad > 0:
            add("bad_values", f"{bad} NaN/inf in column '{col}'", "error")

    bid_gt_ask = (df["bid"] > df["ask"]).sum()
    if bid_gt_ask > 0:
        add("bid_gt_ask", f"{bid_gt_ask} rows where bid > ask", "error")

    for col in ("bid_volume", "ask_volume"):
        bad = (~df[col].apply(lambda x: pd.notna(x) and x not in (float("inf"), float("-inf")))).sum()
        if bad > 0:
            add("bad_volume_values", f"{bad} NaN/inf in '{col}'", "error")
        neg = (df[col] < 0).sum()
        if neg > 0:
            add("negative_volume", f"{neg} rows with negative '{col}'", "error")

    spread = df["ask"] - df["bid"]
    spread_warn = (spread > SPREAD_MAX_OK).sum()
    spread_err = (spread > SPREAD_MAX_ERR).sum()
    if spread_err > 0:
        add(
            "spread_too_large",
            f"{spread_err} rows with spread > {SPREAD_MAX_ERR} ({SPREAD_MAX_ERR * 1e4:.0f} pips)",
            "error",
        )
    elif spread_warn > 0:
        add(
            "spread_elevated",
            f"{spread_warn} rows with spread > {SPREAD_MAX_OK} ({SPREAD_MAX_OK * 1e4:.0f} pips)",
            "warn",
        )

    price_bad = ((df["bid"] < PRICE_MIN) | (df["ask"] > PRICE_MAX)).sum()
    if price_bad > 0:
        add("price_out_of_bounds", f"{price_bad} rows outside [{PRICE_MIN}, {PRICE_MAX}]", "error")

    status = "ok"
    if any(i["severity"] == "error" for i in issues):
        status = "error"
    elif any(i["severity"] == "warn" for i in issues):
        status = "warn"

    return status, warn_codes, issues


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Folder with EURUSD_YYYY-MM-DD_ticks_utc.parquet (default: project dukascopy path).",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Combined Parquet path (default: EURUSD_ticks_utc_combined_<first>_<last>.parquet in out-dir).",
    )
    ap.add_argument(
        "--keep-old-combined",
        action="store_true",
        help="Do not delete other EURUSD_ticks_utc_combined_*.parquet in out-dir (default: delete them).",
    )
    ap.add_argument(
        "--manifest-report-only",
        action="store_true",
        help="Only scan dailies and write manifest + validation_report; require --verify-combined (no new merge).",
    )
    ap.add_argument(
        "--verify-combined",
        type=Path,
        default=None,
        help="With --manifest-report-only: Parquet whose row count must equal sum of daily rows.",
    )
    ap.add_argument(
        "--manifest-files-only",
        action="store_true",
        help="Omit calendar gap rows (only days with EURUSD_YYYY-MM-DD_ticks_utc.parquet).",
    )
    args = ap.parse_args()

    if args.manifest_report_only and not args.verify_combined:
        raise SystemExit("--manifest-report-only requires --verify-combined PATH")

    root = repo_root()
    out_dir = (args.out_dir or root / "data" / "dirty" / "ticks" / "raw_source" / "dukascopy").resolve()
    cal_path = root / "data" / "dirty" / "calendar" / "us_calendar.parquet"
    manifest_path = out_dir / "manifest.csv"
    report_path = out_dir / "validation_report.csv"

    daily_files = list_daily_files(out_dir)
    if not daily_files:
        raise SystemExit(f"No daily Parquets matching EURUSD_YYYY-MM-DD_ticks_utc.parquet in {out_dir}")

    first_d = DAILY_PATTERN.match(daily_files[0].name).group(1)
    last_d = DAILY_PATTERN.match(daily_files[-1].name).group(1)
    tag_s = first_d.replace("-", "")
    tag_e = last_d.replace("-", "")
    combined_path = (args.output or (out_dir / f"EURUSD_ticks_utc_combined_{tag_s}_{tag_e}.parquet")).resolve()

    cal = pd.read_parquet(cal_path)
    cal["date"] = pd.to_datetime(cal["date"]).dt.normalize()
    cal = cal.set_index("date")

    manifest_by_date: dict[str, dict] = {}
    report_rows: list[dict] = []
    writer: pq.ParquetWriter | None = None
    expected_rows = 0

    if args.manifest_report_only:
        combined_path = args.verify_combined.resolve()
        if not combined_path.is_file():
            raise FileNotFoundError(combined_path)
        print(f"Mode          : manifest + validation_report only (no merge)")
        print(f"Verify rows   : {combined_path}")
    else:
        if not args.keep_old_combined:
            for p in out_dir.glob("EURUSD_ticks_utc_combined_*.parquet"):
                if p.resolve() != combined_path:
                    p.unlink()
        if combined_path.exists():
            combined_path.unlink()

    print(f"Days to scan  : {len(daily_files)}  ({first_d} .. {last_d})")
    if not args.manifest_report_only:
        print(f"Output        : {combined_path}")

    for i, fpath in enumerate(daily_files):
        day_str = DAILY_PATTERN.match(fpath.name).group(1)
        tbl = pq.read_table(fpath)
        df = tbl.to_pandas()
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

        n_before = len(df)
        df_work = df.drop_duplicates()
        n_dropped = n_before - len(df_work)
        df_work = df_work.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)

        missing_hours = infer_missing_hours(df_work, day_str)

        day_ts = pd.Timestamp(day_str)
        cal_row = cal.loc[day_ts] if day_ts in cal.index else None
        is_weekend = bool(cal_row["is_weekend"]) if cal_row is not None else None
        is_holiday = bool(cal_row["is_us_holiday"]) if cal_row is not None else None
        is_expected = bool(cal_row["is_expected_data"]) if cal_row is not None else None

        status, warn_codes, day_issues = validate_day(df_work, day_str, missing_hours, cal)
        report_rows.extend(day_issues)

        if not args.manifest_report_only:
            try:
                tbl_out = pa.Table.from_pandas(df_work, schema=tbl.schema, preserve_index=False)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                tbl_out = pa.Table.from_pandas(df_work, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(str(combined_path), tbl_out.schema, compression="snappy")
            writer.write_table(tbl_out)

        min_ts = df_work["timestamp_utc"].min() if len(df_work) > 0 else None
        max_ts = df_work["timestamp_utc"].max() if len(df_work) > 0 else None
        dup_ts = int(df_work["timestamp_utc"].duplicated().sum())

        manifest_by_date[day_str] = {
            "date": day_str,
            "rows": len(df_work),
            "min_ts": str(min_ts) if min_ts is not None else "",
            "max_ts": str(max_ts) if max_ts is not None else "",
            "missing_hours": json.dumps(missing_hours),
            "dup_ts_count": dup_ts,
            "dropped_dupes": n_dropped,
            "is_weekend": is_weekend,
            "is_us_holiday": is_holiday,
            "is_expected_data": is_expected,
            "status": status,
            "warnings": json.dumps(warn_codes),
        }
        expected_rows += len(df_work)

        if (i + 1) % 50 == 0 or i == len(daily_files) - 1:
            print(f"  [{i + 1}/{len(daily_files)}] {day_str}  rows={len(df_work):>8,}  {status}")

    if not args.manifest_report_only:
        assert writer is not None
        writer.close()

    combined_rows = pq.ParquetFile(combined_path).metadata.num_rows
    if combined_rows != expected_rows:
        raise RuntimeError(
            f"Row count mismatch: combined Parquet={combined_rows:,} vs sum(rows) from merged dailies={expected_rows:,}"
        )

    empty_cols = ["timestamp_utc", "bid", "ask", "bid_volume", "ask_volume"]
    empty_df = pd.DataFrame(columns=empty_cols)
    if not args.manifest_files_only:
        calendar_days = pd.date_range(first_d, last_d, freq="D")
        filled = 0
        for d in calendar_days:
            day_str = d.strftime("%Y-%m-%d")
            if day_str in manifest_by_date:
                continue
            day_ts = pd.Timestamp(day_str)
            cal_row = cal.loc[day_ts] if day_ts in cal.index else None
            is_weekend = bool(cal_row["is_weekend"]) if cal_row is not None else None
            is_holiday = bool(cal_row["is_us_holiday"]) if cal_row is not None else None
            is_expected = bool(cal_row["is_expected_data"]) if cal_row is not None else None
            missing_hours = list(range(24))
            status, warn_codes, day_issues = validate_day(empty_df, day_str, missing_hours, cal)
            report_rows.extend(day_issues)
            manifest_by_date[day_str] = {
                "date": day_str,
                "rows": 0,
                "min_ts": "",
                "max_ts": "",
                "missing_hours": json.dumps(missing_hours),
                "dup_ts_count": 0,
                "dropped_dupes": 0,
                "is_weekend": is_weekend,
                "is_us_holiday": is_holiday,
                "is_expected_data": is_expected,
                "status": status,
                "warnings": json.dumps(warn_codes),
            }
            filled += 1
        if filled:
            print(f"Calendar fill  : +{filled} days with no Parquet (e.g. weekends) -> full {len(manifest_by_date)} rows")

    manifest = pd.DataFrame(list(manifest_by_date.values())).sort_values("date")
    sum_manifest = int(pd.to_numeric(manifest["rows"], errors="coerce").fillna(0).sum())
    if sum_manifest != combined_rows:
        raise RuntimeError(
            f"Row count mismatch: sum(manifest['rows'])={sum_manifest:,} vs combined Parquet={combined_rows:,}"
        )
    manifest_written = atomic_write_csv(manifest, manifest_path)

    report = (
        pd.DataFrame(report_rows)
        if report_rows
        else pd.DataFrame(columns=["date", "issue", "detail", "severity"])
    )
    if len(report) > 0:
        report = report.sort_values(["date", "issue"]).drop_duplicates(subset=["date", "issue"], keep="last")
    report_written = atomic_write_csv(report, report_path)

    print("\n" + "=" * 60)
    print("ROW COUNT AUDIT (tick rows only; calendar gap days contribute 0)")
    print("=" * 60)
    print(f"  Sum of manifest 'rows' column     : {sum_manifest:,}")
    print(f"  Combined Parquet metadata num_rows : {combined_rows:,}")
    print(f"  Match                            : {'YES' if sum_manifest == combined_rows else 'NO'}")
    print(f"  Manifest calendar days           : {len(manifest)}  (from {first_d} to {last_d})")
    print("=" * 60)
    print(f"Combined file : {combined_path}")
    print(f"Manifest      : {manifest_written}  ({len(manifest)} days)")
    print(f"Report        : {report_written}  ({len(report)} issue rows)")
    ok = (manifest["status"] == "ok").sum()
    warn = (manifest["status"] == "warn").sum()
    err = (manifest["status"] == "error").sum()
    print(f"Status counts : ok={ok}  warn={warn}  error={err}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
