"""
One-off backfill for Dukascopy days that failed under bulk ingest (timeouts / empty reads).

Fetches:
  - 2021-06-11  (June 11, 2021 — US date 6/11/2021)
  - 2025-11-05 .. 2026-03-27  (Nov 5, 2025 — US 11/5/2025 — through original END_DATE)

Writes daily Parquets next to the main ingest, then patches manifest.csv and validation_report.csv
(replaces rows for those dates).

Run from repo root (default order: **2025-11-05 … 2026-03-27 first**, then **2021-06-11**):
  python scripts/fix_dukascopy_gap_dates.py
  python scripts/fix_dukascopy_gap_dates.py --extension-only   # only Nov 2025 → Mar 27 2026
  python scripts/fix_dukascopy_gap_dates.py --extra-only       # only EXTRA_DATES (2021-06-11)

Later: delete this file if you no longer need it.

Merging with the big combined Parquet (sorted by time):
  - To avoid splitting the whole file into dailies:  python scripts/merge_combined_gap_extension.py
    (one pass over main + insert gap day + append extension dailies).
  - Or recreate dailies:  python scripts/split_combined_ticks_to_dailies.py  then notebook merge cell.
  - Without the combined file, re-run notebooks/ingest_dukascopy.ipynb for the full range; consider
    DELETE_DAILY_AFTER_MERGE = False until you no longer need per-day files.
"""
from __future__ import annotations

import argparse
import json
import lzma
import struct
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd

# --- Tunables (match ingest notebook) ---------------------------------
SYMBOL = "EURUSD"
PRICE_SCALE = 1e5
HTTP_TIMEOUT_SEC = 60
HTTP_RETRIES = 5
HTTP_RETRY_BACKOFF_SEC = 0.8
DELAY_BETWEEN_HOURS_SEC = 0.05

PRICE_MIN = 0.5
PRICE_MAX = 2.5
SPREAD_MAX_OK = 0.005
SPREAD_MAX_ERR = 0.020

# June 11, 2021 (6/11/2021) + range from Nov 5, 2025 (11/5/2025)
EXTRA_DATES = ["2021-06-11"]
GAP_START = "2025-11-05"
GAP_END = "2026-03-27"


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "data").exists() and (here / "src").exists():
        return here
    raise RuntimeError("Run from EUR_USD_Project; data/ and src/ must exist.")


def dukascopy_url(symbol: str, day: datetime, hour: int) -> str:
    return (
        f"https://datafeed.dukascopy.com/datafeed/{symbol}/"
        f"{day.year}/{day.month - 1:02d}/{day.day:02d}/{hour:02d}h_ticks.bi5"
    )


def decode_bi5(blob: bytes, hour_start: datetime) -> pd.DataFrame:
    raw = lzma.decompress(blob)
    rec_size = 20
    n = len(raw) // rec_size
    rows = []
    for i in range(n):
        ms, ask_i, bid_i, ask_vol, bid_vol = struct.unpack_from(">IIIff", raw, i * rec_size)
        ts = hour_start + timedelta(milliseconds=int(ms))
        rows.append((ts, bid_i / PRICE_SCALE, ask_i / PRICE_SCALE, ask_vol, bid_vol))
    return pd.DataFrame(rows, columns=["timestamp_utc", "bid", "ask", "bid_volume", "ask_volume"])


def fetch_bi5_hour(url: str) -> bytes | None:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        },
    )
    backoff = HTTP_RETRY_BACKOFF_SEC
    for attempt in range(HTTP_RETRIES):
        try:
            with urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
                blob = resp.read()
            if blob:
                return blob
        except Exception:
            pass
        if attempt < HTTP_RETRIES - 1:
            time.sleep(backoff * (2 ** attempt))
    return None


def download_day(symbol: str, day_str: str) -> tuple[pd.DataFrame, list[int]]:
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    chunks: list[pd.DataFrame] = []
    missing_hours: list[int] = []

    for hour in range(24):
        hour_start = day + timedelta(hours=hour)
        url = dukascopy_url(symbol, day, hour)
        blob = fetch_bi5_hour(url)
        if not blob:
            missing_hours.append(hour)
            if DELAY_BETWEEN_HOURS_SEC:
                time.sleep(DELAY_BETWEEN_HOURS_SEC)
            continue
        try:
            chunks.append(decode_bi5(blob, hour_start))
        except Exception as exc:
            missing_hours.append(hour)
            print(f"  [WARN] decode {day_str} h{hour:02d}: {exc}")
        if DELAY_BETWEEN_HOURS_SEC:
            time.sleep(DELAY_BETWEEN_HOURS_SEC)

    if not chunks:
        df = pd.DataFrame(columns=["timestamp_utc", "bid", "ask", "bid_volume", "ask_volume"])
    else:
        df = pd.concat(chunks, ignore_index=True)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    return df, missing_hours


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

    dup_ts = df["timestamp_utc"].duplicated().sum()
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


def extension_date_strings() -> list[str]:
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(GAP_START, GAP_END, freq="D")]


def build_target_dates(*, mode: str) -> list[str]:
    """
    mode:
      'all' — forward extension (GAP_START..GAP_END) first, then EXTRA_DATES (your preferred order)
      'extension' — only GAP range
      'extra' — only EXTRA_DATES
    """
    gap = extension_date_strings()
    extra = sorted(EXTRA_DATES)
    if mode == "extension":
        return gap
    if mode == "extra":
        return extra
    return gap + extra


def patch_manifest_report(
    manifest_path: Path,
    report_path: Path,
    new_manifest_rows: list[dict],
    new_report_rows: list[dict],
    processed_dates: set[str],
) -> None:
    new_m = pd.DataFrame(new_manifest_rows)
    if manifest_path.exists():
        old = pd.read_csv(manifest_path, dtype=str)
        old = old[~old["date"].isin(processed_dates)]
        manifest = pd.concat([old, new_m], ignore_index=True)
    else:
        manifest = new_m
    manifest = manifest.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    manifest.to_csv(manifest_path, index=False)

    new_r = (
        pd.DataFrame(new_report_rows)
        if new_report_rows
        else pd.DataFrame(columns=["date", "issue", "detail", "severity"])
    )
    if report_path.exists():
        old_r = pd.read_csv(report_path, dtype=str)
        old_r = old_r[~old_r["date"].isin(processed_dates)]
        report = pd.concat([old_r, new_r], ignore_index=True)
    else:
        report = new_r
    if len(report) > 0 and "date" in report.columns:
        report = report.sort_values("date").drop_duplicates(subset=["date", "issue"], keep="last")
    report.to_csv(report_path, index=False)


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill Dukascopy days; patches manifest + validation_report.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument(
        "--extension-only",
        action="store_true",
        help=f"Only {GAP_START} .. {GAP_END} (run this first if bulk ingest stopped before Nov 2025).",
    )
    g.add_argument(
        "--extra-only",
        action="store_true",
        help="Only EXTRA_DATES (e.g. 2021-06-11); run after extension backfill if you want two steps.",
    )
    args = ap.parse_args()
    mode = "extension" if args.extension_only else "extra" if args.extra_only else "all"

    root = repo_root()
    out_dir = root / "data" / "dirty" / "ticks" / "raw_source" / "dukascopy"
    cal_path = root / "data" / "dirty" / "calendar" / "us_calendar.parquet"
    manifest_path = out_dir / "manifest.csv"
    report_path = out_dir / "validation_report.csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    cal = pd.read_parquet(cal_path)
    cal["date"] = pd.to_datetime(cal["date"]).dt.normalize()
    cal = cal.set_index("date")

    dates = build_target_dates(mode=mode)
    if not dates:
        print("No dates to process.")
        return 0
    print(f"Backfill ({mode}) {len(dates)} days: first={dates[0]} last={dates[-1]}")

    manifest_rows: list[dict] = []
    report_rows: list[dict] = []
    processed: set[str] = set()

    for day_str in dates:
        processed.add(day_str)
        parquet_path = out_dir / f"{SYMBOL}_{day_str}_ticks_utc.parquet"
        day_ts = pd.Timestamp(day_str)
        cal_row = cal.loc[day_ts] if day_ts in cal.index else None
        is_weekend = bool(cal_row["is_weekend"]) if cal_row is not None else None
        is_holiday = bool(cal_row["is_us_holiday"]) if cal_row is not None else None
        is_expected = bool(cal_row["is_expected_data"]) if cal_row is not None else None

        df, missing_hours = download_day(SYMBOL, day_str)
        n_before = len(df)
        df = df.drop_duplicates()
        n_dropped = n_before - len(df)
        df = df.sort_values("timestamp_utc", kind="stable").reset_index(drop=True)

        status, warn_codes, day_issues = validate_day(df, day_str, missing_hours, cal)
        report_rows.extend(day_issues)

        if len(df) > 0:
            df.to_parquet(parquet_path, index=False)

        min_ts = df["timestamp_utc"].min() if len(df) > 0 else None
        max_ts = df["timestamp_utc"].max() if len(df) > 0 else None
        dup_ts = int(df["timestamp_utc"].duplicated().sum()) if len(df) > 0 else 0

        manifest_rows.append(
            {
                "date": day_str,
                "rows": len(df),
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
        )
        print(f"  {day_str}  rows={len(df):>7,}  status={status}  missing_h={len(missing_hours)}")

    patch_manifest_report(manifest_path, report_path, manifest_rows, report_rows, processed)
    print(f"\nWrote manifest -> {manifest_path}")
    print(f"Wrote report   -> {report_path}")
    print("\nTo refresh the BIG combined parquet: re-run the merge cell in ingest_dukascopy.ipynb")
    print("(or use DuckDB / pyarrow to merge+sort — see module docstring).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
