# Features Log

Tracks every feature added to the project, in order.

| # | Feature | Description | Status |
|---|---------|-------------|--------|
| 1 | US Calendar | Date-level calendar (2020–2026) with `is_weekend`, `is_us_holiday`, `is_expected_data` flags. Saved to `data/dirty/calendar/`. Used by validation and downstream joins. | done |
| 2 | Dukascopy Tick Ingest | Download, decode, validate EUR/USD tick data from Dukascopy. Daily Parquet per UTC day; optional merge to one `EURUSD_ticks_utc_combined_*` file and delete dailies. Config: `START_DATE` / `END_DATE` in `notebooks/ingest_dukascopy.ipynb`. | done |
| 3 | Tick Validation | Calendar-aware checks: completeness, time bounds, sort order, duplicates, bad values, spread sanity. | done |
| 4 | Manifest | Per-day summary (`rows`, `min_ts`, `max_ts`, `missing_hours`, `dup_ts_count`, flags, `status`). Saved to `data/dirty/ticks/raw_source/dukascopy/manifest.csv`. | done |
| 5 | Validation Report | Per-issue detail file: `date`, `issue`, `detail`, `severity`. Saved to `data/dirty/ticks/raw_source/dukascopy/validation_report.csv`. | done |
