# EUR/USD Data Pipeline

Short-term prediction work for the EUR/USD pair — structured as a tick → per-second → event pipeline.

**Repository:** [github.com/Rasul1908/EUR-USD_prediction](https://github.com/Rasul1908/EUR-USD_prediction)

**Raw tick Parquet files are not in git** (see `.gitignore`). Download and merge ticks locally with `notebooks/ingest_dukascopy.ipynb` (Dukascopy → daily Parquet → optional single combined file in one flow).

## Folder layout

- `data/dirty/ticks/raw_source/`
  - Raw per-tick data from the data source (expected to cover multiple years).
- `data/interim/per_second/`
  - Cleaned/transformed per-second data derived from the raw ticks.
- `data/processed/events/`
  - Eventized data derived from the per-second stream.
- `notebooks/`
  - Exploratory notebooks for ingest, cleaning, transformation, and event building.
- `src/eur_usd/`
  - Reusable Python code used by `main.py` and notebooks.

## Quick start

1. Install dependencies: `pip install -r requirements.txt`
2. Run the starter pipeline (dry-run by default): `python main.py`
3. Open `main.ipynb` to see the import/run wiring.

## Next steps

- Implement `ticks -> per_second` transformations in `src/eur_usd/pipeline/transform.py`
- Implement `per_second -> events` in the next stage
- Decide the schemas (columns, timestamp conventions, event definitions) once the raw data format is known
