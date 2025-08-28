#!/usr/bin/env python3
"""
Build a tiny analytics warehouse on top of collected history:

- Converts daily CSVs -> partitioned Parquet:
    data/warehouse/system_load/date=YYYY-MM-DD/part-ISONE-YYYYMMDD.parquet
    data/warehouse/zonal_load/date=YYYY-MM-DD/part-ISONE-YYYYMMDD.parquet

- Creates/refreshes a DuckDB database with external views that point at those Parquet folders:
    warehouse/isone.duckdb
      views: system_load, zonal_load

Run:
    python warehouse_build.py

Idempotent: safe to re-run; only (re)writes partitions for dates that exist in history.
"""

import os
from pathlib import Path
import pandas as pd
import duckdb

# ---- Config (keep it simple) ----
HIST_DIR = Path("data/history")
WH_DIR   = Path("data/warehouse")
DB_PATH  = WH_DIR / "isone.duckdb"
ISO_CODE = "ISONE"   # future-proof: when you add PJM/CAISO, set per-source


def _ensure_dirs():
    (WH_DIR / "system_load").mkdir(parents=True, exist_ok=True)
    (WH_DIR / "zonal_load").mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _csvs(pattern: str):
    return sorted(HIST_DIR.glob(pattern))


def _write_parquet_partition(df: pd.DataFrame, subdir: str, date_str: str, file_tag: str):
    """
    Write one Parquet 'partition' folder:
      data/warehouse/<subdir>/date=<YYYY-MM-DD>/<filename>.parquet
    """
    part_dir = WH_DIR / subdir / f"date={date_str}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / f"part-{file_tag}-{date_str.replace('-','')}.parquet"
    # Overwrite atomically
    tmp = out_file.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(out_file)
    return out_file


def _coerce_system_types(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure stable schema; add iso column
    df = df.copy()
    df["iso"] = ISO_CODE
    # Expected columns from your pipeline:
    # ts_utc, ts_local, location, load_mw, is_system
    # (ts_* are strings in CSV; parse)
    df["ts_utc"]   = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df["ts_local"] = pd.to_datetime(df["ts_local"], errors="coerce")
    df["location"] = df["location"].astype(str)
    df["load_mw"]  = pd.to_numeric(df["load_mw"], errors="coerce")
    # Keep a minimal, consistent column order
    return df[["iso", "ts_utc", "ts_local", "location", "load_mw"]]


def _coerce_zonal_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["iso"] = ISO_CODE
    df["ts_utc"]   = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df["ts_local"] = pd.to_datetime(df["ts_local"], errors="coerce")
    # zone_id may be NaN when name-only; keep as string
    df["zone_id"]   = df["zone_id"].astype(str).where(df["zone_id"].notna(), None)
    df["zone_name"] = df["zone_name"].astype(str)
    df["load_mw"]   = pd.to_numeric(df["load_mw"], errors="coerce")
    return df[["iso", "ts_utc", "ts_local", "zone_id", "zone_name", "load_mw"]]


def _date_from_filename(p: Path) -> str:
    # history files are system_load_YYYY-MM-DD.csv / zonal_load_YYYY-MM-DD.csv
    stem = p.stem
    return stem.split("_")[-1]  # YYYY-MM-DD


def build_parquet_from_history():
    sys_csvs  = _csvs("system_load_*.csv")
    zon_csvs  = _csvs("zonal_load_*.csv")

    written = {"system": 0, "zonal": 0}
    for p in sys_csvs:
        date_str = _date_from_filename(p)
        df = pd.read_csv(p)
        df = _coerce_system_types(df)
        _write_parquet_partition(df, "system_load", date_str, file_tag=ISO_CODE)
        written["system"] += len(df)

    for p in zon_csvs:
        date_str = _date_from_filename(p)
        df = pd.read_csv(p)
        df = _coerce_zonal_types(df)
        _write_parquet_partition(df, "zonal_load", date_str, file_tag=ISO_CODE)
        written["zonal"] += len(df)

    return written


def create_duckdb_views():
    # Connect (creates file if not present)
    con = duckdb.connect(str(DB_PATH))

    # Use globbing against partitioned Parquet
    system_glob = str((WH_DIR / "system_load" / "date=*/part-*.parquet").as_posix())
    zonal_glob  = str((WH_DIR / "zonal_load"  / "date=*/part-*.parquet").as_posix())

    # Create or replace views so you never duplicate data inside the DB file
    con.execute(f"""
        CREATE OR REPLACE VIEW system_load AS
        SELECT * FROM read_parquet('{system_glob}');
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW zonal_load AS
        SELECT * FROM read_parquet('{zonal_glob}');
    """)

    # Tiny sanity: ensure the views are readable
    _ = con.execute("SELECT COUNT(*) FROM system_load").fetchone()
    _ = con.execute("SELECT COUNT(*) FROM zonal_load").fetchone()

    con.close()


def main():
    _ensure_dirs()
    wrote = build_parquet_from_history()
    print(f"[warehouse] wrote rows -> system: {wrote['system']} | zonal: {wrote['zonal']}")
    create_duckdb_views()
    print(f"[warehouse] duckdb ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
