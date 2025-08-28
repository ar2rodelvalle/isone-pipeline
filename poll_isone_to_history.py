#!/usr/bin/env python3
"""
Poll ISO-NE current 5-minute loads (system + zonal) and append to daily history CSVs
with idempotent keys.

- Reuses your existing fetchers by importing their functions.
- Writes to:
    data/history/system_load_YYYY-MM-DD.csv
    data/history/zonal_load_YYYY-MM-DD.csv
- Keys for idempotency:
    system: (ts_utc, location)
    zonal : (ts_utc, zone_id)

Usage:
    python poll_isone_to_history.py --once
    python poll_isone_to_history.py --loop --interval-sec 300
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv

# Import your existing fetchers safely (they use if __name__ == "__main__":)
import fetch_isone_fivemin_load as sysload
import fetch_isone_fivemin_zonal_load as zoneload


def _ensure_dirs():
    Path("data/history").mkdir(parents=True, exist_ok=True)
    Path("data/staged").mkdir(parents=True, exist_ok=True)
    Path("data/raw").mkdir(parents=True, exist_ok=True)


def _utc_dates_in_df(df: pd.DataFrame, ts_col: str) -> Iterable[str]:
    """Return a set of YYYY-MM-DD (UTC) strings present in df[ts_col]."""
    return set(pd.to_datetime(df[ts_col], utc=True).dt.date.astype(str).tolist())


def _append_idempotent(df_new: pd.DataFrame, path: Path, key_cols: list[str]) -> int:
    """
    Append df_new to CSV at 'path' without duplicates on key_cols.
    Returns number of rows written after de-dupe (could be 0).
    """
    if path.exists():
        try:
            df_old = pd.read_csv(path, parse_dates=["ts_utc", "ts_local"])
        except Exception:
            # Fallback if previous file had no tz parsing
            df_old = pd.read_csv(path)
    else:
        df_old = pd.DataFrame(columns=df_new.columns)

    # Combine → drop duplicates → sort by ts_utc, then write atomically
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=key_cols).sort_values("ts_utc").reset_index(drop=True)
    after = len(df_all)

    tmp = path.with_suffix(path.suffix + ".tmp")
    df_all.to_csv(tmp, index=False)
    tmp.replace(path)

    # rows newly added
    return after - len(df_old)


def fetch_current_system_df() -> pd.DataFrame:
    """
    Call system fetcher & normalizer to return a tidy DataFrame with columns:
    ts_utc, ts_local, location, load_mw, is_system
    """
    username = os.getenv("ISONE_USERNAME")
    password = os.getenv("ISONE_PASSWORD")
    base_url = os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")

    payload = sysload.fetch_current_fivemin_load(username, password, base_url)
    df = sysload.normalize_payload(payload)
    if df.empty:
        raise RuntimeError("System DF empty")
    return df


def fetch_current_zonal_df() -> pd.DataFrame:
    """
    Call zonal fetcher & normalizer to return a tidy DataFrame with columns:
    ts_utc, ts_local, zone_id, zone_name, load_mw
    """
    username = os.getenv("ISONE_USERNAME")
    password = os.getenv("ISONE_PASSWORD")
    base_url = os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")

    payload = zoneload.fetch_current_zonal(username, password, base_url)
    df = zoneload.normalize_payload(payload)
    if df.empty:
        raise RuntimeError("Zonal DF empty")
    return df


def write_history_system(df_sys: pd.DataFrame) -> dict:
    """
    Write system rows into per-day (UTC) files, idempotently.
    Returns summary: {date_str: added_rows}
    """
    out = {}
    for date_str in _utc_dates_in_df(df_sys, "ts_utc"):
        df_day = df_sys[pd.to_datetime(df_sys["ts_utc"], utc=True).dt.date.astype(str) == date_str]
        path = Path("data/history") / f"system_load_{date_str}.csv"
        added = _append_idempotent(df_day, path, key_cols=["ts_utc", "location"])
        out[date_str] = added
    return out


def write_history_zonal(df_zone: pd.DataFrame) -> dict:
    """
    Write zonal rows into per-day (UTC) files, idempotently.
    Returns summary: {date_str: added_rows}
    """
    out = {}
    for date_str in _utc_dates_in_df(df_zone, "ts_utc"):
        df_day = df_zone[pd.to_datetime(df_zone["ts_utc"], utc=True).dt.date.astype(str) == date_str]
        path = Path("data/history") / f"zonal_load_{date_str}.csv"
        added = _append_idempotent(df_day, path, key_cols=["ts_utc", "zone_id"])
        out[date_str] = added
    return out


def run_once(verbose: bool = True) -> None:
    _ensure_dirs()
    load_dotenv()

    # Fetch
    df_sys = fetch_current_system_df()
    df_zone = fetch_current_zonal_df()

    # Persist to staged (latest snapshots) for quick inspection
    Path("data/staged/system_latest.csv").write_text(df_sys.to_csv(index=False))
    Path("data/staged/zonal_latest.csv").write_text(df_zone.to_csv(index=False))

    # Append to history
    sys_added = write_history_system(df_sys)
    zone_added = write_history_zonal(df_zone)

    if verbose:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] System rows added per day (UTC): {sys_added}")
        print(f"[{now}] Zonal  rows added per day (UTC): {zone_added}")


def _sleep_until_next_interval(interval_sec: int) -> None:
    """Sleep so we wake close to the next multiple of interval_sec."""
    now = time.time()
    next_tick = (int(now) // interval_sec + 1) * interval_sec
    time.sleep(max(0, next_tick - now))


def run_loop(interval_sec: int = 300) -> None:
    print(f"[poll] starting loop at {datetime.now(timezone.utc).isoformat()} (interval={interval_sec}s, UTC)")
    try:
        while True:
            start = time.time()
            try:
                run_once(verbose=True)
            except Exception as e:
                # don't crash the loop on transient errors
                print(f"[poll][ERROR] {type(e).__name__}: {e}", file=sys.stderr)
            # sleep aligned to the next interval
            elapsed = time.time() - start
            # Aim to align to 5-min boundaries; use helper for clean alignment
            if elapsed < interval_sec:
                _sleep_until_next_interval(interval_sec)
    except KeyboardInterrupt:
        print("\n[poll] stopped by user")


def main():
    ap = argparse.ArgumentParser(description="Append ISO-NE 5-min loads to daily history (idempotent)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    mode.add_argument("--loop", action="store_true", help="Run indefinitely")
    ap.add_argument("--interval-sec", type=int, default=300, help="Polling interval seconds (default 300)")
    args = ap.parse_args()

    if args.once:
        run_once(verbose=True)
    else:
        run_loop(interval_sec=args.interval_sec)


if __name__ == "__main__":
    main()
