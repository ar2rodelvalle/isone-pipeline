#!/usr/bin/env python3
"""
Backfill N past days of ISO-NE 5-min loads (system + zonal).

Usage:
    python backfill_isone_days.py --days 30

- Calls ISO-NE day endpoints for each day.
- Normalizes via existing system/zonal normalizers.
- Appends into history CSVs (idempotent keys).
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import pandas as pd
from dotenv import load_dotenv

import fetch_isone_fivemin_load as sysload
import fetch_isone_fivemin_zonal_load as zoneload
from poll_isone_to_history import write_history_system, write_history_zonal, _ensure_dirs


def fetch_system_day(username: str, password: str, base_url: str, day: datetime) -> dict:
    ymd = day.strftime("%Y%m%d")
    url = f"{base_url}/fiveminutesystemload/day/{ymd}.json"
    resp = sysload.requests.get(url, headers={"Accept": "application/json"},
                                auth=(username, password), timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_zonal_day(username: str, password: str, base_url: str, day: datetime) -> dict:
    ymd = day.strftime("%Y%m%d")
    url = f"{base_url}/fiveminuteestimatedzonalload/day/{ymd}.json"
    resp = zoneload.requests.get(url, headers={"Accept": "application/json"},
                                 auth=(username, password), timeout=60)
    resp.raise_for_status()
    return resp.json()


def backfill_days(n_days: int) -> None:
    _ensure_dirs()
    load_dotenv()

    username = os.getenv("ISONE_USERNAME")
    password = os.getenv("ISONE_PASSWORD")
    base_url = os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")

    today = datetime.now(timezone.utc).date()

    for i in range(1, n_days + 1):
        day = today - timedelta(days=i)
        print(f"[backfill] Fetching {day} …", flush=True)

        # SYSTEM
        try:
            payload_sys = fetch_system_day(username, password, base_url, day)
            df_sys = sysload.normalize_payload(payload_sys)
            added_sys = write_history_system(df_sys)
        except Exception as e:
            print(f"[backfill][{day}] System ERROR: {e}", file=sys.stderr)
            added_sys = {}

        # ZONAL
        try:
            payload_zone = fetch_zonal_day(username, password, base_url, day)
            df_zone = zoneload.normalize_payload(payload_zone)
            added_zone = write_history_zonal(df_zone)
        except Exception as e:
            print(f"[backfill][{day}] Zonal ERROR: {e}", file=sys.stderr)
            added_zone = {}

        print(f"[backfill][{day}] Added rows → system: {added_sys} | zonal: {added_zone}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Backfill past N days of ISO-NE 5-min loads")
    ap.add_argument("--days", type=int, required=True, help="Number of past days to fetch (e.g., 30)")
    args = ap.parse_args()
    backfill_days(args.days)


if __name__ == "__main__":
    main()
