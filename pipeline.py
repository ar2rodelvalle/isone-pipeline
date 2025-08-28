#!/usr/bin/env python3
"""
ISO-NE pipeline orchestrator

Examples:
  # Backfill 30 days and exit
  python pipeline.py --backfill 30 --once

  # Backfill 7 days, then start polling every 5 min
  python pipeline.py --backfill 7 --loop --interval-sec 300

  # Just start the poller (no backfill)
  python pipeline.py --loop
"""

import argparse
import os
import sys
import signal
from typing import Optional
from datetime import datetime, timezone

from dotenv import load_dotenv

# We reuse your existing modules:
import backfill_isone_days as backfill_mod
import poll_isone_to_history as poll_mod


def run_backfill(days: int, only: Optional[str] = None) -> None:
    """Backfill N past days. `only` can be 'system', 'zonal', or None (both)."""
    load_dotenv()
    # Use the same helpers already in backfill_isone_days.py but with optional filtering.
    from datetime import timedelta
    from pathlib import Path
    import pandas as pd

    username = os.getenv("ISONE_USERNAME")
    password = os.getenv("ISONE_PASSWORD")
    base_url = os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")

    today = datetime.now(timezone.utc).date()
    backfill_mod._ensure_dirs() if hasattr(backfill_mod, "_ensure_dirs") else poll_mod._ensure_dirs()

    for i in range(1, days + 1):
        day = today - timedelta(days=i)
        print(f"[pipeline][backfill] {day}…", flush=True)

        # SYSTEM
        added_sys = {}
        if only in (None, "system"):
            try:
                payload_sys = backfill_mod.fetch_system_day(username, password, base_url, day)
                df_sys = backfill_mod.sysload.normalize_payload(payload_sys)
                added_sys = poll_mod.write_history_system(df_sys)
            except Exception as e:
                print(f"[pipeline][{day}] System ERROR: {e}", file=sys.stderr)

        # ZONAL
        added_zone = {}
        if only in (None, "zonal"):
            try:
                payload_zone = backfill_mod.fetch_zonal_day(username, password, base_url, day)
                df_zone = backfill_mod.zoneload.normalize_payload(payload_zone)
                added_zone = poll_mod.write_history_zonal(df_zone)
            except Exception as e:
                print(f"[pipeline][{day}] Zonal ERROR: {e}", file=sys.stderr)

        print(f"[pipeline][{day}] Added → system: {added_sys} | zonal: {added_zone}", flush=True)


def run_once(only: Optional[str] = None) -> None:
    """One polling cycle, optionally limited to 'system' or 'zonal'."""
    poll_mod._ensure_dirs()
    # Fetch
    df_sys = df_zone = None
    if only in (None, "system"):
        df_sys = poll_mod.fetch_current_system_df()
    if only in (None, "zonal"):
        df_zone = poll_mod.fetch_current_zonal_df()
    # Stage latest snapshots
    from pathlib import Path
    if df_sys is not None:
        Path("data/staged/system_latest.csv").write_text(df_sys.to_csv(index=False))
        sys_added = poll_mod.write_history_system(df_sys)
        print(f"[{datetime.now(timezone.utc).isoformat()}] System rows added: {sys_added}")
    if df_zone is not None:
        Path("data/staged/zonal_latest.csv").write_text(df_zone.to_csv(index=False))
        zone_added = poll_mod.write_history_zonal(df_zone)
        print(f"[{datetime.now(timezone.utc).isoformat()}] Zonal rows added: {zone_added}")


def run_loop(interval_sec: int = 300, only: Optional[str] = None) -> None:
    """Continuous polling loop with clean shutdown."""
    stop = {"flag": False}

    def _sig_handler(signum, frame):
        stop["flag"] = True
        print("\n[pipeline] stopping…", flush=True)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    print(f"[pipeline] loop start {datetime.now(timezone.utc).isoformat()} (interval={interval_sec}s, UTC)")
    try:
        while not stop["flag"]:
            poll_mod.run_once(verbose=True) if only is None else run_once(only=only)
            if stop["flag"]:
                break
            poll_mod._sleep_until_next_interval(interval_sec)
    finally:
        print("[pipeline] loop ended.")


def main():
    ap = argparse.ArgumentParser(description="ISO-NE orchestrator: backfill + poller")
    ap.add_argument("--backfill", type=int, default=0, help="Backfill N days before polling (0 = skip)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="Run a single cycle (after backfill, if any)")
    mode.add_argument("--loop", action="store_true", help="Run indefinitely (after backfill, if any)")
    ap.add_argument("--interval-sec", type=int, default=300, help="Polling interval in seconds (default 300)")
    ap.add_argument("--only", choices=["system", "zonal"], help="Limit actions to system or zonal only")
    args = ap.parse_args()

    if args.backfill > 0:
        run_backfill(args.backfill, only=args.only)

    if args.once:
        run_once(only=args.only)
    else:
        run_loop(interval_sec=args.interval_sec, only=args.only)


if __name__ == "__main__":
    main()
 