#!/usr/bin/env python3
"""
Fetch current five-minute system load for ALL locations from ISO-NE Web Services,
normalize, and write a tidy CSV.

Why this script exists (design intent):
- Single-responsibility: one endpoint → one normalized CSV.
- Fine granularity: 5-minute instantaneous values, all locations (system + zones).
- Idempotent & testable: safe to re-run; file names include timestamps; prints a summary.
- Minimal deps; no premature abstractions.

Docs:
- Base URL: https://webservices.iso-ne.com/api/v1.1
- Endpoint:  /fiveminutesystemload/current (returns all locations)
"""

import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


def env_or_fail(key: str) -> str:
    v = os.getenv(key)
    if not v:
        print(f"ERROR: Missing required env var {key}", file=sys.stderr)
        sys.exit(2)
    return v


def get_base_url() -> str:
    return os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")


def fetch_current_fivemin_load(username: str, password: str, base_url: str) -> dict:
    """
    Calls /fiveminutesystemload/current.json (force JSON via URL extension),
    validates Content-Type, and saves unexpected payloads for debugging.
    """
    from pathlib import Path
    import json
    import time

    url = f"{base_url}/fiveminutesystemload/current.json"  # force JSON format
    headers = {"Accept": "application/json"}  # belt-and-suspenders
    resp = requests.get(url, headers=headers, auth=(username, password), timeout=30)

    # Save raw bytes for debugging regardless (rotating filename)
    dbg_dir = Path("data/raw"); dbg_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    raw_path = dbg_dir / f"fivemin_current_raw_{stamp}.bin"
    raw_path.write_bytes(resp.content)

    # Raise HTTP errors early (401/403/etc.)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"HTTP {resp.status_code} from ISO-NE. "
            f"Saved raw to {raw_path}. URL={url}"
        ) from e

    # Enforce JSON content-type (some proxies return text/html)
    ctype = resp.headers.get("Content-Type", "")
    if "json" not in ctype.lower():
        txt_path = dbg_dir / f"fivemin_current_unexpected_{stamp}.txt"
        txt_path.write_bytes(resp.content)
        raise RuntimeError(
            f"Unexpected Content-Type '{ctype}'. Likely HTML/XML or a login page. "
            f"Saved to {txt_path}. URL={url}"
        )

    # Parse JSON and catch non-data messages
    try:
        payload = resp.json()
    except ValueError as e:
        raise RuntimeError(
            f"Invalid JSON in response. Saved raw to {raw_path}. URL={url}"
        ) from e

    # Look for error-shaped responses
    if isinstance(payload, dict):
        for k in ("Error", "error", "Message", "message"):
            if k in payload:
                err_path = dbg_dir / f"fivemin_current_error_{stamp}.json"
                err_path.write_text(json.dumps(payload, indent=2))
                raise RuntimeError(
                    f"API returned an error object under '{k}'. "
                    f"Saved to {err_path}. URL={url}"
                )

    return payload

def normalize_payload(payload: dict) -> pd.DataFrame:
    """
    Normalizes Five-Minute System Load JSON into:
      ts_utc, ts_local, location, load_mw, is_system
    Handles system-only shape:
      {"FiveMinSystemLoad": [ { "BeginDate": "...", "LoadMw": ... , ... }, ... ]}
    And remains robust to other shapes.
    """
    TIME_KEYS  = ("BeginDate", "BeginDateTime", "BeginDateUTC", "BeginDatetime")
    VALUE_KEYS = ("LoadMw", "LoadMW", "Load", "Value")  # include 'LoadMw'
    LOC_KEYS   = ("Location", "Loc", "Zone", "LocName")

    def extract_time(d):
        for k in TIME_KEYS:
            if k in d and d[k]:
                return d[k]
        return None

    def extract_val(d):
        for k in VALUE_KEYS:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return None

    def extract_loc(d):
        # If an explicit location exists, use it; else default to system "ISONE"
        for k in LOC_KEYS:
            if k in d and d[k]:
                v = d[k]
                if isinstance(v, dict):
                    for tk in ("#text", "$", "text", "_text", "_value", "value", "name", "LocName", "LocShortName"):
                        if tk in v and isinstance(v[tk], str) and v[tk].strip():
                            return v[tk].strip()
                    for tk, tv in v.items():
                        if isinstance(tv, str) and tv.strip():
                            return tv.strip()
                    return str(v)
                return str(v)
        return "ISONE"  # system default

    rows = []

    # Fast-path: system-only shape
    if isinstance(payload, dict) and isinstance(payload.get("FiveMinSystemLoad"), list):
        for r in payload["FiveMinSystemLoad"]:
            if not isinstance(r, dict):
                continue
            tval = extract_time(r)
            lval = extract_val(r)
            if tval is None or lval is None:
                continue
            ts_local = pd.to_datetime(tval, utc=False, errors="coerce")
            ts_utc = (
                ts_local.tz_convert("UTC")
                if (ts_local is not pd.NaT and ts_local.tzinfo)
                else (ts_local.tz_localize("UTC") if ts_local is not pd.NaT else pd.NaT)
            )
            try:
                load_f = float(lval)
            except (TypeError, ValueError):
                load_f = None
            loc = "ISONE"
            rows.append(
                {
                    "ts_utc": ts_utc,
                    "ts_local": ts_local,
                    "location": loc,
                    "load_mw": load_f,
                    "is_system": True,
                }
            )

    # Fallback: recursive walk for other shapes
    def walk(obj):
        if isinstance(obj, dict):
            tval = extract_time(obj)
            lval = extract_val(obj)
            if tval is not None and lval is not None:
                ts_local = pd.to_datetime(tval, utc=False, errors="coerce")
                ts_utc = (
                    ts_local.tz_convert("UTC")
                    if (ts_local is not pd.NaT and ts_local.tzinfo)
                    else (ts_local.tz_localize("UTC") if ts_local is not pd.NaT else pd.NaT)
                )
                try:
                    load_f = float(lval)
                except (TypeError, ValueError):
                    load_f = None
                loc = extract_loc(obj) or "ISONE"
                rows.append(
                    {
                        "ts_utc": ts_utc,
                        "ts_local": ts_local,
                        "location": loc,
                        "load_mw": load_f,
                        "is_system": loc.upper() in {"ISONE", "NEPOOL", "NEWENGLAND", "NE"},
                    }
                )
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    if not rows:
        walk(payload)

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        debug_path = Path("data/raw") / "unexpected_shape_debug.json"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w") as f:
            json.dump(payload, f, indent=2)
        raise ValueError(
            f"No rows extracted from payload. Saved raw JSON to {debug_path} for inspection."
        )
    df = df.sort_values(["ts_utc", "location"]).reset_index(drop=True)
    return df


def main():
    load_dotenv()
    username = env_or_fail("ISONE_USERNAME")
    password = env_or_fail("ISONE_PASSWORD")
    base_url = get_base_url()

    # Output folders
    out_raw = Path("data/raw")
    out_stage = Path("data/staged")
    out_raw.mkdir(parents=True, exist_ok=True)
    out_stage.mkdir(parents=True, exist_ok=True)

    # Fetch
    payload = fetch_current_fivemin_load(username, password, base_url)

    # Save raw snapshot (JSON) with timestamp for traceability
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = out_raw / f"isone_fivemin_load_{ts}.json"
    with raw_path.open("w") as f:
        json.dump(payload, f)

    # Normalize → CSV
    df = normalize_payload(payload)
    csv_path = out_stage / "isone_fivemin_load_latest.csv"
    df.to_csv(csv_path, index=False)

    # Friendly summary
    n_total = len(df)
    n_system = int(df["is_system"].sum())
    n_zones = n_total - n_system
    ts_local = df["ts_local"].iloc[0] if n_total else "n/a"

    print(f"[OK] Wrote raw:   {raw_path}")
    print(f"[OK] Wrote CSV:   {csv_path}")
    print(f"[OK] Records:     {n_total} (system={n_system}, zones={n_zones})")
    print(f"[OK] Interval:    {ts_local} (local)")
    if n_total:
        print(df.head(min(8, n_total)).to_string(index=False))


if __name__ == "__main__":
    main()
