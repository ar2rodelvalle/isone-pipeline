#!/usr/bin/env python3
"""
Fetch the current five-minute **estimated zonal load** for ALL zones from ISO-NE,
normalize, and write a tidy CSV.

Design intent:
- Single responsibility: one endpoint -> one normalized CSV.
- Observed near-RT data (no forecasts).
- Robust normalizer (handles minor field/shape variations).
- Idempotent & testable; saves raw snapshot and a stable staged CSV.

Docs:
- Base URL: https://webservices.iso-ne.com/api/v1.1
- Endpoint:  /fiveminuteestimatedzonalload/current  (JSON supported) 
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv


ZONE_NAME_BY_ID = {
    # Helpful human labels; used if payload only includes numeric IDs
    "4001": "ME",
    "4002": "NH",
    "4003": "VT",
    "4004": "CT",
    "4005": "RI",
    "4006": "SEMA",
    "4007": "WCMA",
    "4008": "NEMA/Boston",
    "4000": "HUB",  # not a load zone, but appears in some contexts
}


def env_or_fail(key: str) -> str:
    v = os.getenv(key)
    if not v:
        print(f"ERROR: Missing required env var {key}", file=sys.stderr)
        sys.exit(2)
    return v


def get_base_url() -> str:
    return os.getenv("ISONE_BASE_URL", "https://webservices.iso-ne.com/api/v1.1").rstrip("/")


def fetch_current_zonal(username: str, password: str, base_url: str) -> dict:
    """
    Call /fiveminuteestimatedzonalload/current.json (force JSON), validate,
    and return parsed JSON. Save raw bytes for debugging.
    """
    url = f"{base_url}/fiveminuteestimatedzonalload/current.json"
    headers = {"Accept": "application/json"}
    resp = requests.get(url, headers=headers, auth=(username, password), timeout=30)

    # Save raw for traceability
    dbg_dir = Path("data/raw"); dbg_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = dbg_dir / f"isone_fivemin_zonal_raw_{stamp}.bin"
    raw_path.write_bytes(resp.content)

    # Raise HTTP errors
    resp.raise_for_status()

    # Content-Type must be JSON
    ctype = resp.headers.get("Content-Type", "")
    if "json" not in ctype.lower():
        bad = dbg_dir / f"isone_fivemin_zonal_unexpected_{stamp}.txt"
        bad.write_bytes(resp.content)
        raise RuntimeError(f"Unexpected Content-Type '{ctype}' from {url}. Saved to {bad}")

    try:
        payload = resp.json()
    except ValueError as e:
        raise RuntimeError(f"Invalid JSON from {url}; see {raw_path}") from e

    return payload


def _first(d: dict, *keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _extract_zone_text(v) -> Optional[str]:
    """
    Location/zone can be a string, an object with a text field, or a numeric id.
    """
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    if isinstance(v, (int, float)):
        return str(int(v))
    if isinstance(v, dict):
        # try common text holders
        for tk in ("#text", "$", "text", "_text", "_value", "value", "name", "LocName", "LocShortName", "LoadZone", "Zone"):
            s = v.get(tk)
            if isinstance(s, str) and s.strip():
                return s.strip()
            if isinstance(s, (int, float)):
                return str(int(s))
        # last resort: any stringy member
        for tk, tv in v.items():
            if isinstance(tv, str) and tv.strip():
                return tv.strip()
    return str(v)


def normalize_payload(payload: dict) -> pd.DataFrame:
    """
    Normalize ISO-NE Five-Minute Estimated Zonal Load into:
      ts_utc, ts_local, zone_id, zone_name, load_mw

    Supports shapes with keys like:
      interval_begin_date, load_zone_id, load_zone_name, estimated_load_mw
    and falls back to older variants.
    """
    # exact keys we saw on your account
    HARD_TIME   = ("interval_begin_date",)
    HARD_ZONEID = ("load_zone_id",)
    HARD_ZONENM = ("load_zone_name",)
    HARD_VALUE  = ("estimated_load_mw",)

    # additional fallbacks seen in docs/variants
    TIME_KEYS   = HARD_TIME   + ("BeginDate", "BeginDateTime", "BeginDateUTC", "BeginDatetime", "StartTime")
    ZONEID_KEYS = HARD_ZONEID + ("LoadZone", "LoadZoneId", "LoadZoneID", "ZoneID", "ZoneId")
    ZONENM_KEYS = HARD_ZONENM + ("Zone", "LocName", "Location", "LoadZoneName")
    VALUE_KEYS  = HARD_VALUE  + ("LoadMw", "LoadMW", "Load", "Value", "estimated_zonal_load_mw")

    def first_key(d: dict, keys: tuple) -> Optional[any]:
        for k in keys:
            if k in d and d[k] not in (None, ""):
                return d[k]
        return None

    def clean_zone_name(z: Optional[str]) -> Optional[str]:
        if z is None:
            return None
        s = str(z).strip()
        # ISO-NE often prefixes .Z.  e.g., ".Z.MAINE"
        if s.startswith(".Z."):
            s = s[3:]
        # normalize spaces
        return s

    ZONE_NAME_BY_ID = {
        "4001": "ME",
        "4002": "NH",
        "4003": "VT",
        "4004": "CT",
        "4005": "RI",
        "4006": "SEMA",
        "4007": "WCMA",
        "4008": "NEMA/Boston",
        "4000": "HUB",
    }
    REV_BY_NAME = {v.upper(): k for k, v in ZONE_NAME_BY_ID.items()}

    rows = []

    # Walk entire payload; pick records that have time + value + zone id/name
    def walk(obj):
        if isinstance(obj, dict):
            t   = first_key(obj, TIME_KEYS)
            val = first_key(obj, VALUE_KEYS)
            zid = first_key(obj, ZONEID_KEYS)
            znm = first_key(obj, ZONENM_KEYS)

            if t is not None and val is not None and (zid is not None or znm is not None):
                ts_local = pd.to_datetime(t, utc=False, errors="coerce")
                ts_utc = (
                    ts_local.tz_convert("UTC")
                    if (ts_local is not pd.NaT and ts_local.tzinfo)
                    else (ts_local.tz_localize("UTC") if ts_local is not pd.NaT else pd.NaT)
                )

                # zone id → string
                zid_s = None
                if isinstance(zid, (int, float)):
                    zid_s = str(int(zid))
                elif isinstance(zid, str) and zid.strip():
                    zid_s = zid.strip() if zid.strip().isdigit() else None

                # zone name → cleaned
                znm_s = clean_zone_name(znm) if znm is not None else None

                # If missing id, try reverse map from name; if missing name, map from id
                if zid_s is None and isinstance(znm_s, str):
                    zid_s = REV_BY_NAME.get(znm_s.upper())
                if znm_s is None and zid_s is not None:
                    znm_s = ZONE_NAME_BY_ID.get(zid_s, znm_s)

                # value → float
                try:
                    load_f = float(val)
                except (TypeError, ValueError):
                    load_f = None

                rows.append(
                    {
                        "ts_utc": ts_utc,
                        "ts_local": ts_local,
                        "zone_id": zid_s,
                        "zone_name": znm_s,
                        "load_mw": load_f,
                    }
                )

            # recurse
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(payload)

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        debug_path = Path("data/raw") / "unexpected_shape_zonal.json"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w") as f:
            json.dump(payload, f, indent=2)
        raise ValueError(f"No zonal rows extracted; saved raw JSON to {debug_path}")

    # Final tidy
    df["zone_name"] = df["zone_name"].fillna(df["zone_id"])
    df = df.sort_values(["ts_utc", "zone_id", "zone_name"]).reset_index(drop=True)
    return df



def main():
    load_dotenv()
    username = env_or_fail("ISONE_USERNAME")
    password = env_or_fail("ISONE_PASSWORD")
    base_url = get_base_url()

    out_raw = Path("data/raw"); out_raw.mkdir(parents=True, exist_ok=True)
    out_stage = Path("data/staged"); out_stage.mkdir(parents=True, exist_ok=True)

    payload = fetch_current_zonal(username, password, base_url)

    # Save a human-readable raw snapshot too
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_json_path = out_raw / f"isone_fivemin_zonal_{ts}.json"
    with raw_json_path.open("w") as f:
        json.dump(payload, f)

    df = normalize_payload(payload)
    csv_path = out_stage / "isone_fivemin_zonal_latest.csv"
    df.to_csv(csv_path, index=False)

    # Friendly summary
    zones = sorted(set(df["zone_name"].astype(str)))
    print(f"[OK] Wrote raw:   {raw_json_path}")
    print(f"[OK] Wrote CSV:   {csv_path}")
    print(f"[OK] Records:     {len(df)} zones in this interval")
    print(f"[OK] Zones:       {', '.join(zones)}")
    if not df.empty:
        print(df.head(min(10, len(df))).to_string(index=False))


if __name__ == "__main__":
    main()
