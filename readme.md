Perfect 👍 — let’s wrap everything into a clear **manual** you (or a junior analyst) can follow end-to-end.
This is a *single document* that describes setup, daily use, and troubleshooting.

---

# ISO-NE 5-Minute Load Data Pipeline & Dashboard

**Manual for Automatic Data Pull + Visualization**

---

## 1. Project Overview

This system automatically collects ISO-NE 5-minute load data (system + zonal), stores it safely, and provides a simple dashboard for analysis.

**Main components:**

* **Fetchers** (`fetch_isone_fivemin_load.py`, `fetch_isone_fivemin_zonal_load.py`) – get latest interval.
* **Poller** (`poll_isone_to_history.py`) – appends new rows every 5 minutes (idempotent).
* **Backfill** (`backfill_isone_days.py`) – fetches full days of history.
* **Warehouse** (`warehouse_build.py`) – converts CSV history → Parquet + DuckDB views.
* **Dashboard** (`app_streamlit.py`) – browser app with charts and parity checks.

---

## 2. Setup (one-time)

1. **Enter project folder & activate venv**

   ```bash
   cd /path/to/isone-pipeline
   source .venv/bin/activate
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials**
   Edit `.env` in project root:

   ```
   ISONE_USERNAME=your_webservices_username
   ISONE_PASSWORD=your_webservices_password
   ISONE_BASE_URL=https://webservices.iso-ne.com/api/v1.1
   ```

---

## 3. Data Collection

### A) Backfill N past days

To seed history (e.g., last 30 days):

```bash
python backfill_isone_days.py --days 30
```

→ Creates per-day history files in `data/history/`.

### B) Start continuous polling

To keep pulling new intervals:

```bash
python poll_isone_to_history.py --loop --interval-sec 300
```

* Runs forever until `Ctrl+C`.
* Every 5 minutes: adds **1 system row + 8 zonal rows**.
* Data stored in `data/history/system_load_YYYY-MM-DD.csv` and `…/zonal_load_YYYY-MM-DD.csv`.

*(Tip: use `--once` for testing, or run under `cron`/`systemd` for automation.)*

---

## 4. Warehouse for Analysis

Convert history CSVs → Parquet + DuckDB:

```bash
python warehouse_build.py
```

→ Creates `data/warehouse/` with:

* Partitioned Parquet (by date)
* DuckDB database: `data/warehouse/isone.duckdb`

  * `system_load` view
  * `zonal_load` view

---

## 5. Visualization Dashboard

Launch Streamlit dashboard:

```bash
python -m streamlit run app_streamlit.py
```

Then open [http://localhost:8501](http://localhost:8501).

**Features:**

* **Now Panel**: system vs. zonal parity, latest timestamp.
* **Zonal Bar**: latest 8 zones side-by-side.
* **Per-Zone Sparklines**: 24h (adjustable to 1–168h).
* **Heatmap**: hour × weekday patterns (last 28d).

Refresh the browser to see the newest data.

---

## 6. Quick SQL Checks

Run ad-hoc queries against DuckDB:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("data/warehouse/isone.duckdb", read_only=True)
print(con.execute("SELECT MAX(ts_utc) FROM system_load").df())
print(con.execute("""
  SELECT zone_name, AVG(load_mw) AS avg_last_6h
  FROM zonal_load
  WHERE ts_utc > now() - INTERVAL 6 HOUR
  GROUP BY 1 ORDER BY avg_last_6h DESC
""").df())
con.close()
PY
```

---

## 7. Troubleshooting

* **No new rows in poller** → ISO-NE publishes every 5 min; wait for next boundary.
* **Duplicates** → safe: poller de-dupes on `(ts_utc, zone_id/location)`.
* **Dashboard blank/error** → check `warehouse_build.py` has been run and `isone.duckdb` exists.
* **Credential errors** → confirm `.env` has valid ISO-NE Web Services username & password.
* **Want faster refresh** → run dashboard with shorter TTL:

  ```bash
  DASH_REFRESH_SEC=30 python -m streamlit run app_streamlit.py
  ```

---

## 8. Typical Workflow

1. **First run (new environment):**

   ```bash
   source .venv/bin/activate
   python backfill_isone_days.py --days 30
   python poll_isone_to_history.py --loop
   # (in another terminal)
   python warehouse_build.py
   python -m streamlit run app_streamlit.py
   ```

2. **Daily ops (for junior analyst):**

   ```bash
   source .venv/bin/activate
   python warehouse_build.py
   python -m streamlit run app_streamlit.py
   ```

   → Explore data in dashboard.

---

✅ That’s the full manual: from credentials to dashboard.

Would you like me to also produce a **1-page “cheat sheet” PDF** (with the key commands only, no explanations) so a junior analyst can print & pin it on their desk?
