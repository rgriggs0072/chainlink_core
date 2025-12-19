# utils/gap_history_helpers.py
"""
Gap History Helpers (Snapshots)

Page overview for future devs:
- This module writes weekly "gap snapshots" to two tables:
    1) GAP_REPORT_RUNS      (one row per tenant/week)
    2) GAP_REPORT_SNAPSHOT  (detail rows for that run)
- IMPORTANT:
    - Snapshots MUST store UPC and SR_UPC as DIGITS ONLY.
    - Excel/pandas often introduces ".0" artifacts (e.g. '850017944176.0').
    - We normalize UPC/SR_UPC immediately before write_pandas() so nothing
      reintroduces bad formats after earlier transforms.
"""

from __future__ import annotations

from typing import Optional, Union
import numpy as np
import pandas as pd
import datetime
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------------------------------------------------------
# UPC NORMALIZATION
# -----------------------------------------------------------------------------
def normalize_upc(value) -> Optional[str]:
    """
    Normalize UPC / SR_UPC values before snapshot writes.

    Why this exists:
    - Excel + pandas sometimes produce values like '850017944176.0'
    - These break streak logic and cross-week joins
    - Snapshots MUST store digit-only UPCs

    Rules:
    - Accept int, float, or string
    - Remove trailing '.0'
    - Strip all non-digit characters
    - Return None if empty/invalid
    """
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # Numeric paths (Excel / pandas)
    if isinstance(value, (int, np.integer)):
        s = str(int(value))

    elif isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return None
        # critical: removes ".0" and sci-notation artifacts
        s = str(int(round(value)))

    else:
        s = str(value).strip()

    if not s or s.lower() in ("nan", "none", "null"):
        return None

    # Kill string ".0" artifacts
    if s.endswith(".0"):
        s = s[:-2]

    # Digits only
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or None


# -----------------------------------------------------------------------------
# WEEK START
# -----------------------------------------------------------------------------
def get_week_start(d: pd.Timestamp) -> pd.Timestamp:
    """
    Normalize any given date to the Monday of that ISO week.
    Used to make weekly snapshots consistent.
    """
    return (d - pd.Timedelta(days=d.weekday())).normalize()


# -----------------------------------------------------------------------------
# SNAPSHOT WRITE
# -----------------------------------------------------------------------------
def save_gap_snapshot(
    conn,
    tenant_id: int,
    df_gaps: pd.DataFrame,
    snapshot_week_start: Optional[Union[pd.Timestamp, "datetime.date"]] = None,
    triggered_by: Optional[str] = None,
) -> bool:
    """
    Persist a weekly gap snapshot into:
      - GAP_REPORT_RUNS (header row)
      - GAP_REPORT_SNAPSHOT (detail rows)

    Returns True if inserted, False if skipped/failed.

    Skip rules:
    - If df_gaps is empty
    - If a run already exists for (TENANT_ID, SNAPSHOT_WEEK_START) (first-run-only)
    """
    # 0) Early exit
    if df_gaps is None or df_gaps.empty:
        return False

    # 1) Determine week start (as Python date)
    if snapshot_week_start is None:
        today = pd.Timestamp.utcnow().normalize()
        snapshot_week_start = get_week_start(today)

    if isinstance(snapshot_week_start, pd.Timestamp):
        snapshot_week_start_param = snapshot_week_start.to_pydatetime().date()
    else:
        snapshot_week_start_param = snapshot_week_start  # assume date/datetime

    # 2) Insert header row into GAP_REPORT_RUNS (if not already present)
    cur = conn.cursor()
    try:
        check_sql = """
            SELECT RUN_ID
            FROM GAP_REPORT_RUNS
            WHERE TENANT_ID = %s
              AND SNAPSHOT_WEEK_START = %s
            LIMIT 1
        """
        cur.execute(check_sql, (tenant_id, snapshot_week_start_param))
        existing = cur.fetchone()
        if existing is not None:
            # First-run-only rule: don't overwrite history
            return False

        row_count = int(len(df_gaps))

        insert_run_sql = """
            INSERT INTO GAP_REPORT_RUNS
                (TENANT_ID, SNAPSHOT_WEEK_START, TRIGGERED_BY, ROW_COUNT)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(
            insert_run_sql,
            (tenant_id, snapshot_week_start_param, triggered_by, row_count),
        )

        # Re-fetch RUN_ID (unique per TENANT_ID + SNAPSHOT_WEEK_START)
        cur.execute(
            """
            SELECT RUN_ID
            FROM GAP_REPORT_RUNS
            WHERE TENANT_ID = %s
              AND SNAPSHOT_WEEK_START = %s
            LIMIT 1
            """,
            (tenant_id, snapshot_week_start_param),
        )
        run_row = cur.fetchone()
        if not run_row:
            return False

        run_id = run_row[0]

    finally:
        try:
            cur.close()
        except Exception:
            pass

    # 3) Prepare DataFrame for GAP_REPORT_SNAPSHOT
    df_to_save = df_gaps.copy()

    # Required context columns
    df_to_save["TENANT_ID"] = tenant_id
    df_to_save["SNAPSHOT_WEEK_START"] = snapshot_week_start_param
    df_to_save["RUN_ID"] = run_id

    # Snapshot columns (slice to what exists)
    snapshot_cols = [
        "TENANT_ID",
        "SNAPSHOT_WEEK_START",
        "RUN_ID",
        "SALESPERSON_ID",
        "SALESPERSON_NAME",
        "MANAGER_ID",
        "MANAGER_NAME",
        "CHAIN_NAME",
        "STORE_NUMBER",
        "STORE_NAME",
        "PRODUCT_ID",
        "UPC",
        "SR_UPC",
        "PRODUCT_NAME",
        "SUPPLIER_NAME",
        "CATEGORY",
        "SUBCATEGORY",
        "GAP_CASES",
        "IN_SCHEMATIC",
        "IS_GAP",
        "LAST_PURCHASE_DATE",
    ]

    existing_cols = [c for c in snapshot_cols if c in df_to_save.columns]
    df_to_save = df_to_save[existing_cols]

    if df_to_save.empty:
        return False

    # --- HARD NORMALIZATION (DO NOT REMOVE) ---
    # This is the last possible moment before writing to Snowflake.
    # It prevents ".0" artifacts from entering the snapshot tables.
    if "UPC" in df_to_save.columns:
        df_to_save["UPC"] = df_to_save["UPC"].apply(normalize_upc)

    if "SR_UPC" in df_to_save.columns:
        df_to_save["SR_UPC"] = df_to_save["SR_UPC"].apply(normalize_upc)

    # Normalize BOOLEAN columns to real Python bools
    for col in ["IN_SCHEMATIC", "IS_GAP"]:
        if col in df_to_save.columns:
            df_to_save[col] = df_to_save[col].map(
                {
                    1: True,
                    0: False,
                    "1": True,
                    "0": False,
                    True: True,
                    False: False,
                }
            )

    # 4) Bulk insert into GAP_REPORT_SNAPSHOT
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_to_save,
        "GAP_REPORT_SNAPSHOT",
        quote_identifiers=False,
    )

    return bool(success)
