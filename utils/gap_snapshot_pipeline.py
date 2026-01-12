# utils/gap_snapshot_pipeline.py
"""
Gap Snapshot Pipeline
---------------------

Page overview for future devs:
- Publishes tenant-wide weekly snapshots into:
    - GAP_REPORT_RUNS (header row per week)
    - GAP_REPORT_SNAPSHOT (detail rows)
- Snapshot is built from the UNFILTERED Excel gap report (create_gap_report(..., "All","All","All")).

Hard rules:
- NO Streamlit imports / st.session_state.
- This module is "pipeline" only:
    - fetch snapshot status
    - normalize UPCs
    - build snapshot DF
    - write snapshot rows
    - publish weekly snapshot (tenant-wide)

Dependencies:
- create_gap_report(conn, chain, supplier, salesperson) -> path to Excel file
- snowflake.connector.pandas_tools.write_pandas for bulk insert
"""

from __future__ import annotations

import os
from datetime import date
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# IMPORTANT: update this import path to wherever your create_gap_report lives
# Example:
# from utils.gap_report_builder import create_gap_report
from utils.gap_report_builder import create_gap_report



# -----------------------------------------------------------------------------
# Week start helper
# -----------------------------------------------------------------------------
def get_week_start(d: pd.Timestamp) -> pd.Timestamp:
    """Return Monday (ISO week start) for a given timestamp."""
    return (d - pd.Timedelta(days=d.weekday())).normalize()


# -----------------------------------------------------------------------------
# Snapshot status
# -----------------------------------------------------------------------------
def fetch_snapshot_status(conn, tenant_id: int) -> pd.DataFrame:
    """
    Pull latest runs for the tenant from GAP_REPORT_RUNS.

    Returns columns:
      SNAPSHOT_WEEK_START, RUN_AT, TRIGGERED_BY, ROW_COUNT
    """
    sql = """
        SELECT
            SNAPSHOT_WEEK_START,
            RUN_AT,
            TRIGGERED_BY,
            ROW_COUNT
        FROM GAP_REPORT_RUNS
        WHERE TENANT_ID = %s
        ORDER BY SNAPSHOT_WEEK_START DESC, RUN_AT DESC
        LIMIT 20
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (int(tenant_id),))
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


# -----------------------------------------------------------------------------
# UPC normalization
# -----------------------------------------------------------------------------
def normalize_upc(value) -> Optional[str]:
    """
    Normalize UPC / SR_UPC values before snapshot writes.

    Why:
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

    if isinstance(value, (int, np.integer)):
        s = str(int(value))
    elif isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return None
        s = str(int(round(value)))
    else:
        s = str(value).strip()

    if not s or s.lower() in ("nan", "none", "null"):
        return None

    if s.endswith(".0"):
        s = s[:-2]

    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or None


# -----------------------------------------------------------------------------
# Snapshot DF builder (from Excel gap report)
# -----------------------------------------------------------------------------
def build_snapshot_df_from_gap_report(df_gaps: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the Excel gap report DF into the snapshot DF we persist.

    Important:
    - Normalizes UPC and SR_UPC through normalize_upc() to prevent '.0' artifacts.
    - IS_GAP is TRUE when SR_UPC is missing/blank (meaning no sales).
    """
    snapshot_df = pd.DataFrame()

    snapshot_df["CHAIN_NAME"] = df_gaps.get("CHAIN_NAME")
    snapshot_df["STORE_NUMBER"] = df_gaps.get("STORE_NUMBER")
    snapshot_df["STORE_NAME"] = df_gaps.get("STORE_NAME")

    snapshot_df["SUPPLIER_NAME"] = df_gaps.get("SUPPLIER")
    snapshot_df["PRODUCT_NAME"] = df_gaps.get("PRODUCT_NAME")
    snapshot_df["SALESPERSON_NAME"] = df_gaps.get("SALESPERSON")

    # dg_upc -> UPC (schematic key)
    snapshot_df["UPC"] = (
        df_gaps["dg_upc"].apply(normalize_upc) if "dg_upc" in df_gaps.columns else None
    )

    # sr_upc -> SR_UPC (sales key)
    snapshot_df["SR_UPC"] = (
        df_gaps["sr_upc"].apply(normalize_upc) if "sr_upc" in df_gaps.columns else None
    )

    # IN_SCHEMATIC (best-effort)
    snapshot_df["IN_SCHEMATIC"] = df_gaps["In_Schematic"] if "In_Schematic" in df_gaps.columns else True

    sr = snapshot_df["SR_UPC"]
    snapshot_df["IS_GAP"] = sr.isna() | (sr.astype(str).str.strip() == "")

    snapshot_df["GAP_CASES"] = None
    snapshot_df["LAST_PURCHASE_DATE"] = None
    snapshot_df["CATEGORY"] = None
    snapshot_df["SUBCATEGORY"] = None

    return snapshot_df


# -----------------------------------------------------------------------------
# Snapshot write
# -----------------------------------------------------------------------------
def save_gap_snapshot(
    conn,
    tenant_id: int,
    df_gaps: pd.DataFrame,
    snapshot_week_start: Optional[Union[pd.Timestamp, date]] = None,
    triggered_by: Optional[str] = None,
) -> bool:
    """
    Persist a weekly gap snapshot into:
      - GAP_REPORT_RUNS (header row)
      - GAP_REPORT_SNAPSHOT (detail rows)

    Returns True if inserted, False if skipped.

    Skip rules:
    - If df_gaps is empty
    - If a run already exists for (TENANT_ID, SNAPSHOT_WEEK_START) (first-run-only)
    """
    if df_gaps is None or df_gaps.empty:
        return False

    if snapshot_week_start is None:
        snapshot_week_start = get_week_start(pd.Timestamp.utcnow().normalize())

    if isinstance(snapshot_week_start, pd.Timestamp):
        snapshot_week_start_param = snapshot_week_start.to_pydatetime().date()
    else:
        snapshot_week_start_param = snapshot_week_start

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT RUN_ID
            FROM GAP_REPORT_RUNS
            WHERE TENANT_ID = %s
              AND SNAPSHOT_WEEK_START = %s
            LIMIT 1
            """,
            (int(tenant_id), snapshot_week_start_param),
        )
        if cur.fetchone() is not None:
            return False  # first-run-only

        row_count = int(len(df_gaps))

        cur.execute(
            """
            INSERT INTO GAP_REPORT_RUNS
                (TENANT_ID, SNAPSHOT_WEEK_START, TRIGGERED_BY, ROW_COUNT)
            VALUES (%s, %s, %s, %s)
            """,
            (int(tenant_id), snapshot_week_start_param, triggered_by, row_count),
        )

        cur.execute(
            """
            SELECT RUN_ID
            FROM GAP_REPORT_RUNS
            WHERE TENANT_ID = %s
              AND SNAPSHOT_WEEK_START = %s
            LIMIT 1
            """,
            (int(tenant_id), snapshot_week_start_param),
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

    df_to_save = df_gaps.copy()
    df_to_save["TENANT_ID"] = int(tenant_id)
    df_to_save["SNAPSHOT_WEEK_START"] = snapshot_week_start_param
    df_to_save["RUN_ID"] = run_id

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
    df_to_save = df_to_save[[c for c in snapshot_cols if c in df_to_save.columns]]

    if df_to_save.empty:
        return False

    # hard normalize UPCs
    if "UPC" in df_to_save.columns:
        df_to_save["UPC"] = df_to_save["UPC"].apply(normalize_upc)
    if "SR_UPC" in df_to_save.columns:
        df_to_save["SR_UPC"] = df_to_save["SR_UPC"].apply(normalize_upc)

    # normalize booleans
    for col in ["IN_SCHEMATIC", "IS_GAP"]:
        if col in df_to_save.columns:
            df_to_save[col] = df_to_save[col].map(
                {1: True, 0: False, "1": True, "0": False, True: True, False: False}
            )

    success, _, _, _ = write_pandas(
        conn,
        df_to_save,
        "GAP_REPORT_SNAPSHOT",
        quote_identifiers=False,
    )
    return bool(success)


# -----------------------------------------------------------------------------
# Orchestrator: publish weekly snapshot (tenant-wide)
# -----------------------------------------------------------------------------
def publish_weekly_snapshot_all(
    conn,
    tenant_id: int,
    triggered_by: str = "gap_snapshot_pipeline",
) -> Tuple[bool, str]:
    """
    Publish tenant-wide weekly snapshot using UNFILTERED gap report generation.

    Returns (success, message).
    """
    snapshot_week_start = get_week_start(pd.Timestamp.utcnow().normalize())

    temp_file_path: Optional[str] = None
    try:
        temp_file_path = create_gap_report(conn, "All", "All", "All")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return False, "Report generation failed (no file returned)."

        df_gaps = pd.read_excel(temp_file_path, engine="openpyxl")
        if df_gaps is None or df_gaps.empty:
            return False, "Generated report was empty; nothing to snapshot."

        snapshot_df = build_snapshot_df_from_gap_report(df_gaps)

        saved = save_gap_snapshot(
            conn=conn,
            tenant_id=int(tenant_id),
            df_gaps=snapshot_df,
            snapshot_week_start=snapshot_week_start,
            triggered_by=triggered_by,
        )

        if saved:
            return True, "✅ Published weekly gap snapshot (tenant-wide)."
        return False, "Snapshot already exists for this week (skipped)."

    except Exception as e:
        return False, f"Publish failed: {e}"

    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception:
                pass
