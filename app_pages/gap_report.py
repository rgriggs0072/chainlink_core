# ---------- app_pages/gap_report.py ----------

# -*- coding: utf-8 -*-
"""
Gap Report Generator Page

Overview (for future devs)
--------------------------
This page generates the Excel Gap Report and optionally writes a weekly history snapshot.

Flow:
1) Load tenant-scoped Snowflake connection from st.session_state["conn"]
2) Render filters (Salesperson, Chain, Supplier) inside a form
3) On submit: generate Excel report via create_gap_report()
4) Read generated Excel back into a DataFrame
5) Build a snapshot_df (lightweight schema aligned to GAP_REPORT_SNAPSHOT)
6) save_gap_snapshot() writes:
   - GAP_REPORT_RUNS (header)
   - GAP_REPORT_SNAPSHOT (detail)
   First-run-only per (TENANT_ID, SNAPSHOT_WEEK_START).

Key rule:
- UPC fields must be normalized BEFORE writing snapshots:
  Excel/pandas can produce values like "850017944176.0" which break joins/streaks.
"""

import os
from datetime import datetime

import pandas as pd
import streamlit as st

from utils.reports_utils import create_gap_report
from utils.snowflake_utils import fetch_distinct_values
from utils.gap_history_helpers import normalize_upc


# -----------------------------
# Helpers
# -----------------------------
def _get_tenant_id(tenant_config) -> int | None:
    """
    Safely extract tenant_id from tenant_config which may be dict or object.
    """
    if tenant_config is None:
        return None
    if isinstance(tenant_config, dict):
        return tenant_config.get("tenant_id")
    return getattr(tenant_config, "tenant_id", None)


# def _build_snapshot_df(df_gaps: pd.DataFrame) -> pd.DataFrame:
#     """
#     Build a snapshot dataframe aligned to GAP_REPORT_SNAPSHOT expectations.

#     Notes:
#     - We intentionally normalize dg_upc and sr_upc using normalize_upc()
#       to permanently prevent ".0" artifacts.
#     - IS_GAP is computed once:
#         IS_GAP = (UPC exists) AND (SR_UPC missing)
#       This is the safest definition for your gap history logic.
#     """
#     snapshot_df = pd.DataFrame()

#     # Store / chain
#     snapshot_df["CHAIN_NAME"] = df_gaps.get("CHAIN_NAME")
#     snapshot_df["STORE_NUMBER"] = df_gaps.get("STORE_NUMBER")
#     snapshot_df["STORE_NAME"] = df_gaps.get("STORE_NAME")

#     # Product / supplier
#     snapshot_df["SUPPLIER_NAME"] = df_gaps.get("SUPPLIER")
#     snapshot_df["PRODUCT_NAME"] = df_gaps.get("PRODUCT_NAME")

#     # Salesperson
#     snapshot_df["SALESPERSON_NAME"] = df_gaps.get("SALESPERSON")

#     # -----------------------------
#     # UPCs: schematic vs sales
#     # -----------------------------
#     # dg_upc = in-schematic item key
#     if "dg_upc" in df_gaps.columns:
#         snapshot_df["UPC"] = df_gaps["dg_upc"].apply(normalize_upc)
#     else:
#         snapshot_df["UPC"] = None

#     # sr_upc = sold item key (may be blank for gaps)
#     if "sr_upc" in df_gaps.columns:
#         snapshot_df["SR_UPC"] = df_gaps["sr_upc"].apply(normalize_upc)
#     else:
#         snapshot_df["SR_UPC"] = None

#     # -----------------------------
#     # IN_SCHEMATIC flag
#     # -----------------------------
#     if "In_Schematic" in df_gaps.columns:
#         snapshot_df["IN_SCHEMATIC"] = df_gaps["In_Schematic"]
#     else:
#         snapshot_df["IN_SCHEMATIC"] = True  # safe fallback

#     # -----------------------------
#     # GAP definition (single source of truth)
#     # -----------------------------
#     # Gap if: in schematic UPC exists AND SR_UPC is missing
#     snapshot_df["IS_GAP"] = snapshot_df["UPC"].notna() & snapshot_df["SR_UPC"].isna()

#     # -----------------------------
#     # Optional fields (placeholders)
#     # -----------------------------
#     snapshot_df["GAP_CASES"] = None
#     snapshot_df["LAST_PURCHASE_DATE"] = None
#     snapshot_df["CATEGORY"] = None
#     snapshot_df["SUBCATEGORY"] = None

#     return snapshot_df


# -----------------------------
# Page
# -----------------------------
def render():
    st.title("Gap Report Generator")

    # ------------------------------------------------------------------
    # Connection + tenant context
    # ------------------------------------------------------------------
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Database connection not available.")
        return

    tenant_config = st.session_state.get("tenant_config")
    tenant_id = _get_tenant_id(tenant_config)

    # ------------------------------------------------------------------
    # Filter options
    # ------------------------------------------------------------------
    try:
        salesperson_options = fetch_distinct_values(conn, "SALESPERSON", "SALESPERSON")
        store_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        supplier_options = fetch_distinct_values(conn, "SUPPLIER_COUNTY", "SUPPLIER")
    except Exception as e:
        st.error(f"❌ Failed to fetch filter values: {e}")
        return

    for options in (salesperson_options, store_options, supplier_options):
        options.sort()
        options.insert(0, "All")

    # ------------------------------------------------------------------
    # Filters form
    # ------------------------------------------------------------------
    with st.form(
        key=f"Gap_Report_{st.session_state.get('user_email', 'default')}",
        clear_on_submit=True,
    ):
        salesperson = st.selectbox("Filter by Salesperson", salesperson_options)
        store = st.selectbox("Filter by Chain", store_options)
        supplier = st.selectbox("Filter by Supplier", supplier_options)
        submitted = st.form_submit_button("Generate Gap Report")

    if not submitted:
        return

    # ------------------------------------------------------------------
    # Generate report
    # ------------------------------------------------------------------
    with st.spinner("Generating report..."):
        temp_file_path = create_gap_report(conn, salesperson, store, supplier)

        if not temp_file_path or not os.path.exists(temp_file_path):
            st.error("❌ Report generation failed (no file produced).")
            return

        # Download
        with open(temp_file_path, "rb") as f:
            bytes_data = f.read()

        today = datetime.today().strftime("%Y-%m-%d")
        st.download_button(
            label="Download Gap Report",
            data=bytes_data,
            file_name=f"Gap_Report_{today}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        try:
            os.remove(temp_file_path)
        except Exception as e:
            st.warning(f"Failed to delete temporary file: {e}")

