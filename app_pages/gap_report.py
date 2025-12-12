# ---------- gap_report.py ----------

import streamlit as st
import pandas as pd
import os
from datetime import datetime

from utils.reports_utils import create_gap_report
from utils.snowflake_utils import fetch_distinct_values
from utils.gap_history_helpers import save_gap_snapshot   # <-- HISTORY HELPER


def render():
    """
    Gap Report Generator

    Overview:
    - Lets the user filter by Salesperson, Chain (store), and Supplier.
    - Generates an Excel Gap Report via create_gap_report().
    - After generation, reads the Excel back into a DataFrame and,
      on the first run of the week per tenant, saves a snapshot into:
         - GAP_REPORT_RUNS
         - GAP_REPORT_SNAPSHOT
    """
    st.title("Gap Report Generator")

    # ------------------------------------------------------------------
    # Connection + tenant context
    # ------------------------------------------------------------------
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Database connection not available.")
        return

    tenant_config = st.session_state.get("tenant_config")

    # Robust tenant_id extraction: supports both dict and object configs
    tenant_id = None
    if tenant_config is not None:
        if isinstance(tenant_config, dict):
            tenant_id = tenant_config.get("tenant_id")
        else:
            tenant_id = getattr(tenant_config, "tenant_id", None)

    # TEMP debug: remove later if you want
   # st.caption(f"DEBUG: tenant_id detected in gap_report = {tenant_id}")

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

    for options in [salesperson_options, store_options, supplier_options]:
        options.sort()
        options.insert(0, "All")

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

        if not temp_file_path:
            st.error("❌ Report generation failed.")
            return

        # ==============================================================
        # GAP HISTORY SNAPSHOT (first run per week per tenant)
        # ==============================================================
        if tenant_id is not None:
            try:
                # Load the generated report into a DataFrame.
                df_gaps = pd.read_excel(temp_file_path, engine="openpyxl")

                # after df_gaps = pd.read_excel(...)

                if df_gaps.empty:
                    st.caption("DEBUG: df_gaps is empty; skipping history snapshot.")
                else:
                    snapshot_df = pd.DataFrame()

                    # Store / chain
                    snapshot_df["CHAIN_NAME"]   = df_gaps.get("CHAIN_NAME")
                    snapshot_df["STORE_NUMBER"] = df_gaps.get("STORE_NUMBER")
                    snapshot_df["STORE_NAME"]   = df_gaps.get("STORE_NAME")

                    # Product / supplier
                    snapshot_df["SUPPLIER_NAME"] = df_gaps.get("SUPPLIER")
                    snapshot_df["PRODUCT_NAME"]  = df_gaps.get("PRODUCT_NAME")

                    # Salesperson
                    snapshot_df["SALESPERSON_NAME"] = df_gaps.get("SALESPERSON")

                                        # -----------------------------
                    # UPCs: schematic vs sales
                    # -----------------------------
                    # dg_upc = in-schematic item key
                    if "dg_upc" in df_gaps.columns:
                        snapshot_df["UPC"] = (
                            df_gaps["dg_upc"]
                            .astype(str)
                            .str.strip()
                            .replace({"": None})
                        )

                                        # -----------------------------
                    # SR_UPC: sold item key (can be blank for gaps)
                    # We intentionally preserve NaN as NaN and only
                    # treat non-empty, non-"nan" strings as real sales.
                    # -----------------------------
                    if "sr_upc" in df_gaps.columns:
                        sr_raw = df_gaps["sr_upc"]

                        # Keep original nulls; only strip text around real strings
                        sr_clean = sr_raw.copy()

                        # For non-null entries, normalize whitespace / text
                        mask_non_null = ~sr_raw.isna()
                        sr_clean.loc[mask_non_null] = (
                            sr_raw.loc[mask_non_null]
                            .astype(str)
                            .str.strip()
                        )

                        # Treat "", "nan", "NaN", "NONE" as missing as well
                        sr_clean = sr_clean.replace(
                            {
                                "": None,
                                "nan": None,
                                "NaN": None,
                                "NONE": None,
                            }
                        )

                        snapshot_df["SR_UPC"] = sr_clean
                    else:
                        snapshot_df["SR_UPC"] = None

                    # -----------------------------
                    # IN_SCHEMATIC flag
                    # -----------------------------
                    if "In_Schematic" in df_gaps.columns:
                        # keep raw value; we'll normalize to TRUE/FALSE later
                        snapshot_df["IN_SCHEMATIC"] = df_gaps["In_Schematic"]
                    else:
                        snapshot_df["IN_SCHEMATIC"] = True  # worst-case fallback

                    # -----------------------------
                    # GAP definition:
                    # dg_upc present AND sr_upc missing
                    # -----------------------------
                    if "SR_UPC" in snapshot_df.columns:
                        sr_series = snapshot_df["SR_UPC"]

                        # Missing if: NaN OR None OR empty/placeholder text
                        is_missing_sr = (
                            sr_series.isna()
                            | sr_series.astype(str).str.strip().isin(["", "nan", "NaN", "NONE"])
                        )

                        snapshot_df["IS_GAP"] = is_missing_sr
                    else:
                        # If somehow no SR_UPC column, mark all as gaps (conservative)
                        snapshot_df["IS_GAP"] = True



                    # -----------------------------
                    # In_schematic + gap flag
                    # -----------------------------
                    if "In_Schematic" in df_gaps.columns:
                        # keep raw value, Snowflake BOOLEAN can handle 1/0 or True/False via write_pandas
                        snapshot_df["IN_SCHEMATIC"] = df_gaps["In_Schematic"]
                    else:
                        snapshot_df["IN_SCHEMATIC"] = True  # worst case fallback

                    # Gap definition: dg_upc present AND sr_upc missing
                    if "SR_UPC" in snapshot_df.columns:
                        snapshot_df["IS_GAP"] = snapshot_df["SR_UPC"].isna() | (snapshot_df["SR_UPC"] == "")
                    else:
                        # If we somehow don't have sr_upc, just mark all as gaps (conservative)
                        snapshot_df["IS_GAP"] = True

                    # -----------------------------
                    # Optional fields in snapshot table
                    # -----------------------------
                    snapshot_df["GAP_CASES"]          = None
                    snapshot_df["LAST_PURCHASE_DATE"] = None
                    snapshot_df["CATEGORY"]           = None
                    snapshot_df["SUBCATEGORY"]        = None

                    triggered_by = (
                        st.session_state.get("user_email")
                        or st.session_state.get("username")
                        or "gap_report_page"
                    )

                    snapshot_saved = save_gap_snapshot(
                        conn=conn,
                        tenant_id=tenant_id,
                        df_gaps=snapshot_df,      # NOTE: we pass snapshot_df, not raw df_gaps
                        snapshot_week_start=None,
                        triggered_by=triggered_by,
                    )

                    if snapshot_saved:
                        st.info("📌 Gap history snapshot saved for this week (first run only).")
                    else:
                        st.caption("DEBUG: save_gap_snapshot returned False (no new history saved).")


            except Exception as e:
                # Don't break the download just because history failed.
                st.warning(f"Gap history snapshot failed: {e}")
        else:
            st.caption("DEBUG: tenant_id is None in gap_report; skipping history snapshot.")

        # ------------------------------------------------------------------
        # Offer download as usual
        # ------------------------------------------------------------------
        with open(temp_file_path, "rb") as f:
            bytes_data = f.read()

        today = datetime.today().strftime("%Y-%m-%d")
        st.download_button(
            label="Download Gap Report",
            data=bytes_data,
            file_name=f"Gap_Report_{today}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Clean up temp file
        try:
            os.remove(temp_file_path)
        except Exception as e:
            st.warning(f"Failed to delete temporary file: {e}")
