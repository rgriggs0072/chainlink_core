# ------------- home.py ----------------
"""
Home Page (Chainlink Core)

Overview for future devs:
- Uses the active tenant Snowflake connection from st.session_state["conn"].
- Header shows CLIENTS.BUSINESS_NAME (fallback: tenant_config.tenant_name).
- Sections:
  1) Execution summary card + Chain bar chart
  2) Salesperson table + Gap history pivot (with downloads + snapshot processor)
  3) Supplier performance scatter (multiselect filter)

Notes:
- Do NOT close the shared tenant connection here.
- Keep heavy query logic in utils.dashboard_data.home_dashboard where possible.
"""

import streamlit as st
import pandas as pd
import altair as alt
from io import BytesIO
from datetime import datetime

# UI helpers
from utils.ui_helpers import render_supplier_filter
from utils.home_ui_helpers import render_supplier_scatter

# Gap snapshot helper (calls BUILD_GAP_TRACKING with overwrite protection)
from utils.snowflake_utils import check_and_process_data

# Data helpers (centralize query logic here)
from utils.dashboard_data.home_dashboard import (
    get_execution_summary,
    fetch_chain_schematic_data,
    fetch_supplier_schematic_summary_data,  # expects (conn, suppliers)
)

# Tenant/org display name
from utils.org_utils import get_business_name


def render() -> None:
    """
    Entry point for the Home page.

    Relies on:
        - st.session_state["conn"]: active Snowflake connection for the tenant.
        - st.session_state["tenant_config"]: tenant metadata (db/schema/etc.).
        - st.session_state["tenant_id"]: used to resolve BUSINESS_NAME.
    """
    # ---------------- Session / Tenant guards ----------------
    conn = st.session_state.get("conn")
    tenant_config = st.session_state.get("tenant_config") or {}
    if not conn or not tenant_config:
        st.error("Missing tenant connection/config. Please log in.")
        return

    tenant_id = st.session_state.get("tenant_id")
    fallback_name = tenant_config.get("tenant_name") or "Tenant"

    # Resolve display name: BUSINESS_NAME -> TENANT_NAME
    display_name = get_business_name(tenant_id) or fallback_name

    # ---------------- Header ----------------
    st.markdown(f"# {display_name}  Chain Dashboard")
    st.markdown(
        "<p style='margin-top: 0rem; font-size: 1rem; color: gray;'>"
        "Welcome to your data intelligence hub."
        "</p>",
        unsafe_allow_html=True,
    )

        # ---------------- Button styling (Download + Process) ----------------
    st.markdown(
        """
        <style>
        /* Make Streamlit buttons match Chainlink primary styling */
        div[data-testid="stButton"] > button {
            background-color: #6497D6 !important;  /* primary */
            color: white !important;
            border-radius: 0.5rem !important;
            border: none !important;
            padding: 0.4rem 0.9rem !important;
            font-size: 0.85rem !important;
            font-weight: 600 !important;
        }
        div[data-testid="stButton"] > button:hover {
            filter: brightness(0.92);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


    # ======================================================================
    # 1) Execution Summary + Chain Bar Chart
    # ======================================================================
    row1_col1, row1_col2 = st.columns(2, gap="large")

    try:
        # Execution summary numbers
        total_in_schematic, total_purchased, total_gaps, purchased_pct = get_execution_summary(conn)
        missed_revenue = total_gaps * 40.19  # TODO: parameterize $/gap if needed

        with row1_col1:
            # Minimal styled card (keep CSS local to avoid global bleed)
            st.markdown(
                f"""
                <div style="
                    background-color:#F8F2EB;
                    padding:24px;
                    border-radius:12px;
                    box-shadow:0 2px 8px rgba(0,0,0,0.06);
                    text-align:left;">
                    <div style="font-weight:700; font-size:1.1rem; margin-bottom:8px;">Execution Summary</div>
                    <div>Total In Schematic: <b>{total_in_schematic:,}</b></div>
                    <div>Total Purchased: <b>{total_purchased:,}</b></div>
                    <div>Total Gaps: <b>{total_gaps:,}</b></div>
                    <div>Overall Purchased %: <b>{purchased_pct:.2f}%</b></div>
                    <div>Overall Missed Revenue: <b>${missed_revenue:,.2f}</b></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with row1_col2:
            chain_summary_df = fetch_chain_schematic_data(conn, tenant_config)
            if not chain_summary_df.empty:
                chart = (
                    alt.Chart(chain_summary_df, background="#F8F2EB")
                    .mark_bar()
                    .encode(
                        x=alt.X("CHAIN_NAME:N", title="Chain"),
                        y=alt.Y("Total_In_Schematic:Q", title="In Schematic"),
                        color=alt.Color("CHAIN_NAME:N", scale=alt.Scale(scheme="viridis")),
                        tooltip=[
                            "CHAIN_NAME",
                            "Total_In_Schematic",
                            "Purchased",
                            alt.Tooltip("Purchased_Percentage:Q", title="Purchased %", format=".2%"),
                        ],
                    )
                    .properties(height=250)
                    .configure_title(align="center", fontSize=16)
                )
                st.altair_chart(chart, width="stretch")
            else:
                st.warning("No chain summary data available.")

    except Exception as e:
        row1_col1.error("Failed to render execution summary or chain chart.")
        row1_col1.exception(e)

    st.markdown("---")

    # ======================================================================
    # 2) Salesperson Summary (left) + Gap History Pivot (right)
    # ======================================================================
    row2_col1, row2_col2 = st.columns([40, 70], gap="small")

    # ----------------------------------------------------------------------
    # 2a) Salesperson summary table + download (left)
    # ----------------------------------------------------------------------
    try:
        query = """
            SELECT SALESPERSON, TOTAL_DISTRIBUTION, TOTAL_GAPS, EXECUTION_PERCENTAGE
            FROM SALESPERSON_EXECUTION_SUMMARY
            ORDER BY TOTAL_GAPS DESC
        """
        salesperson_df = pd.read_sql_query(query, conn)

        if not salesperson_df.empty:
            salesperson_df["EXECUTION_PERCENTAGE"] = (
                salesperson_df["EXECUTION_PERCENTAGE"].astype(float).round(2)
            )
            salesperson_df = salesperson_df.rename(
                columns={
                    "SALESPERSON": "Salesperson",
                    "TOTAL_DISTRIBUTION": "Distribution",
                    "TOTAL_GAPS": "Gaps",
                    "EXECUTION_PERCENTAGE": "Execution Percentage",
                }
            )

            limited_df = salesperson_df.head(100)
            table_html = limited_df.to_html(
                classes=["table", "table-striped"], escape=False, index=False
            )

            # Bold Salesperson names
            for _, row in limited_df.iterrows():
                table_html = table_html.replace(
                    f"<td>{row['Salesperson']}</td>",
                    f"<td style='font-weight:700;'>{row['Salesperson']}</td>",
                )

            container_css = """
                max-height: 365px;
                overflow-y: auto;
                background-color: #F8F2EB;
                text-align: center;
                padding: 1% 2% 2% 0%;
                border-radius: 10px;
                border-left: 0.5rem solid #9AD8E1 !important;
                box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important;
                width: 100%;
                font-size: 0.85rem;
            """

            with row2_col1:
                st.markdown(f"<div style='{container_css}'>{table_html}</div>", unsafe_allow_html=True)

                # Excel download
                excel_data = BytesIO()
                salesperson_df.to_excel(excel_data, index=False)
                excel_data.seek(0)
                st.download_button(
                    "Download Salesperson Summary",
                    data=excel_data,
                    file_name="salesperson_execution_summary.xlsx",
                )
        else:
            row2_col1.info("No salesperson summary data.")
    except Exception as e:
        row2_col1.error("Failed to load salesperson summary.")
        row2_col1.exception(e)

    # ----------------------------------------------------------------------
    # 2b) Gap History pivot + download + "Process Gap Pivot Data" (right)
    # ----------------------------------------------------------------------
    try:
        gap_query = """
            SELECT SALESPERSON, TOTAL_GAPS, EXECUTION_PERCENTAGE, LOG_DATE
            FROM SALESPERSON_EXECUTION_SUMMARY_TBL
            ORDER BY TOTAL_GAPS DESC
        """
        gap_df = pd.read_sql_query(gap_query, conn)

        if not gap_df.empty:
            # Rename for readability in the UI
            gap_df = gap_df.rename(
                columns={
                    "SALESPERSON": "Salesperson",
                    "TOTAL_GAPS": "Gaps",
                    "EXECUTION_PERCENTAGE": "Execution Percentage",
                    "LOG_DATE": "Log Date",
                }
            )

            # Sort by Log Date (most recent first) and get the 12 latest distinct dates
            gap_df_sorted = gap_df.sort_values(by="Log Date", ascending=False)
            latest_dates = gap_df_sorted["Log Date"].drop_duplicates().head(12)

            # Pivot: rows = Salesperson, columns = Log Date, values = Gaps
            gap_df_pivot = gap_df.pivot_table(
                index="Salesperson",
                columns="Log Date",
                values="Gaps",
                aggfunc="sum",
            )

            # Limit to the latest 12 dates (columns)
            gap_df_pivot_limited = gap_df_pivot[latest_dates]

            # Drop any columns/rows that are entirely NaN (no data at all)
            gap_df_pivot_limited = gap_df_pivot_limited.dropna(axis=1, how="all")
            gap_df_pivot_limited = gap_df_pivot_limited.dropna(axis=0, how="all")

            # Format the date columns for display (YY/MM/DD)
            gap_df_pivot_limited.columns = pd.to_datetime(
                gap_df_pivot_limited.columns
            ).strftime("%y/%m/%d")

            # ---------- UI DISPLAY: show 0 instead of NaN ----------
            gap_display = gap_df_pivot_limited.fillna(0)

            # Try to render as ints where possible (looks cleaner than floats)
            try:
                gap_display = gap_display.astype(int)
            except Exception:
                gap_display = gap_display.astype(float)

            table_html = gap_display.to_html(
                classes=["table", "table-striped"],
                escape=False,
                na_rep="0",
            )

            container_css = """
                max-height: 365px;
                overflow-y: auto;
                background-color: #F8F2EB;
                text-align: center;
                padding: 1% 2% 2% 0%;
                border-radius: 10px;
                border-left: 0.5rem solid #9AD8E1 !important;
                box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important;
                width: 100%;
                font-size: 0.85rem;
            """

            # Render table + Download button + Process button in the same column,
            # in the order: table -> download -> "Process Gap Pivot Data"
            row2_col2.markdown(
                f"<div style='{container_css}'>{table_html}</div>",
                unsafe_allow_html=True,
            )

            # Excel download: numeric, NaNs -> 0
            excel_data_pivot = BytesIO()
            gap_df_pivot_limited.fillna(0).to_excel(excel_data_pivot, index=True)
            excel_data_pivot.seek(0)

            row2_col2.download_button(
                "Download Gap History",
                data=excel_data_pivot,
                file_name="gap_history_report.xlsx",
            )

            # Button BELOW the table and BELOW the download button
            if row2_col2.button("Process Gap Pivot Data", key="process_gap_pivot"):
                # This will handle "already processed today" logic + CALL BUILD_GAP_TRACKING()
                check_and_process_data(conn)

        else:
            row2_col2.info("No gap history data.")
    except Exception as e:
        row2_col2.error("Failed to load gap pivot table.")
        row2_col2.exception(e)

    st.markdown("---")

    # ======================================================================
    # 3) Supplier Performance Scatter
    # ======================================================================
    st.subheader("Supplier Performance Scatter")

    # Compact multiselect styling
    st.markdown(
        """
        <style>
        div[data-testid="stMultiSelect"] label { font-size: 0.85rem !important; }
        div[data-testid="stMultiSelect"] span { font-size: 0.85rem !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar filter (sets st.session_state["selected_suppliers"])
    render_supplier_filter()

    selected_suppliers = st.session_state.get("selected_suppliers", [])
    if selected_suppliers:
        try:
            df_supplier = fetch_supplier_schematic_summary_data(conn, selected_suppliers)
            if df_supplier is not None and not df_supplier.empty:
                render_supplier_scatter(df_supplier)
            else:
                st.info("No supplier performance data for the selected suppliers.")
        except Exception as e:
            st.error("Failed to render supplier scatter.")
            st.exception(e)
    else:
        st.info("Please select suppliers from the sidebar to view the scatter chart.")

    # IMPORTANT: Do not close the shared session connection here.
    # The app uses st.session_state['conn'] across pages.
