# ---------- app_pages/gap_history.py ----------

"""
Gap History & Streaks

Overview for future devs:
- Uses the GAP_CURRENT_STREAKS view (built on GAP_REPORT_SNAPSHOT) to show
  how long each item has been a gap in consecutive weekly snapshots.
- All streaks are calculated PER SALESPERSON + CHAIN + STORE + UPC.
  That means the same schematic item at the same store can have different
  streaks for different reps, which is what we want for accountability.
- Key semantics:
    - UPC      = dg_upc (in-schematic product UPC)
    - SR_UPC   = last sales UPC (NULL means no sales)
    - IS_GAP   = TRUE when SR_UPC is NULL in the snapshot week
- The GAP_CURRENT_STREAKS view exposes:
    - STREAK_WEEKS    = # of consecutive weeks (ending at latest snapshot)
    - FIRST_GAP_WEEK  = first week in the current streak
    - LAST_GAP_WEEK   = last week in the current streak (same as snapshot)
- This page:
    - Filters by Salesperson, Chain, Supplier.
    - Color-codes streaks:
        1 week   -> no color
        2 weeks  -> yellow (be aware)
        3 weeks  -> orange (issue)
        4+ weeks -> red (urgent)
    - Allows export to CSV and PDF for sharing/printing.

Notes:
- Relies on st.session_state["conn"] for tenant-scoped Snowflake connection.
- Relies on st.session_state["tenant_config"].tenant_id (or dict equivalent).
"""

import streamlit as st
import pandas as pd
from utils.pdf_reports import build_gap_streaks_pdf



# ----------------------------------------------------------------------
# Tenant helpers
# ----------------------------------------------------------------------
def _get_tenant_id():
    """
    Extract tenant_id from st.session_state["tenant_config"].

    Handles both dict-based and object-based tenant_config structures so
    future refactors don't break this page.
    """
    tenant_config = st.session_state.get("tenant_config")
    if tenant_config is None:
        return None

    if isinstance(tenant_config, dict):
        return tenant_config.get("tenant_id")

    return getattr(tenant_config, "tenant_id", None)


# ----------------------------------------------------------------------
# Data access
# ----------------------------------------------------------------------
def fetch_gap_streaks(conn, tenant_id: int) -> pd.DataFrame:
    """
    Fetch current gap streaks for this tenant from GAP_CURRENT_STREAKS.

    The view must exist in the active DB/SCHEMA for the tenant connection.

    Returns
    -------
    pd.DataFrame
        Columns:
          SNAPSHOT_WEEK_START, FIRST_GAP_WEEK, LAST_GAP_WEEK,
          SALESPERSON_NAME, CHAIN_NAME, STORE_NUMBER, STORE_NAME,
          UPC, PRODUCT_NAME, SUPPLIER_NAME, STREAK_WEEKS
    """
    sql = """
        SELECT
            SNAPSHOT_WEEK_START,
            FIRST_GAP_WEEK,
            LAST_GAP_WEEK,
            SALESPERSON_NAME,
            CHAIN_NAME,
            STORE_NUMBER,
            STORE_NAME,
            UPC,
            PRODUCT_NAME,
            SUPPLIER_NAME,
            STREAK_WEEKS
        FROM GAP_CURRENT_STREAKS
        WHERE TENANT_ID = %s
        ORDER BY
            SALESPERSON_NAME,
            STREAK_WEEKS DESC,
            CHAIN_NAME,
            STORE_NUMBER,
            PRODUCT_NAME
    """

    cur = conn.cursor()
    try:
        cur.execute(sql, (tenant_id,))
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()

        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


# ----------------------------------------------------------------------
# Styling / streak logic
# ----------------------------------------------------------------------
def _assign_streak_color(streak_weeks: int) -> str:
    """
    Map streak length to a simple label that we translate into colors.

    1 week  -> ""
    2 weeks -> "yellow"
    3 weeks -> "orange"
    4+      -> "red"
    """
    if streak_weeks >= 4:
        return "red"
    elif streak_weeks == 3:
        return "orange"
    elif streak_weeks == 2:
        return "yellow"
    else:
        return ""


def _style_gap_table(df: pd.DataFrame):
    """
    Apply color styling to streak rows using pandas Styler.

    Colors:
    - Yellow: 2-week streak
    - Orange: 3-week streak
    - Red: 4+ week streak

    Expects a helper column "_STREAK_COLOR" already present in the DataFrame.
    """
    def row_style(row):
        color = row.get("_STREAK_COLOR", "")
        if color == "red":
            return ["background-color: #ffb3b3"] * len(row)
        elif color == "orange":
            return ["background-color: #ffd9b3"] * len(row)
        elif color == "yellow":
            return ["background-color: #fff7b3"] * len(row)
        else:
            return [""] * len(row)

    return df.style.apply(row_style, axis=1)


# ----------------------------------------------------------------------
# Page render
# ----------------------------------------------------------------------
def render():
    """
    Render the Gap History & Streaks page.

    Flow:
    - Validate Snowflake connection + tenant_id.
    - Fetch streaks from GAP_CURRENT_STREAKS (per salesperson).
    - Build filters (Salesperson / Chain / Supplier).
    - Render color-coded table + CSV + PDF download.
    """
    st.title("Gap History & Streaks (By Salesperson)")

    # ------------------------------------------------------------------
    # Connection + tenant context
    # ------------------------------------------------------------------
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Database connection not available.")
        return

    tenant_id = _get_tenant_id()
    if tenant_id is None:
        st.error("❌ tenant_id not found in session_state. Cannot load gap history.")
        return

    tenant_config = st.session_state.get("tenant_config", {}) or {}

    # ------------------------------------------------------------------
    # Load streak data
    # ------------------------------------------------------------------
    with st.spinner("Loading gap streaks…"):
        df = fetch_gap_streaks(conn, tenant_id)

    if df.empty:
        st.info("No gap history found yet. Generate at least one Gap Report to start tracking.")
        return

    # Normalize date columns for display
    df["SNAPSHOT_WEEK_START"] = pd.to_datetime(df["SNAPSHOT_WEEK_START"]).dt.date
    df["FIRST_GAP_WEEK"] = pd.to_datetime(df["FIRST_GAP_WEEK"]).dt.date
    df["LAST_GAP_WEEK"] = pd.to_datetime(df["LAST_GAP_WEEK"]).dt.date

    # ------------------------------------------------------------------
    # Build filter options
    # ------------------------------------------------------------------
    salespeople = sorted(df["SALESPERSON_NAME"].dropna().unique().tolist())
    chains = sorted(df["CHAIN_NAME"].dropna().unique().tolist())
    suppliers = sorted(df["SUPPLIER_NAME"].dropna().unique().tolist())

    salespeople.insert(0, "All")
    chains.insert(0, "All")
    suppliers.insert(0, "All")

    # ------------------------------------------------------------------
    # Filters (reactive widgets)
    # ------------------------------------------------------------------
    col1, col2, col3 = st.columns(3)

    with col1:
        salesperson_filter = st.selectbox(
            "Salesperson",
            salespeople,
            index=0,
        )
    with col2:
        chain_filter = st.selectbox(
            "Chain",
            chains,
            index=0,
        )
    with col3:
        supplier_filter = st.selectbox(
            "Supplier",
            suppliers,
            index=0,
        )

    # Compute max streak length present in the data
    max_streak = int(df["STREAK_WEEKS"].max() or 1)

    # ✅ Always default to 1 so "All" truly shows ALL gaps (including 1-week gaps)
    min_streak = st.slider(
        "Minimum streak length (weeks)",
        min_value=1,
        max_value=max_streak,
        value=1,
        help="Show items that have been gaps for at least this many consecutive weeks.",
    )

    # Optional: keep the helpful caption when history is only 1 week
    if max_streak <= 1:
        st.caption(
            "Only one week of gap history so far — showing all current gaps "
            "(streak length = 1 week)."
        )

    # ------------------------------------------------------------------
    # Apply filters
    # ------------------------------------------------------------------
    filtered = df.copy()

    if salesperson_filter != "All":
        filtered = filtered[filtered["SALESPERSON_NAME"] == salesperson_filter]

    if chain_filter != "All":
        filtered = filtered[filtered["CHAIN_NAME"] == chain_filter]

    if supplier_filter != "All":
        filtered = filtered[filtered["SUPPLIER_NAME"] == supplier_filter]

    # ✅ Apply min streak filter AFTER other filters
    filtered = filtered[filtered["STREAK_WEEKS"] >= min_streak]

    if filtered.empty:
        st.warning("No gaps match the selected filters and minimum streak length.")
        return

    # Assign color labels per streak length
    filtered["_STREAK_COLOR"] = filtered["STREAK_WEEKS"].apply(_assign_streak_color)


    # ------------------------------------------------------------------
    # Display table (salesperson-first)
    # ------------------------------------------------------------------
    st.subheader("Current Gap Streaks by Salesperson")

    display_cols = [
        "STREAK_WEEKS",
        "SNAPSHOT_WEEK_START",
        "FIRST_GAP_WEEK",
        "LAST_GAP_WEEK",
        "SALESPERSON_NAME",
        "CHAIN_NAME",
        "STORE_NUMBER",
        "STORE_NAME",
        "SUPPLIER_NAME",
        "PRODUCT_NAME",
        "UPC",
    ]

    display_df = filtered[display_cols].sort_values(
        by=["SALESPERSON_NAME", "STREAK_WEEKS", "CHAIN_NAME", "STORE_NUMBER", "PRODUCT_NAME"],
        ascending=[True, False, True, True, True],
    )

    # Use Styler to color rows and feed it to Streamlit
    styled = _style_gap_table(display_df.join(filtered["_STREAK_COLOR"]))
    st.dataframe(styled, width='stretch')

    # ------------------------------------------------------------------
    # Downloads: CSV + PDF
    # ------------------------------------------------------------------
    st.write(f"Showing {len(display_df)} gap streak(s).")

    # CSV download (row-level detail)
    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Gap Streaks (CSV)",
        data=csv_bytes,
        file_name="gap_streaks_by_salesperson.csv",
        mime="text/csv",
        key="gap_streaks_csv",
    )

    # PDF download (color-coded, shareable)

    # ---- PDF Download (use a slim column set so the PDF fits nicely) ----
    pdf_cols = [
        "SALESPERSON_NAME",
        "CHAIN_NAME",
        "STORE_NUMBER",
        "STORE_NAME",
        "SUPPLIER_NAME",
        "PRODUCT_NAME",
        "STREAK_WEEKS",
        "FIRST_GAP_WEEK",
        "LAST_GAP_WEEK",
    ]

    pdf_df = display_df[pdf_cols].copy()

    tenant_name = (
        (tenant_config.get("display_name") if isinstance(tenant_config, dict) else None)
        or (tenant_config.get("tenant_name") if isinstance(tenant_config, dict) else None)
        or "Client"
    )

    pdf_bytes = build_gap_streaks_pdf(
        pdf_df,
        tenant_name=tenant_name,
    )

    st.download_button(
        label="📄 Download Gap Streaks (PDF)",
        data=pdf_bytes,
        file_name="gap_streaks_report.pdf",
        mime="application/pdf",
        key="gap_streaks_pdf",
    )
