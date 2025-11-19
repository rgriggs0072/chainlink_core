# app_pages/driver_forecast.py
# -*- coding: utf-8 -*-
"""
Truck Forecast Load Plan

Overview
---------
- Runs Snowflake SQL summarizing 4-week predicted truck loads by UPC/Product/Supplier.
- Each row = one unique sellable UPC.
- Columns show Week 1–4 case forecasts + 4-week total.
- Includes a final "TOTAL TRUCK LOAD" footer row.

UX flow
--------
1. Select Salesperson (dropdown from CUSTOMERS table)
2. (Optional) Adjust Anchor Date (defaults to today)
3. Click "Generate Forecast"
4. View, download CSV, or export PDF

Notes
-----
- Reuses session connection from Chainlink Core or re-establishes it if missing.
- Read-only; no data writes.
- Uses st.form() to isolate reruns.
"""

import streamlit as st
import pandas as pd
from datetime import date
from io import BytesIO
from utils.forecasting_truck import fetch_distinct_salespeople
from sf_connector.service_connector import connect_to_tenant_snowflake


# =============================================================================
# Helper: run the Snowflake SQL and return a DataFrame
# =============================================================================
def fetch_truck_forecast(conn, salesperson: str | None, anchor_date: date) -> pd.DataFrame:
    """
    Fetch a 4-week per-UPC forecast for a given salesperson starting from anchor_date.

    Logic:
    - Uses SALES_RAW_IMPORT as the historical source (UNITS_SOLD by TX_DATE).
    - Aggregates to weekly volume per SALESPERSON + UPC + PRODUCT_NAME + SUPPLIER.
    - Computes a 4-week moving average (ma4_units) as the baseline.
    - Takes the most recent baseline per UPC as the forecast level.
    - Projects that baseline into the next 4 weeks starting from anchor_date.
    - Pivots into WEEK1–WEEK4 columns and adds a 'TOTAL TRUCK LOAD' footer row.

    Assumptions:
    - SALES_RAW_IMPORT has columns: SALESPERSON, UPC, PRODUCT_NAME, SUPPLIER, TX_DATE, UNITS_SOLD.
      (If SALESPERSON/SUPPLIER are missing, they should be added or joined in upstream.)
    """

    sales_filter = (salesperson or "").strip() or None

    sql = """
WITH hist AS (
  -- 1) Base weekly history per UPC for the salesperson
  SELECT
      UPPER(TRIM(SALESPERSON))                   AS SALESPERSON,
      TO_VARCHAR(UPC)                            AS UPC,
      PRODUCT_NAME,
      SUPPLIER,
      DATE_TRUNC('WEEK', TX_DATE)                AS WK_START,
      SUM(UNITS_SOLD)                            AS WK_UNITS
  FROM SALES_RAW_IMPORT
  WHERE TX_DATE IS NOT NULL
    AND UPC IS NOT NULL
    AND ( %s IS NULL OR UPPER(TRIM(SALESPERSON)) = UPPER(TRIM(%s)) )
  GROUP BY
      UPPER(TRIM(SALESPERSON)),
      TO_VARCHAR(UPC),
      PRODUCT_NAME,
      SUPPLIER,
      DATE_TRUNC('WEEK', TX_DATE)
),

roll AS (
  -- 2) Rolling 4-week average per UPC as the baseline signal
  SELECT
      SALESPERSON,
      UPC,
      PRODUCT_NAME,
      SUPPLIER,
      WK_START,
      WK_UNITS,
      AVG(WK_UNITS) OVER (
        PARTITION BY SALESPERSON, UPC
        ORDER BY WK_START
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) AS ma4_units
  FROM hist
),

baseline AS (
  -- 3) Take the most recent baseline per UPC
  SELECT
      SALESPERSON,
      UPC,
      PRODUCT_NAME,
      SUPPLIER,
      ma4_units AS baseline_units
  FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY SALESPERSON, UPC
          ORDER BY WK_START DESC
        ) AS rn
    FROM roll
  )
  WHERE rn = 1
    AND ma4_units IS NOT NULL
),

future_weeks AS (
  -- 4) Define 4 future weekly buckets starting from the anchor_date's week
  SELECT
      DATEADD('WEEK', seq, DATE_TRUNC('WEEK', %s::DATE)) AS WK_START,
      seq + 1 AS HORIZON_WEEK
  FROM TABLE(GENERATOR(ROWCOUNT => 4))
),

forecast_upc AS (
  -- 5) Cross-join each UPC baseline to the 4 future weeks
  SELECT
      b.SALESPERSON,
      b.UPC,
      b.PRODUCT_NAME,
      b.SUPPLIER,
      f.WK_START,
      f.HORIZON_WEEK,
      b.baseline_units AS UPC_TOTAL_UNITS
  FROM baseline b
  CROSS JOIN future_weeks f
),

pivoted AS (
  -- 6) Pivot weeks 1–4 into columns
  SELECT *
  FROM forecast_upc
  PIVOT (
    SUM(UPC_TOTAL_UNITS) FOR HORIZON_WEEK
    IN (1 AS WEEK1, 2 AS WEEK2, 3 AS WEEK3, 4 AS WEEK4)
  )
),

detailed AS (
  -- 7) Final detail rows per UPC
  SELECT
      SALESPERSON,
      UPC,
      PRODUCT_NAME,
      SUPPLIER,
      WK_START,
      ROUND(WEEK1, 2) AS WEEK1_CASES,
      ROUND(WEEK2, 2) AS WEEK2_CASES,
      ROUND(WEEK3, 2) AS WEEK3_CASES,
      ROUND(WEEK4, 2) AS WEEK4_CASES,
      ROUND(
        COALESCE(WEEK1, 0)
        + COALESCE(WEEK2, 0)
        + COALESCE(WEEK3, 0)
        + COALESCE(WEEK4, 0),
        2
      ) AS TOTAL_4WK_CASES
  FROM pivoted
),

footer AS (
  -- 8) Rollup row with TOTAL TRUCK LOAD per salesperson
  SELECT
      MAX(SALESPERSON)              AS SALESPERSON,
      'ALL PRODUCTS'                AS UPC,
      'TOTAL TRUCK LOAD'            AS PRODUCT_NAME,
      NULL                          AS SUPPLIER,
      MAX(WK_START)                 AS WK_START,
      ROUND(SUM(WEEK1_CASES), 2)    AS WEEK1_CASES,
      ROUND(SUM(WEEK2_CASES), 2)    AS WEEK2_CASES,
      ROUND(SUM(WEEK3_CASES), 2)    AS WEEK3_CASES,
      ROUND(SUM(WEEK4_CASES), 2)    AS WEEK4_CASES,
      ROUND(SUM(TOTAL_4WK_CASES),2) AS TOTAL_4WK_CASES
  FROM detailed
)

SELECT * FROM detailed
UNION ALL
SELECT * FROM footer
ORDER BY PRODUCT_NAME NULLS LAST;
"""

    params = (
        sales_filter,  # first %s
        sales_filter,  # second %s
        anchor_date.strftime("%Y-%m-%d"),  # for future_weeks anchor
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()

    return pd.DataFrame(rows, columns=cols)









# =============================================================================
# Page render
# =============================================================================
def render():
    """Streamlit page entry point for Truck Forecast Load Plan."""
    st.title("🚛 Truck Forecast Load Plan")

    # --- Tenant context ---
    tenant_config = st.session_state.get("tenant_config")
    if not isinstance(tenant_config, dict):
        st.error("Tenant context is missing. Please log in again.")
        st.stop()

    tenant_db = tenant_config.get("database")
    tenant_sch = tenant_config.get("schema")
    if not tenant_db or not tenant_sch:
        st.error("Tenant TOML missing 'database' or 'schema'.")
        st.stop()

    # --- Connection ---
    conn = st.session_state.get("conn")
    if conn is None:
        conn = connect_to_tenant_snowflake(tenant_config)
        st.session_state["conn"] = conn

    # --- Salesperson selection ---
    salespeople = fetch_distinct_salespeople(conn, tenant_db, tenant_sch)
    if not salespeople:
        st.warning("No salespeople found in CUSTOMERS table.")
        return

    with st.form("truck_forecast_form"):
        st.write("Generate a driver-ready 4-week forecast of total truck loads by product.")
        col1, col2 = st.columns([2, 1])
        salesperson = col1.selectbox("Salesperson", salespeople, index=0)
        anchor_date = col2.date_input("Anchor Date", date.today())
        submitted = st.form_submit_button("Generate Forecast")

    if submitted:
        with st.spinner("Generating forecast, please wait..."):
            df = fetch_truck_forecast(conn, salesperson, anchor_date)

        if df.empty:
            st.warning("No forecast data found for the selected salesperson.")
            return

        # --- Display results ---
        st.success(f"Forecast generated for **{salesperson}**, starting {anchor_date:%b %d, %Y}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # --- Download CSV ---
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download CSV",
            csv,
            file_name=f"truck_forecast_{salesperson.replace(' ', '_')}.csv",
            mime="text/csv",
        )

        # --- Optional PDF Export ---
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet

            def make_pdf(dataframe: pd.DataFrame) -> BytesIO:
                buf = BytesIO()
                doc = SimpleDocTemplate(buf, pagesize=letter)
                styles = getSampleStyleSheet()
                elements = [
                    Paragraph(f"Truck Forecast Load Plan – {salesperson}", styles['Title']),
                    Spacer(1, 12)
                ]
                table_data = [dataframe.columns.tolist()] + dataframe.values.tolist()
                t = Table(table_data, repeatRows=1)
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ]))
                elements.append(t)
                doc.build(elements)
                buf.seek(0)
                return buf

            pdf_buf = make_pdf(df)
            st.download_button(
                "📄 Download PDF",
                pdf_buf,
                file_name=f"truck_forecast_{salesperson.replace(' ', '_')}.pdf",
                mime="application/pdf",
            )
        except Exception as e:
            st.info("PDF export unavailable (missing ReportLab or runtime error).")
            st.exception(e)
