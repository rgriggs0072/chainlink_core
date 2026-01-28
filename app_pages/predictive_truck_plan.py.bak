# app_pages/predictive_truck_plan.py
# -*- coding: utf-8 -*-
"""
Predictive Truck Plan (per-salesperson weekly)

Page Overview
-------------
- User selects target week (ISO Monday start) and horizon (1–4 weeks).
- Salesperson dropdown comes from SALES_RAW_IMPORT via utils.forecasting_truck.
- Forecasts per (store × UPC); shows summary by salesperson and detailed grid.
- Uses SALES_RAW_IMPORT as the single source of truth for history.
- CSV + PDF export supported (write-back currently disabled).

Dev Notes
---------
- Wrapped in `render()` so your router can call it directly.
- Uses st.form to avoid full-page reruns.
- Requires st.session_state["tenant_config"] → {"database": ..., "schema": ..., ...}.
- Requires st.session_state["tenant_id"] for tenant display (optional).
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from sf_connector.service_connector import connect_to_tenant_snowflake
from utils.forecasting_truck import (
    fetch_distinct_salespeople,
    fetch_route_scope,
    fetch_90d_weekly_sales,
    build_truck_plan_detail,
    get_sales_date_source,
    get_sales_measure_source,
)
from utils.pdf_reports import build_predictive_truck_pdf


# -----------------------------
# Context Helper
# -----------------------------
def _ensure_tenant_context(conn) -> tuple[str, str]:
    """
    Ensure st.session_state['tenant_db'] and ['tenant_schema'] are set.

    Priority:
      1) Existing session_state values (if present)
      2) CURRENT_DATABASE(), CURRENT_SCHEMA() from the live connection
      3) (Optional) Lookup via TOML using st.session_state['tenant_id']
    """
    db = st.session_state.get("tenant_db")
    sch = st.session_state.get("tenant_schema")
    if db and sch:
        return db, sch

    # 2) Pull from current Snowflake session
    try:
        with conn.cursor() as cur:
            cur.execute("select current_database(), current_schema()")
            row = cur.fetchone()
        if row and row[0] and row[1]:
            db, sch = row[0], row[1]
            st.session_state["tenant_db"] = db
            st.session_state["tenant_schema"] = sch
            return db, sch
    except Exception:
        pass

    # 3) Optional TOML fallback if you store tenant_id in state
    tenant_id = st.session_state.get("tenant_id")
    if tenant_id:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select DATABASE, SCHEMA
                    from TENANTUSERDB.CHAINLINK_SCH.TOML
                    where TENANT_ID = %s
                    limit 1
                    """,
                    (tenant_id,),
                )
                row = cur.fetchone()
            if row and row[0] and row[1]:
                db, sch = row[0], row[1]
                with conn.cursor() as cur:
                    cur.execute(f'use database "{db}"')
                    cur.execute(f'use schema "{sch}"')
                st.session_state["tenant_db"] = db
                st.session_state["tenant_schema"] = sch
                return db, sch
        except Exception:
            pass

    raise RuntimeError("Tenant context not set and could not be inferred.")


# =============================
# Page Entry Point
# =============================
def render():
    """
    Render the Predictive Truck Plan page.

    Flow:
    -----
    1. Resolve tenant + Snowflake connection.
    2. User selects week start, horizon, and salesperson.
    3. Build route scope from SALES_RAW_IMPORT.
    4. Fetch last 90 days weekly sales.
    5. Compute MA4-based forecasts per store × UPC.
    6. Render summary + detail, and expose CSV/PDF downloads.
    """
    # ---- Header ----
    tenant_id = st.session_state.get("tenant_id")

    st.title("🚛 Predictive Truck Plan")

    if tenant_id:
        st.caption(
            f"Tenant: **{tenant_id}** · Weekly truck load forecast (cases) per salesperson, "
            "with detail by store and UPC."
        )
    else:
        st.caption("Weekly truck load forecast (cases) per salesperson, with detail by store and UPC.")

    # ---- Tenant / Connection ----
    if "tenant_config" not in st.session_state:
        st.error("Tenant configuration not found in session. Please login/select a tenant.")
        return

    tenant = st.session_state["tenant_config"]
    try:
        conn = connect_to_tenant_snowflake(tenant)
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return

    db, sch = tenant.get("database"), tenant.get("schema")
    if not db or not sch:
        try:
            db, sch = _ensure_tenant_context(conn)
        except RuntimeError as e:
            st.error(str(e))
            return

    # ---- Inputs (form to avoid full reruns) ----
    with st.form("truck_plan_form", clear_on_submit=False):
        # Default next Monday (ISO Monday = 1)
        today = date.today()
        next_monday = today + timedelta(days=(7 - today.isoweekday() + 1) % 7 or 7)

        target_week = st.date_input("Week start (Monday)", value=next_monday)
        horizon_weeks = st.slider("Horizon (weeks)", min_value=1, max_value=4, value=1, step=1)

        # Salesperson dropdown (from SALES_RAW_IMPORT via utils.forecasting_truck)
        try:
            sp_options = ["All salespeople"] + fetch_distinct_salespeople(conn, db, sch)
        except Exception:
            st.warning("Could not load salesperson list from SALES_RAW_IMPORT; defaulting to all.")
            sp_options = ["All salespeople"]
        salesperson_choice = st.selectbox("Salesperson", sp_options, index=0)

        submitted = st.form_submit_button("Run Forecast")

    if not submitted:
        st.info("Choose a week and horizon, select a salesperson (optional), then run the forecast.")
        return

    # ---- Build Scope ----
    with st.spinner("Loading route scope..."):
        scope_df = fetch_route_scope(conn, db, sch)  # SALESPERSON, STORE_NUMBER, CHAIN_NAME, STORE_NAME, UPC
        scope_df["UPC"] = scope_df["UPC"].astype(str)
        scope_df = scope_df.drop_duplicates(subset=["SALESPERSON", "STORE_NUMBER", "UPC"])

        if salesperson_choice and salesperson_choice != "All salespeople":
            scope_df = scope_df[scope_df["SALESPERSON"] == salesperson_choice]

        if scope_df.empty:
            st.warning("No route scope found for the selected salesperson.")
            return

    # ---- Fetch Weekly Sales ----
    with st.spinner("Fetching 90-day weekly sales..."):
        # asof_date = day before target week to avoid bleeding into horizon
        weekly_sales_df = fetch_90d_weekly_sales(
            conn,
            db,
            sch,
            scope_df,
            asof_date=target_week - timedelta(days=1),
        )

    # Optional visibility into detected date/measure sources
    try:
        date_src = get_sales_date_source(conn, db, sch)
        measure_src = get_sales_measure_source(conn, db, sch)
        st.caption(f"Using date: {date_src} • measure: {measure_src}")
    except Exception:
        pass

    if weekly_sales_df.empty:
        st.warning("No sales history found for the selected scope/horizon. Nothing to forecast.")
        return

    # ---- Forecast per (store × UPC) ----
    with st.spinner("Forecasting per store × UPC..."):
        detail_df = build_truck_plan_detail(scope_df, weekly_sales_df, horizon_weeks, target_week)

    if detail_df.empty:
        st.warning("Forecast produced no rows. Scope may be empty after filters or baseline was zero.")
        return

    # ---- Summaries ----
    summary = (
        detail_df.groupby(["SALESPERSON"], as_index=False)
        .agg(
            TOTAL_CASES=("Pred", "sum") if "Pred" in detail_df.columns else ("PRED_CASES", "sum"),
            STORES=("STORE_NUMBER", "nunique"),
            SKUS=("UPC", "nunique"),
        )
        .sort_values("TOTAL_CASES", ascending=False)
    )

    st.subheader("Summary by Salesperson")
    st.dataframe(summary, use_container_width=True)

    # ---- Detail Grid ----
    st.subheader("Detail (Store → UPC)")

    # Align columns as: Store #, Chain, Store, UPC, Product Name, Pred, Lo, Hi
    display_df = detail_df.copy()

    # Ensure column exists even if missing in baseline
    if "PRODUCT_NAME" not in display_df.columns:
        display_df["PRODUCT_NAME"] = None

    display_df = display_df[[
        "STORE_NUMBER",
        "CHAIN_NAME",
        "STORE_NAME",
        "UPC",
        "PRODUCT_NAME",
        "PRED_CASES",
        "PRED_CASES_LO",
        "PRED_CASES_HI",
    ]].rename(
        columns={
            "STORE_NUMBER": "Store #",
            "CHAIN_NAME": "Chain",
            "STORE_NAME": "Store",
            "UPC": "UPC",
            "PRODUCT_NAME": "Product Name",
            "PRED_CASES": "Pred",
            "PRED_CASES_LO": "Lo",
            "PRED_CASES_HI": "Hi",
        }
    )

    st.dataframe(display_df, use_container_width=True)

    # ---- Exports ----
    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name=f"truck_plan_{uuid.uuid4().hex}.csv",
        mime="text/csv",
    )

    pdf_bytes = build_predictive_truck_pdf(
        week_start=target_week,
        horizon_weeks=horizon_weeks,
        summary_df=summary,
        detail_df=detail_df,
        tenant_id=tenant_id,
        run_id=None
    )
    st.download_button(
        "Download PDF",
        data=pdf_bytes,
        file_name=f"truck_plan_{uuid.uuid4().hex}.pdf",
        mime="application/pdf",
    )
