# utils/forecasting_truck.py
# -*- coding: utf-8 -*-
"""
Truck forecasting utilities (SALES_RAW_IMPORT-based)

Overview
--------
These helpers power the Predictive Truck Plan page. This version is fully
rewired to use SALES_RAW_IMPORT as the single source of truth for history.

Key assumptions:
- SALES_RAW_IMPORT columns:
    SALESPERSON, TX_DATE, UPC, PRODUCT_ID, PRODUCT_NAME, SUPPLIER,
    UNITS_SOLD, REVENUE, STORE_NUMBER, CHAIN_NAME, PACKAGE,
    PRODUCT_MANAGER, CATEGORY, SEGMENT, CURRENCY, VENDOR_DOC_ID, RAW_JSON

- PRODUCTS is only used upstream to populate SALES_RAW_IMPORT; it is NOT
  required here.

Functions
---------
- fetch_distinct_salespeople: dropdown options for salesperson filter.
- fetch_route_scope: unique (SALESPERSON, STORE_NUMBER, UPC, CHAIN_NAME, STORE_NAME).
- fetch_90d_weekly_sales: last 90 days of weekly volume by store × UPC.
- build_truck_plan_detail: rolling 4-week baseline → horizon forecast.
- get_sales_date_source / get_sales_measure_source: informational only.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List

import pandas as pd


# ---------------------------------------------------------------------------
# Helper: run a simple query and return all rows
# ---------------------------------------------------------------------------
def _run_query(conn, sql: str, params: tuple | None = None) -> list[tuple]:
    """Execute a SQL statement and return all rows as a list of tuples."""
    with conn.cursor() as cur:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Public: salesperson list
# ---------------------------------------------------------------------------
def fetch_distinct_salespeople(conn, db: str, sch: str) -> List[str]:
    """
    Return distinct salesperson names from SALES_RAW_IMPORT for the tenant.

    Excludes admin / non-route buckets ('NON BUY', 'NEW ACCOUNTS').
    """
    with conn.cursor() as cur:
        # Make sure we're in the right DB/SCHEMA for the tenant.
        cur.execute(f'USE DATABASE "{db}"')
        cur.execute(f'USE SCHEMA "{sch}"')
        cur.execute(
            """
            SELECT DISTINCT SALESPERSON
            FROM SALES_RAW_IMPORT
            WHERE SALESPERSON IS NOT NULL
              AND TRIM(SALESPERSON) <> ''
              AND UPPER(SALESPERSON) NOT IN ('NON BUY', 'NEW ACCOUNTS')
            ORDER BY SALESPERSON
            """
        )
        rows = cur.fetchall()

    return [r[0] for r in rows if r[0] is not None]


# ---------------------------------------------------------------------------
# Public: route scope
# ---------------------------------------------------------------------------
def fetch_route_scope(conn, db: str, sch: str) -> pd.DataFrame:
    """
    Build the "route scope" for forecasting.

    Each row represents a unique (SALESPERSON, STORE_NUMBER, UPC, CHAIN_NAME).
    STORE_NAME is left as NULL for now; you can later join to CUSTOMERS if
    you want a human-readable store name.

    Source: SALES_RAW_IMPORT
    """
    with conn.cursor() as cur:
        cur.execute(f'USE DATABASE "{db}"')
        cur.execute(f'USE SCHEMA "{sch}"')
        cur.execute(
            """
            SELECT DISTINCT
                SALESPERSON,
                STORE_NUMBER,
                CHAIN_NAME,
                CAST(NULL AS VARCHAR) AS STORE_NAME,
                UPC
            FROM SALES_RAW_IMPORT
            WHERE TX_DATE IS NOT NULL
              AND UPC IS NOT NULL
              AND SALESPERSON IS NOT NULL
            """
        )
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]

    scope_df = pd.DataFrame(rows, columns=cols)

    # Normalize types explicitly for downstream logic.
    if not scope_df.empty:
        scope_df["STORE_NUMBER"] = scope_df["STORE_NUMBER"].astype(str)
        scope_df["UPC"] = scope_df["UPC"].astype(str)

    return scope_df


# ---------------------------------------------------------------------------
# Public: last 90 days of weekly sales
# ---------------------------------------------------------------------------
def fetch_90d_weekly_sales(
    conn,
    db: str,
    sch: str,
    scope_df: pd.DataFrame,
    asof_date: date,
) -> pd.DataFrame:
    """
    Fetch 90-day weekly sales history for all route scope combinations.

    Returns columns:
        SALESPERSON, STORE_NUMBER, CHAIN_NAME,
        UPC, PRODUCT_NAME, SUPPLIER,
        WEEK_START_DATE, WK_UNITS
    """
    start_date = asof_date - timedelta(days=90)

    with conn.cursor() as cur:
        cur.execute(f'USE DATABASE "{db}"')
        cur.execute(f'USE SCHEMA "{sch}"')
        cur.execute(
            """
            SELECT
                SALESPERSON,
                STORE_NUMBER,
                CHAIN_NAME,
                UPC,
                PRODUCT_NAME,
                SUPPLIER,
                DATE_TRUNC('WEEK', TX_DATE) AS WEEK_START_DATE,
                SUM(UNITS_SOLD)            AS WK_UNITS
            FROM SALES_RAW_IMPORT
            WHERE TX_DATE >= %s
              AND TX_DATE <= %s
              AND UPC IS NOT NULL
              AND SALESPERSON IS NOT NULL
            GROUP BY
                SALESPERSON,
                STORE_NUMBER,
                CHAIN_NAME,
                UPC,
                PRODUCT_NAME,
                SUPPLIER,
                DATE_TRUNC('WEEK', TX_DATE)
            ORDER BY
                SALESPERSON,
                STORE_NUMBER,
                CHAIN_NAME,
                UPC,
                WEEK_START_DATE
            """,
            (start_date, asof_date),
        )
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]

    weekly_df = pd.DataFrame(rows, columns=cols)

    if weekly_df.empty:
        return weekly_df

    weekly_df["STORE_NUMBER"] = weekly_df["STORE_NUMBER"].astype(str)
    weekly_df["UPC"] = weekly_df["UPC"].astype(str)
    weekly_df["WEEK_START_DATE"] = pd.to_datetime(
        weekly_df["WEEK_START_DATE"]
    ).dt.date

    return weekly_df


# ---------------------------------------------------------------------------
# Public: build truck plan detail
# ---------------------------------------------------------------------------
def build_truck_plan_detail(
    scope_df: pd.DataFrame,
    weekly_sales_df: pd.DataFrame,
    horizon_weeks: int,
    target_week: date,
) -> pd.DataFrame:
    """
    Build per-store × UPC truck plan detail.

    Forecast model
    --------------
    - For each (SALESPERSON, STORE_NUMBER, CHAIN_NAME, UPC, PRODUCT_NAME, SUPPLIER):
        • Use weekly WK_UNITS over ~90 days.
        • Compute a 4-week rolling mean (MA4_UNITS).
        • Take the most recent MA4_UNITS as BASELINE_UNITS.
    - For each horizon week h = 1..horizon_weeks:
        • PRED_CASES     = BASELINE_UNITS
        • PRED_CASES_LO  = BASELINE_UNITS * 0.9
        • PRED_CASES_HI  = BASELINE_UNITS * 1.1
        • WEEK_START_DATE = target_week + 7 * (h-1)
        • MODEL_NAME     = 'MA4_SIMPLE'
    """
    if weekly_sales_df.empty:
        return pd.DataFrame()

    # Keys that uniquely describe an item at a store for a salesperson
    group_keys = [
        "SALESPERSON",
        "STORE_NUMBER",
        "CHAIN_NAME",
        "UPC",
        "PRODUCT_NAME",
        "SUPPLIER",
    ]

    weekly_sales_df = weekly_sales_df.copy()
    weekly_sales_df["STORE_NUMBER"] = weekly_sales_df["STORE_NUMBER"].astype(str)
    weekly_sales_df["UPC"] = weekly_sales_df["UPC"].astype(str)

    # Sort for rolling calculations.
    weekly_sales_df = weekly_sales_df.sort_values(group_keys + ["WEEK_START_DATE"])

    # Rolling 4-week average using transform to avoid index issues.
    weekly_sales_df["MA4_UNITS"] = (
        weekly_sales_df
        .groupby(group_keys, dropna=False)["WK_UNITS"]
        .transform(lambda s: s.rolling(window=4, min_periods=1).mean())
    )

    # Most recent MA4 per key becomes the baseline.
    last_rows = (
        weekly_sales_df
        .sort_values("WEEK_START_DATE")
        .groupby(group_keys, as_index=False)
        .tail(1)
    )
    baseline_df = last_rows.rename(columns={"MA4_UNITS": "BASELINE_UNITS"})

    # Bring in STORE_NAME from scope (if present).
    scope_cols = ["SALESPERSON", "STORE_NUMBER", "CHAIN_NAME", "UPC", "STORE_NAME"]
    scope_trim = (
        scope_df[scope_cols].drop_duplicates()
        if not scope_df.empty
        else pd.DataFrame(columns=scope_cols)
    )

    merged = baseline_df.merge(
        scope_trim,
        on=["SALESPERSON", "STORE_NUMBER", "CHAIN_NAME", "UPC"],
        how="left",
    )

    # Build forecast rows for each horizon week.
    records: list[dict] = []
    for _, row in merged.iterrows():
        baseline = float(row["BASELINE_UNITS"] or 0.0)
        if baseline <= 0:
            # Skip dead items (no meaningful history).
            continue

        product_name = row.get("PRODUCT_NAME")

        for h in range(1, horizon_weeks + 1):
            week_start = target_week + timedelta(weeks=h - 1)
            records.append(
                {
                    "SALESPERSON": row["SALESPERSON"],
                    "CHAIN_NAME": row["CHAIN_NAME"],
                    "STORE_NAME": row.get("STORE_NAME"),
                    "STORE_NUMBER": str(row["STORE_NUMBER"]),
                    "UPC": str(row["UPC"]),
                    "PRODUCT_NAME": product_name,
                    "WEEK_START_DATE": week_start,
                    "HORIZON_WEEKS": h,
                    "PRED_CASES": baseline,
                    "PRED_CASES_LO": baseline * 0.9,
                    "PRED_CASES_HI": baseline * 1.1,
                    "MODEL_NAME": "MA4_SIMPLE",
                }
            )

    detail_df = pd.DataFrame.from_records(records)

    if detail_df.empty:
        return detail_df

    detail_df["STORE_NUMBER"] = detail_df["STORE_NUMBER"].astype(str)
    detail_df["UPC"] = detail_df["UPC"].astype(str)

    return detail_df




# ---------------------------------------------------------------------------
# Informational helpers (for the caption)
# ---------------------------------------------------------------------------
def get_sales_date_source(conn, db: str, sch: str) -> str:
    """
    Return the name of the date column used for sales history.

    In the SALES_RAW_IMPORT world this is statically 'TX_DATE'.
    """
    return "TX_DATE"


def get_sales_measure_source(conn, db: str, sch: str) -> str:
    """
    Return the name of the measure column used for sales history.

    In the SALES_RAW_IMPORT world this is statically 'UNITS_SOLD'.
    """
    return "UNITS_SOLD"
