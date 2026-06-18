# utils/dashboard_data/home_dashboard.py
import pandas as pd
import streamlit as st
from typing import List


def _q(*parts: str) -> str:
    return ".".join(f'"{p}"' for p in parts if p)


@st.cache_data(ttl=300, show_spinner=False)
def get_execution_summary(_conn, tenant_id: str) -> tuple:
    """
    Home page execution summary. Cached per tenant for 5 minutes.
    Returns (total_in_schematic, purchased, total_gaps, purchased_pct_float).
    tenant_id is the cache-key discriminator; _conn is excluded from hashing.
    """
    if not _conn:
        return 0, 0, 0, 0.0

    query = """
        SELECT
            SUM("In_Schematic")                                             AS TOTAL_IN_SCHEMATIC,
            SUM(COALESCE("PURCHASED_YES_NO", 0))                            AS PURCHASED,
            SUM(CASE WHEN COALESCE("PURCHASED_YES_NO", 0) = 0 THEN 1 ELSE 0 END) AS TOTAL_GAPS,
            SUM(COALESCE("PURCHASED_YES_NO", 0)) / NULLIF(COUNT(*), 0)     AS PURCHASED_PERCENTAGE
        FROM GAP_REPORT
    """
    try:
        with _conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
        if row and any(row):
            return (
                row[0] or 0,
                row[1] or 0,
                row[2] or 0,
                float(row[3] or 0) * 100,
            )
        return 0, 0, 0, 0.0
    except Exception as e:
        st.error(f"Execution summary query error: {e}")
        return 0, 0, 0, 0.0


@st.cache_data(ttl=300, show_spinner=False)
def fetch_chain_schematic_data(_conn, tenant_config: dict) -> pd.DataFrame:
    """Chain-level in-schematic / purchased summary. Cached for 5 minutes."""
    if _conn is None or not tenant_config:
        return pd.DataFrame()
    db = tenant_config.get("database")
    sch = tenant_config.get("schema")
    if not db or not sch:
        return pd.DataFrame()
    query = f"""
        SELECT
            CHAIN_NAME,
            SUM("In_Schematic")                                                           AS "Total_In_Schematic",
            SUM(COALESCE("PURCHASED_YES_NO", 0))                                          AS "Purchased",
            COALESCE(
                SUM(COALESCE("PURCHASED_YES_NO", 0)) / NULLIF(SUM("In_Schematic"), 0), 0
            )::FLOAT AS "Purchased_Percentage"
        FROM {_q(db, sch, "GAP_REPORT")}
        GROUP BY CHAIN_NAME
        ORDER BY "Purchased_Percentage" DESC
    """
    try:
        df = pd.read_sql(query, _conn)
        df["Purchased_Percentage"] = pd.to_numeric(df["Purchased_Percentage"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Chain schematic query error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_salesperson_execution(_conn, tenant_id: str) -> pd.DataFrame:
    """Salesperson execution summary. Cached per tenant for 5 minutes."""
    if not _conn:
        return pd.DataFrame()
    query = """
        SELECT SALESPERSON, TOTAL_DISTRIBUTION, TOTAL_GAPS, EXECUTION_PERCENTAGE
        FROM SALESPERSON_EXECUTION_SUMMARY
        ORDER BY TOTAL_GAPS DESC
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.error(f"Salesperson execution query error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_gap_history(_conn, tenant_id: str) -> pd.DataFrame:
    """Gap history pivot data. Cached per tenant for 5 minutes."""
    if not _conn:
        return pd.DataFrame()
    query = """
        SELECT SALESPERSON, TOTAL_GAPS, EXECUTION_PERCENTAGE, LOG_DATE
        FROM SALESPERSON_EXECUTION_SUMMARY_TBL
        ORDER BY TOTAL_GAPS DESC
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.error(f"Gap history query error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_supplier_schematic_summary_data(_conn, tenant_id: str, selected_suppliers: tuple) -> pd.DataFrame:
    """
    Supplier performance scatter data. Cached per tenant for 5 minutes.
    selected_suppliers must be a tuple (hashable) not a list.
    """
    if not _conn or not selected_suppliers:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(selected_suppliers))
    query = f"""
        SELECT
            PRODUCT_NAME,
            "dg_upc"                                                                  AS UPC,
            SUM("In_Schematic")                                                       AS "Total_In_Schematic",
            SUM(COALESCE("PURCHASED_YES_NO", 0))                                      AS "Total_Purchased",
            ROUND(
                SUM(COALESCE("PURCHASED_YES_NO", 0)) / NULLIF(SUM("In_Schematic"), 0) * 100,
                2
            )                                                                         AS "Purchased_Percentage"
        FROM GAP_REPORT_TMP2
        WHERE "sc_STATUS" = 'Yes'
          AND SUPPLIER IN ({placeholders})
        GROUP BY PRODUCT_NAME, "dg_upc"
        ORDER BY "Purchased_Percentage" DESC
    """
    try:
        with _conn.cursor() as cur:
            cur.execute(query, list(selected_suppliers))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        for col in ["Total_In_Schematic", "Total_Purchased", "Purchased_Percentage"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Supplier scatter query error: {e}")
        return pd.DataFrame()


def create_gap_report(conn, salesperson: str, chain: str, supplier: str) -> pd.DataFrame:
    try:
        with conn.cursor() as cur:
            cur.execute("CALL PROCESS_GAP_REPORT()")
        query = "SELECT * FROM GAP_REPORT WHERE 1=1"
        params = []
        if salesperson != "All":
            query += " AND SALESPERSON = %s"
            params.append(salesperson)
        if chain != "All":
            query += " AND CHAIN_NAME = %s"
            params.append(chain)
        if supplier != "All":
            query += " AND SUPPLIER = %s"
            params.append(supplier)
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Gap report error: {e}")
        return pd.DataFrame()


def fetch_distinct_values(conn, table_name: str, column_name: str) -> List:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT {column_name} FROM {table_name} ORDER BY {column_name}")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        return [f"Error: {e}"]


def fetch_supplier_names(conn) -> List[str]:
    try:
        query = "SELECT DISTINCT SUPPLIER FROM SUPPLIER_COUNTY ORDER BY SUPPLIER"
        df = pd.read_sql(query, conn)
        return df["SUPPLIER"].dropna().tolist()
    except Exception as e:
        st.error(f"Failed to fetch supplier names: {e}")
        return []
