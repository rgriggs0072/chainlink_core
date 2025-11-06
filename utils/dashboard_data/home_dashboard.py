# utils/dashboard_data/home_dashboard.py

import pandas as pd
import streamlit as st
from typing import List

def q(db: str, sch: str, obj: str) -> str:
    """Qualify an object with database and schema."""
    return f'{db}.{sch}.{obj}'

def fetch_salesperson_execution_summary(conn, tenant_config):
    try:
        db, sch = tenant_config["database"], tenant_config["schema"]
        query = f"""
            SELECT *
            FROM {q(db, sch, "SALESPERSON_EXECUTION_SUMMARY")}
            ORDER BY EXECUTION_PERCENTAGE DESC
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error("❌ Failed to fetch Salesperson Execution Summary")
        st.exception(e)
        return pd.DataFrame()

def fetch_chain_schematic_data(conn, tenant_config):
    try:
        db, sch = tenant_config["database"], tenant_config["schema"]
        query = f"""
            SELECT
                CHAIN_NAME,
                SUM("In_Schematic") AS "Total_In_Schematic",
                SUM("PURCHASED_YES_NO") AS "Purchased",
                COALESCE(
                    SUM("PURCHASED_YES_NO") / NULLIF(COUNT(*), 0), 0
                )::FLOAT AS "Purchased_Percentage"
            FROM {db}.{sch}.GAP_REPORT
            GROUP BY CHAIN_NAME
            ORDER BY "Purchased_Percentage" DESC;
        """
        df = pd.read_sql(query, conn)

        # Keep this if downstream expects numeric
        df["Purchased_Percentage"] = pd.to_numeric(df["Purchased_Percentage"], errors="coerce")

        # If you want the old display format (e.g., "12.34%"), add:
        # df["Purchased_Percentage"] = (df["Purchased_Percentage"] * 100).round(2).astype(str) + "%"

        return df
    except Exception as e:
        st.error("❌ Failed to fetch chain schematic data")
        st.exception(e)
        return pd.DataFrame()


def fetch_supplier_schematic_summary_data(conn, selected_suppliers):
 
    try:
        if not selected_suppliers:
            return pd.DataFrame()

        # Quote supplier names for SQL injection safety (this assumes input is clean, trusted)
        supplier_list = ", ".join(f"'{s}'" for s in selected_suppliers)

        query = f"""
            SELECT 
                PRODUCT_NAME,
                "dg_upc" AS UPC,
                SUM("In_Schematic") AS "Total_In_Schematic",
                SUM("PURCHASED_YES_NO") AS "Total_Purchased",
                ROUND(SUM("PURCHASED_YES_NO") / NULLIF(SUM("In_Schematic"), 0) * 100, 2) AS "Purchased_Percentage"
            FROM GAP_REPORT_TMP2
            WHERE "sc_STATUS" = 'Yes'
              AND SUPPLIER IN ({supplier_list})
            GROUP BY PRODUCT_NAME, "dg_upc"
            ORDER BY "Purchased_Percentage" DESC
        """

        df = pd.read_sql(query, conn)

        # Clean + Convert
        for col in ["Total_In_Schematic", "Total_Purchased", "Purchased_Percentage"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    except Exception as e:
        st.error("❌ Failed to fetch supplier schematic summary data")
        st.exception(e)
        return pd.DataFrame()



def create_gap_report(conn, salesperson: str, store: str, supplier: str) -> pd.DataFrame:
    try:
        # Step 1: Trigger stored procedure
        with conn.cursor() as cur:
            cur.execute("CALL PROCESS_GAP_REPORT()")

        # Step 2: Build query dynamically with filters
        query = "SELECT * FROM GAP_REPORT WHERE 1=1"
        if salesperson != "All":
            query += f" AND SALESPERSON = '{salesperson}'"
        if store != "All":
            query += f" AND STORE_NAME = '{store}'"
        if supplier != "All":
            query += f" AND SUPPLIER = '{supplier}'"

        return pd.read_sql(query, conn)
    except Exception as e:
        st.error("❌ Failed to generate Gap Report")
        st.exception(e)
        return pd.DataFrame()


def fetch_distinct_values(conn, table_name, column_name):
    try:
        cur = conn.cursor()
        query = f"SELECT DISTINCT {column_name} FROM {table_name} ORDER BY {column_name}"
        cur.execute(query)
        results = [row[0] for row in cur.fetchall()]
        cur.close()
        return results
    except Exception as e:
        return [f"⚠ Error: {e}"]



def get_execution_summary(conn):
    if not conn:
        return 0, 0, 0, "0.00"

    cursor = conn.cursor()
    query = """
        SELECT 
            SUM("In_Schematic") AS TOTAL_IN_SCHEMATIC,
            SUM("PURCHASED_YES_NO") AS PURCHASED,
            (SUM("PURCHASED_YES_NO") / NULLIF(COUNT(*),0)) AS PURCHASED_PERCENTAGE
        FROM GAP_REPORT;
    """

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
       # conn.close()

        if result and any(result):
            total_in_schematic = result[0] or 0
            purchased = result[1] or 0
            purchased_percentage = result[2] or 0
            formatted_percentage = purchased_percentage * 100

            total_gaps = total_in_schematic - purchased

            return total_in_schematic, purchased, total_gaps, formatted_percentage
        else:
            return 0, 0, 0, "0.00"
    except Exception as e:
       
        st.error(f"Query Error: {str(e)}")
        return 0, 0, 0, "0.00"



def fetch_salesperson_summary(conn):
  
    # TODO: Replace with actual query logic
    data = [
        {"SALESPERSON": "Alice", "TOTAL_DISTRIBUTION": 100, "TOTAL_GAPS": 10, "EXECUTION_PERCENTAGE": 90},
        {"SALESPERSON": "Bob", "TOTAL_DISTRIBUTION": 80, "TOTAL_GAPS": 20, "EXECUTION_PERCENTAGE": 75}
    ]
    return pd.DataFrame(data)




def fetch_supplier_names(conn) -> List[str]:
    try:
        query = "SELECT DISTINCT SUPPLIER FROM SUPPLIER_COUNTY ORDER BY SUPPLIER"
        df = pd.read_sql(query, conn)
        return df["SUPPLIER"].dropna().tolist()
    except Exception as e:
        st.error("❌ Failed to fetch supplier names")
        st.exception(e)
        return []
