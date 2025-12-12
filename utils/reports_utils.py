# ---------------- utils/reports_utils.py ----------------

import streamlit as st
import pandas as pd
from datetime import datetime
from sf_connector.service_connector import connect_to_tenant_snowflake



def fetch_distinct_values(conn, table_name, column_name):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    df = pd.read_sql(query, conn)
    return df[column_name].dropna().tolist()



def create_gap_report(conn, salesperson, chain, supplier):
    toml_info = st.session_state.get('toml_info')
    if not toml_info:
        st.error("❌ Tenant configuration not loaded.")
        return None

    required_keys = ["account", "snowflake_user", "private_key", "database", "schema", "warehouse"]
    missing_keys = [k for k in required_keys if not toml_info.get(k)]
    if missing_keys:
        st.error(f"❌ TOML configuration is incomplete. Missing: {', '.join(missing_keys)}")
        return None

    try:
        # 🔁 Use the same connection to run the procedure
        cursor = conn.cursor()
        cursor.execute(f"CALL {toml_info['database']}.{toml_info['schema']}.PROCESS_GAP_REPORT()")
        cursor.close()
    except Exception as e:
        st.error(f"❌ Error calling stored procedure: {e}")
        return None

    # 🔽 Now query from the view
    filters = []
    if salesperson != "All":
        filters.append(f"SALESPERSON = '{salesperson}'")
    if chain != "All":
        filters.append(f"CHAIN_NAME = '{chain}'")
    if supplier != "All":
        filters.append(f"SUPPLIER = '{supplier}'")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    #st.write("what the heck is the where clause", where_clause)
    query = f"SELECT * FROM GAP_REPORT {where_clause}"

    df = pd.read_sql(query, conn)

    temp_file_path = f"temp_gap_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(temp_file_path, index=False)
    return temp_file_path
