# ---------------- utils/reports_utils.py ----------------

import streamlit as st
import pandas as pd
from datetime import datetime
from sf_connector.service_connector import connect_to_tenant_snowflake



from utils.gap_report_builder import create_gap_report as _create_gap_report



def fetch_distinct_values(conn, table_name, column_name):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    df = pd.read_sql(query, conn)
    return df[column_name].dropna().tolist()





def create_gap_report(conn, salesperson, chain, supplier):
    """
    Streamlit wrapper for create_gap_report().

    Page overview:
    - Reads toml_info from session_state to build fully-qualified procedure/view names.
    - Delegates actual work to utils.gap_report_builder (Streamlit-free).
    """
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("❌ Tenant configuration not loaded.")
        return None

    db = toml_info.get("database")
    sch = toml_info.get("schema")
    if not db or not sch:
        st.error("❌ TOML configuration missing database/schema.")
        return None

    proc_fqn = f"{db}.{sch}.PROCESS_GAP_REPORT"
    view_fqn = f"{db}.{sch}.GAP_REPORT"

    try:
        return _create_gap_report(
            conn,
            salesperson=salesperson,
            chain=chain,
            supplier=supplier,
            proc_fqn=proc_fqn,
            view_fqn=view_fqn,
        )
    except Exception as e:
        st.error(f"❌ Gap report build failed: {e}")
        return None
