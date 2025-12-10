# ---------- gap_report.py ----------

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils.reports_utils import create_gap_report
from utils.snowflake_utils import  fetch_distinct_values

def render():
    st.title("Gap Report Generator")

    conn = st.session_state.get("conn")
    if not conn:
        st.error("? Database connection not available.")
        return

    # Fetch filter options
    try:
        salesperson_options = fetch_distinct_values(conn, "SALESPERSON", "SALESPERSON")
        store_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        supplier_options = fetch_distinct_values(conn, "SUPPLIER_COUNTY", "SUPPLIER")
    except Exception as e:
        st.error(f"? Failed to fetch filter values: {e}")
        return

    for options in [salesperson_options, store_options, supplier_options]:
        options.sort()
        options.insert(0, "All")

    with st.form(key=f"Gap_Report_{st.session_state.get('user_email', 'default')}", clear_on_submit=True):
        salesperson = st.selectbox("Filter by Salesperson", salesperson_options)
        store = st.selectbox("Filter by Chain", store_options)
        supplier = st.selectbox("Filter by Supplier", supplier_options)
        submitted = st.form_submit_button("Generate Gap Report")

   



    if submitted:
        with st.spinner("Generating report..."):
            temp_file_path = create_gap_report(conn, salesperson, store, supplier)

            if not temp_file_path:
                st.error("Report generation failed.")
                return

            with open(temp_file_path, "rb") as f:
                bytes_data = f.read()

            today = datetime.today().strftime("%Y-%m-%d")
            st.download_button(
                label="Download Gap Report",
                data=bytes_data,
                file_name=f"Gap_Report_{today}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # ? Clean up temp file
            try:
                os.remove(temp_file_path)
            except Exception as e:
                st.warning(f"Failed to delete temporary file: {e}")

