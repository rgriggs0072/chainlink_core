
# test_pages/test_distro_grid.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils.distro_grid_helpers import (
    format_non_pivot_table,
    format_pivot_table,
    upload_distro_grid_to_snowflake
)
from utils.snowflake_utils import fetch_distinct_values

TEST_MODE = True  # Toggle to skip real uploads

st.title("🧪 Distro Grid Test Harness")
st.caption("Test the formatting and upload pipeline for both Standard and Pivot formats.")

# ---------------------------
# Select Chain
# ---------------------------
conn = st.session_state.get("conn")
if not conn:
    st.error("❌ Snowflake connection not available.")
    st.stop()

try:
    chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
    chain_options.sort()
    selected_chain = st.selectbox("Select Test Chain", chain_options, key="test_distro_grid_chain")
except Exception as e:
    st.error(f"❌ Failed to load chain names: {e}")
    st.stop()

# ---------------------------
# Upload File
# ---------------------------
format_type = st.radio("Choose Test Format", ["Standard", "Pivot"], horizontal=True)
test_file = st.file_uploader("Upload Test Spreadsheet", type=["xlsx"], key="test_distro_upload")

if test_file and selected_chain:
    import openpyxl
    workbook = openpyxl.load_workbook(test_file)

    with st.spinner("📄 Formatting test spreadsheet..."):
        try:
            if format_type == "Standard":
                df = format_non_pivot_table(workbook)
            else:
                df = format_pivot_table(workbook, selected_chain)
            st.success("✅ Formatting successful.")
            st.dataframe(df.head())
        except Exception as e:
            st.error(f"❌ Format failed: {e}")
            st.stop()

    if st.button("🚀 Run Upload (Test Mode)"):
        if TEST_MODE:
            st.warning("⚠️ Test Mode Enabled. No real data will be affected.")
            st.code(df.head().to_csv(index=False))
        else:
            with st.spinner("📤 Uploading to Snowflake..."):
                upload_distro_grid_to_snowflake(df, selected_chain, st.text)

    # Optional snapshot for visual comparison
    if st.checkbox("Show Current Snowflake Snapshot"):
        try:
            query = f"SELECT * FROM DISTRO_GRID WHERE CHAIN_NAME = '{selected_chain}' LIMIT 100"
            snapshot = pd.read_sql(query, conn)
            st.dataframe(snapshot)
        except Exception as e:
            st.error(f"Snapshot error: {e}")
