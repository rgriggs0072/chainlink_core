# ---------------- reset_schedule_sections.py ----------------

import streamlit as st
import pandas as pd
import openpyxl
from datetime import datetime
from io import BytesIO

from utils.reset_schedule_helpers import format_reset_schedule, upload_reset_data
from utils.ui_helpers import download_workbook
from utils.snowflake_utils import fetch_distinct_values


# ---------------- reset_schedule_sections.py ----------------

import streamlit as st
import pandas as pd
import openpyxl
from datetime import datetime
from io import BytesIO

from utils.reset_schedule_helpers import format_reset_schedule, upload_reset_data
from utils.ui_helpers import download_workbook
from utils.snowflake_utils import fetch_distinct_values


def render_reset_schedule_formatter_section():
    st.subheader("📄 Format Reset Schedule File")
    uploaded_file = st.file_uploader("Upload Reset Schedule Excel", type=["xlsx"], key="reset_schedule_upload")

    if uploaded_file:
        try:
            workbook = openpyxl.load_workbook(uploaded_file)
            with st.spinner("🔄 Formatting reset schedule..."):
                formatted_wb = format_reset_schedule(workbook)

            if formatted_wb:
                st.success("✅ Formatting complete. Download to review before upload.")
                download_workbook(formatted_wb, "Formatted_Reset_Schedule.xlsx")
        except Exception as e:
            st.error(f"❌ Failed to format reset schedule: {e}")


def render_reset_schedule_uploader_section():
    st.subheader("📤 Upload Reset Schedule to Snowflake")

    # Use chain names from CUSTOMERS table, just like in Gap Report
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Snowflake connection not found.")
        return

    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"❌ Could not load chain names: {e}")
        return

    selected_chain = st.selectbox("Select Chain Name", chain_options, key="reset_schedule_chain_select")
    uploaded_file = st.file_uploader("Upload Formatted Reset Schedule File", type=["xlsx"], key="reset_schedule_final_upload")

    if uploaded_file and selected_chain:
        try:
            df = pd.read_excel(uploaded_file, engine="openpyxl")
            st.write("📋 Preview of formatted reset schedule:")
            st.dataframe(df.head())

            if st.button("Upload Reset Schedule to Snowflake", key="upload_reset_schedule_btn"):
                with st.spinner("📤 Uploading to Snowflake..."):
                    upload_reset_data(df, selected_chain)
        except Exception as e:
            st.error(f"❌ Failed to upload reset schedule: {e}")
