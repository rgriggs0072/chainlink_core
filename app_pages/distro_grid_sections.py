
# ---------------- distro_grid_sections.py ----------------

import streamlit as st
import pandas as pd
import openpyxl
from io import BytesIO

from utils.distro_grid_helpers import (
    format_non_pivot_table,
    format_pivot_table,
    upload_distro_grid_to_snowflake
)
from utils.ui_helpers import download_workbook
from utils.snowflake_utils import fetch_distinct_values





def render_distro_grid_formatter_section():
    st.subheader("📄 Format Distribution Grid Spreadsheet")

    st.write("📥 Download Template Files:")
    st.markdown("[📊 Pivot Table Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Pivot_Table_Distro_Grid_Template.xlsx)")
    st.markdown("[📋 Standard Distro Grid Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Distribution_Grid_Template.xlsx)")

    selected_format = st.selectbox("Select Spreadsheet Format", ["Standard Column Format", "Pivot Table Format"], key="distro_grid_format_select")

    uploaded_file = st.file_uploader("Upload Distribution Grid Excel File", type=["xlsx"], key="distro_grid_upload")

    if uploaded_file:
        try:
            workbook = openpyxl.load_workbook(uploaded_file)
            selected_option = st.session_state.get("selected_option", "Unknown Chain")

            with st.spinner("🔄 Formatting distribution grid..."):
                if selected_format == "Standard Column Format":
                    formatted_wb = format_non_pivot_table(workbook)
                else:
                    formatted_wb = format_pivot_table(workbook, selected_option)

            if formatted_wb:
                st.success("✅ Formatting complete. You may download to review before uploading.")
                download_workbook(formatted_wb, "Formatted_Distro_Grid.xlsx")

        except Exception as e:
            st.error(f"❌ Failed to format distribution grid: {e}")





def render_distro_grid_uploader_section():
    st.subheader("📤 Upload Distribution Grid to Snowflake")

    # 🔌 Snowflake connection
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Snowflake connection not found.")
        return

    # 🔍 Fetch chain names from CUSTOMERS table
    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"❌ Could not load chain names: {e}")
        return

    # 🧾 Upload and select chain
    selected_chain = st.selectbox("Select Chain Name", chain_options, key="distro_grid_chain_select")
    uploaded_file = st.file_uploader("Upload Formatted Distro Grid File", type=["xlsx"], key="distro_grid_final_upload")

    if uploaded_file and selected_chain:
        try:
            df = pd.read_excel(uploaded_file, engine="openpyxl")
            st.write("📋 Preview of formatted distro grid:")
            st.dataframe(df.head())

            if st.button("Upload Distribution Grid to Snowflake", key="upload_distro_grid_btn"):
                with st.spinner("📤 Uploading to Snowflake..."):
                    upload_distro_grid_to_snowflake(df, selected_chain, st.spinner)
        except Exception as e:
            st.error(f"❌ Failed to upload distro grid: {e}")



def update_spinner(message):
    st.text(f"{message} ...")
