# ---------------- distro_grid_sections.py ----------------

import streamlit as st
import pandas as pd
import openpyxl
from io import BytesIO

from utils.distro_grid_helpers import (
    format_non_pivot_table,
    format_pivot_table,
    upload_distro_grid_to_snowflake,
    enrich_with_customer_data,
    get_season_options
)
from utils.snowflake_utils import fetch_distinct_values
from utils.ui_helpers import download_workbook


def render_distro_grid_formatter_section():
    """
    Renders the UI for formatting a Distribution Grid spreadsheet before uploading.
    Includes chain selection, format type, upload field, and a button to initiate formatting.
    """
    st.subheader("📄 Format Distribution Grid Spreadsheet")

    # 🔌 Get Snowflake connection
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Snowflake connection not found.")
        return

    # 🔍 Fetch available chain names
    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"❌ Could not load chain names: {e}")
        return

    # 🔽 Chain selector with placeholder
    chain_options_with_placeholder = ["🔽 Select Chain"] + chain_options
    selected_chain = st.selectbox(
        "Select Chain Name for This Format",
        chain_options_with_placeholder,
        key="distro_grid_chain_select"
    )

    if selected_chain == "🔽 Select Chain":
        st.warning("⚠️ Please select a chain before formatting.")
        st.stop()

    # 📥 Template links
    st.write("📥 Download Template Files:")
    st.markdown("[📊 Pivot Table Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Pivot_Table_Distro_Grid_Template.xlsx)")
    st.markdown("[📋 Standard Distro Grid Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Distribution_Grid_Template.xlsx)")
    st.markdown("-----------------------------------------------------")

    # 📑 Format type selector
    selected_format = st.selectbox(
        "Select Spreadsheet Format",
        ["Standard Column Format", "Pivot Table Format"],
        key="distro_grid_format_select"
    )
    st.markdown("-----------------------------------------------------")

    # 📂 Upload file field
    uploaded_file = st.file_uploader(
        "Upload Distribution Grid Excel File to Format",
        type=["xlsx"],
        key="distro_grid_upload"
    )

    # 🟢 Format trigger button
    if uploaded_file and st.button("🛠️ Format Now", key="format_distro_grid_btn"):
        try:
            with st.spinner("🔄 Formatting distribution grid..."):
                workbook = openpyxl.load_workbook(uploaded_file)

                # Format according to selected type
                if selected_format == "Standard Column Format":
                    formatted_df = format_non_pivot_table(workbook, selected_option=selected_chain)

                    # Build downloadable Excel file
                    buffer = BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        formatted_df.to_excel(writer, index=False)
                    buffer.seek(0)

                else:
                    formatted_wb = format_pivot_table(workbook, selected_option=selected_chain)

                    # Save workbook to stream
                    buffer = BytesIO()
                    formatted_wb.save(buffer)
                    buffer.seek(0)

            # ✅ Download button
            st.download_button(
                label="📥 Download Formatted Grid",
                data=buffer,
                file_name=f"{selected_chain}_Formatted_Distro_Grid.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Failed to format distribution grid: {e}")





def render_distro_grid_uploader_section():
    """
    Renders the UI for uploading a formatted Distribution Grid to Snowflake.
    Validates selected chain and season, verifies file integrity, and enforces
    CHAIN_NAME match to prevent cross-chain data uploads.
    """
    st.subheader("📤 Upload Distribution Grid to Snowflake")

    # 🔌 Get Snowflake connection from session state
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

    # 🔽 Chain selector (same key as formatter for continuity)
    chain_options_with_placeholder = ["🔽 Select Chain"] + chain_options
    selected_chain = st.selectbox(
        "Select Chain Name",
        chain_options_with_placeholder,
        key="distro_grid_chain_select_upload"
    )

    # 📅 Season selector with placeholder
    season_options_with_placeholder = ["📅 Select Season"] + get_season_options()
    selected_season = st.selectbox(
        "📅 Select Season for this Upload",
        options=season_options_with_placeholder,
        key="distro_grid_season_select"
    )

    # 📂 Upload final formatted file
    uploaded_file = st.file_uploader(
        "Upload Formatted Distro Grid File",
        type=["xlsx"],
        key="distro_grid_final_upload"
    )

    # 🚀 Upload trigger
    if st.button("Upload Distribution Grid to Snowflake", key="upload_distro_grid_btn"):
        # ✅ Validate selections
        if selected_chain == "🔽 Select Chain":
            st.error("❌ Please select a valid chain.")
            return
        if selected_season == "📅 Select Season":
            st.error("❌ Please select a valid season.")
            return
        if not uploaded_file:
            st.error("❌ Please upload a formatted distro grid file.")
            return

        try:
            # 📖 Load file and preview first 5 rows
            df = pd.read_excel(uploaded_file, engine="openpyxl")
            st.info("📋 Preview of uploaded data:")
            st.dataframe(df.head())

            # 🔄 Normalize CHAIN_NAME values for comparison
            df["CHAIN_NAME"] = df["CHAIN_NAME"].astype(str).str.strip()
            selected_chain_clean = selected_chain.strip()

            # 🔍 Check for mismatches
            mismatched_rows = df[df["CHAIN_NAME"] != selected_chain_clean]
            unique_chains_in_file = df["CHAIN_NAME"].dropna().unique()

            if not mismatched_rows.empty:
                st.error(f"❌ Chain mismatch detected.\n\nSelected: `{selected_chain_clean}`\nFound in file: `{', '.join(unique_chains_in_file)}`")
                st.warning(f"Rows that don't match the selected chain '{selected_chain_clean}':")
                st.dataframe(mismatched_rows)
                return

            # 🧼 Normalize and enrich data
            enrich_with_customer_data(df, conn)

            # 📤 Upload to Snowflake
            with st.spinner("📤 Uploading to Snowflake..."):
                upload_distro_grid_to_snowflake(df, selected_chain_clean, selected_season, update_spinner)

        except Exception as e:
            st.error(f"❌ Failed to upload distro grid: {e}")



def update_spinner(message):
    """
    Legacy helper for showing a status message (not commonly used anymore).
    """
    st.text(f"{message} ...")
