# ---------------- reset_schedule_sections.py ----------------

import streamlit as st
import pandas as pd
import openpyxl
from datetime import datetime
from io import BytesIO

from utils.reset_schedule_helpers import (
    format_reset_schedule,
    upload_reset_data,
    generate_reset_schedule_template,
)
from utils.ui_helpers import download_workbook
from utils.snowflake_utils import fetch_distinct_values


def render_reset_schedule_formatter_section():
    """
    Step 0 + Step 1:
    - Provide a Reset Schedule template for download.
    - Allow user to upload a completed template for validation/formatting.
    - On success, return a formatted workbook ready for upload step.

    Fix (2026-03-25): Wrapped file_uploader in st.form to prevent CP reruns
    from dropping the uploaded file before it can be read.
    """
    st.subheader("Reset Schedule Template & Formatter")

    # -------------------------------
    # Step 0: Download template (outside form — download buttons can't be inside forms)
    # -------------------------------
    st.markdown("**Step 0:** Download the Reset Schedule template and fill in your data.")

    tmpl_wb = generate_reset_schedule_template()
    tmpl_buffer = BytesIO()
    tmpl_wb.save(tmpl_buffer)
    tmpl_buffer.seek(0)

    st.download_button(
        label="📥 Download Reset Schedule Template (XLSX)",
        data=tmpl_buffer,
        file_name="Reset_Schedule_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="reset_schedule_template_download",
    )

    st.markdown(
        """
        <small>
        Required columns per row:<br>
        • <b>CHAIN_NAME</b><br>
        • <b>STORE_NUMBER</b><br>
        • <b>STORE_NAME</b><br>
        • <b>ADDRESS</b><br>
        • <b>CITY</b><br>
        • <b>RESET_DATE</b> (mm/dd/yyyy or Excel date)<br>
        • <b>RESET_TIME</b> (e.g. '8:00 AM' or '13:00')<br>
        </small>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # -------------------------------
    # Step 1: Upload for formatting/validation
    # Wrapped in st.form so the file is held in place across reruns (CP fix)
    # -------------------------------
    st.markdown("**Step 1:** Upload the completed Reset Schedule template for validation & formatting.")

    with st.form("reset_schedule_formatter_form"):
        uploaded_file = st.file_uploader(
            "Upload Reset Schedule Excel (based on the template)",
            type=["xlsx"],
            key="reset_schedule_upload",
        )
        submitted = st.form_submit_button("Validate & Format")

    if not submitted:
        return

    if uploaded_file is None:
        st.warning("Please upload a Reset Schedule Excel file before submitting.")
        return

    try:
        workbook = openpyxl.load_workbook(uploaded_file)
        with st.spinner("Validating and formatting reset schedule..."):
            formatted_wb = format_reset_schedule(workbook)

        if formatted_wb:
            st.success("✅ Formatting complete. Download to review before upload.")
            download_workbook(formatted_wb, "Formatted_Reset_Schedule.xlsx")
    except Exception as e:
        st.error(f"Failed to format reset schedule: {e}")


def render_reset_schedule_uploader_section():
    """
    Final upload step:
    - User uploads the formatted reset schedule file.
    - User selects a chain (from CUSTOMERS.CHAIN_NAME).
    - We delete existing records for that chain and insert the new ones.

    Fix (2026-03-25): Wrapped in st.form to prevent CP reruns from dropping
    the uploaded file or firing the upload on every widget interaction.
    """
    st.subheader("Upload Reset Schedule to Tables")

    conn = st.session_state.get("conn")
    if not conn:
        st.error("Database connection not found.")
        return

    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"Could not load chain names: {e}")
        return

    with st.form("reset_schedule_uploader_form"):
        selected_chain = st.selectbox(
            "Select Chain Name",
            chain_options,
            key="reset_schedule_chain_select",
        )

        uploaded_file = st.file_uploader(
            "Upload Formatted Reset Schedule File",
            type=["xlsx"],
            key="reset_schedule_final_upload",
        )

        submitted = st.form_submit_button("Upload Reset Schedule to Tables")

    if not submitted:
        return

    if uploaded_file is None:
        st.error("Please upload a formatted Reset Schedule file.")
        return

    if not selected_chain:
        st.error("Please select a chain.")
        return

    try:
        df = pd.read_excel(uploaded_file, engine="openpyxl")
        st.markdown("**Preview of uploaded data:**")
        st.dataframe(df.head(), use_container_width=True)

        with st.spinner("Uploading to Tables..."):
            upload_reset_data(df, selected_chain)

    except Exception as e:
        st.error(f"❌ Failed to upload reset schedule: {e}")
