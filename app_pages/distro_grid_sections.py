# ---------------- distro_grid_sections.py ----------------
"""
Distro Grid Streamlit sections

Overview for future devs:
- This module defines the two main UI sections for the Distro Grid workflow:

  1) render_distro_grid_formatter_section()
     - Lets the user:
       * Download standard / pivot templates (generated on the fly)
       * Pick chain + layout (standard / pivot)
       * Upload a raw grid
       * Download a cleaned / normalized Excel file (NO DB WRITES)

  2) render_distro_grid_uploader_section()
     - Lets the user:
       * Select a chain
       * Upload a previously formatted grid
       * Validates CHAIN_NAME vs selection
       * Calls upload_distro_grid_to_snowflake(), which:
           - Archives old records for that chain+season
           - Deletes old DISTRO_GRID rows for that chain
           - Inserts new rows and runs UPDATE_DISTRO_GRID()

Notes:
- Heavy lifting lives in:
    * utils.distro_grid.formatters
    * utils.distro_grid_helpers
- Forms are used to prevent full-page reruns on every widget interaction.
"""

from io import BytesIO

import pandas as pd
import streamlit as st

from utils.distro_grid.formatters import (
    format_uploaded_grid,
    build_standard_template_xlsx,
    build_pivot_template_xlsx,
)
from utils.distro_grid_helpers import (
    upload_distro_grid_to_snowflake,
    update_spinner,  # spinner callback for upload
)
from utils.snowflake_utils import fetch_distinct_values


# ---------------------------------------------------------------------
# Internal helper: CHAIN_NAME validation
# ---------------------------------------------------------------------


def _validate_chain_in_df(df: pd.DataFrame, selected_chain: str, context: str) -> bool:
    """
    Validate that CHAIN_NAME in the uploaded DataFrame matches the
    chain chosen in the UI.

    - If no CHAIN_NAME column exists, returns True (nothing to validate).
    - If mismatches exist, displays errors/warnings + sample rows and
      returns False.

    Args:
        df:             DataFrame loaded from Excel.
        selected_chain: Chain selected in the UI (case-insensitive).
        context:        Short label for where this validation runs
                        (e.g., 'formatter', 'uploader').

    Returns:
        bool: True if validation passes (or not applicable), False otherwise.
    """
    selected_chain_clean = selected_chain.strip().upper()

    # Find a column that normalizes to CHAIN_NAME
    chain_col = None
    for col in df.columns:
        normalized = str(col).strip().upper().replace(" ", "_")
        if normalized == "CHAIN_NAME":
            chain_col = col
            break

    if chain_col is None:
        # No CHAIN_NAME column present; nothing to validate.
        return True

    df_chain = df.copy()
    df_chain[chain_col] = (
        df_chain[chain_col]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    mismatched = df_chain[
        df_chain[chain_col].notna()
        & (df_chain[chain_col] != selected_chain_clean)
    ]

    if mismatched.empty:
        return True

    unique_chains = sorted(x for x in df_chain[chain_col].dropna().unique())

    st.error(
        "❌ CHAIN_NAME mismatch detected between the file and your selection.\n\n"
        f"- Selected chain: `{selected_chain_clean}`\n"
        f"- Chains found in file ({context}): `{', '.join(unique_chains)}`"
    )
    st.warning(
        "Rows below have CHAIN_NAME values that do not match "
        f"'{selected_chain_clean}'. Please fix the file and try again."
    )
    st.dataframe(mismatched.head(200))

    return False


# ---------------------------------------------------------------------
# Section 1: Formatter (no DB writes)
# ---------------------------------------------------------------------


def render_distro_grid_formatter_section():
    """
    Distro Grid - Step 1: Format spreadsheet (no DB writes)

    UX:
    - Template download buttons at the top of the section.
    - Form for:
        * Chain selection
        * Layout selection (standard / pivot)
        * File upload
        * "Format Now" submit
    - Only runs formatting logic when the form is submitted.
    """
    st.subheader("Format Distribution Grid Spreadsheet")

    # --- 1) Get Snowflake connection + chain list ---
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

    chain_options_with_placeholder = ["Select Chain"] + chain_options

    # --- 2) Template download buttons at the very top of the section ---
    st.markdown("#### Download Distro Grid Templates")
    tmpl_col1, tmpl_col2 = st.columns(2)

    with tmpl_col1:
        std_tmpl_buffer = build_standard_template_xlsx()
        st.download_button(
            label="Standard Distro Grid Template",
            data=std_tmpl_buffer,
            file_name="standard_distro_grid_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dg_standard_template_dl",
        )

    with tmpl_col2:
        pivot_tmpl_buffer = build_pivot_template_xlsx()
        st.download_button(
            label="Pivot Distro Grid Template",
            data=pivot_tmpl_buffer,
            file_name="pivot_distro_grid_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dg_pivot_template_dl",
        )

    st.markdown("---")

    # --- 3) Form: chain + layout + file upload + submit ---
    with st.form("distro_grid_formatter_form"):
        st.markdown("#### Step 1: Select Chain and Layout")

        selected_chain = st.selectbox(
            "Select Chain Name for This Format",
            chain_options_with_placeholder,
            key="distro_grid_chain_select",
        )

        selected_format = st.selectbox(
            "Select Spreadsheet Format",
            ["Standard Column Format", "Pivot Table Format"],
            key="distro_grid_format_select",
            help="Use 'Standard' for column layout. Pivot uses the new standardized pivot formatter.",
        )

        st.markdown("#### Step 2: Upload and Format")

        uploaded_file = st.file_uploader(
            "Upload Distribution Grid Excel File to Format",
            type=["xlsx"],
            key="distro_grid_upload",
        )

        submitted = st.form_submit_button("Format Now")

    # --- 4) Handle submit only when button pressed ---
    if not submitted:
        return

    # Basic validations
    if selected_chain == "Select Chain":
        st.warning("Please select a chain before formatting.")
        return

    if uploaded_file is None:
        st.warning("Please upload a distribution grid Excel file before formatting.")
        return

    try:
        with st.spinner("Formatting distribution grid..."):
            # Load the raw sheet as DataFrame
            raw_df = pd.read_excel(uploaded_file, engine="openpyxl")

            # 1) Validate CHAIN_NAME vs selected chain (if the column exists)
            if not _validate_chain_in_df(
                df=raw_df,
                selected_chain=selected_chain,
                context="formatter",
            ):
                return

            # 2) Run formatting via the standardized formatter
            layout = "standard" if selected_format == "Standard Column Format" else "pivot"

            formatted_df = format_uploaded_grid(
                df_raw=raw_df,
                layout=layout,
                chain_name=selected_chain,
            )

            # Build downloadable Excel file from formatted DataFrame
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                formatted_df.to_excel(writer, index=False)
            buffer.seek(0)

        # Download button for the formatted file
        st.download_button(
            label="Download Formatted Grid",
            data=buffer,
            file_name=f"{selected_chain}_Formatted_Distro_Grid.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="formatted_dg_download_btn",
        )

        st.success("Formatting complete. Download the cleaned file above.")

    except Exception as e:
        st.error(f"Failed to format distribution grid: {e}")


# ---------------------------------------------------------------------
# Section 2: Uploader (DB write)
# ---------------------------------------------------------------------


def render_distro_grid_uploader_section():
    """
    Distro Grid - Step 2: Upload formatted grid to Snowflake

    UX:
    - Uses a form so heavy work only runs on submit.
    - No season picker; season is inferred automatically in the backend
      (Spring/Fall <year> via infer_season_label()).
    - Validates CHAIN_NAME in the file matches the selected chain.
    - Shows a small preview before upload.
    """
    st.subheader("Upload Distribution Grid to Database")

    # Get Snowflake connection from session
    conn = st.session_state.get("conn")
    if not conn:
        st.error("Database connection not found.")
        return

    # Fetch distinct chains for selection
    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"Could not load chain names: {e}")
        return

    chain_options_with_placeholder = ["Select Chain"] + chain_options

    st.markdown(
        "_Note: Season for archiving (Spring/Fall <year>) is inferred "
        "automatically at upload time. No season selection needed._"
    )

    st.markdown("---")

    # --- Form for upload flow ---
    with st.form("distro_grid_uploader_form"):
        st.markdown("#### Step 3: Select Chain and Upload Formatted File")

        selected_chain = st.selectbox(
            "Select Chain Name for This Upload",
            chain_options_with_placeholder,
            key="distro_grid_chain_select_upload",
        )

        uploaded_file = st.file_uploader(
            "Upload Formatted Distro Grid Excel File",
            type=["xlsx"],
            key="distro_grid_final_upload",
        )

        submitted = st.form_submit_button("Upload Distribution Grid to Database")

    if not submitted:
        return

    # Basic validations
    if selected_chain == "Select Chain":
        st.error("Please select a valid chain before uploading.")
        return

    if not uploaded_file:
        st.error("Please upload a formatted distro grid file.")
        return

    try:
        # Load file for validation + preview
        df = pd.read_excel(uploaded_file, engine="openpyxl")

        st.markdown("##### Preview of Uploaded Data")
        st.dataframe(df.head())

        # --- Validate CHAIN_NAME in file vs selected_chain ---
        if not _validate_chain_in_df(
            df=df,
            selected_chain=selected_chain,
            context="uploader",
        ):
            return

        # --- Upload via helper (season inferred in backend) ---
        with st.spinner("Uploading distribution grid and updating Database..."):
            upload_distro_grid_to_snowflake(
                df=df,
                selected_chain=selected_chain,
                selected_season=None,         # season will be inferred in backend
                update_spinner_callback=update_spinner,
            )

        st.success(f"✅ Upload complete for chain '{selected_chain}'.")

    except Exception as e:
        st.error(f"Failed to upload distro grid: {e}")
