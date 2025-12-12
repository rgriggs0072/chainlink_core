# ------------- load_company_sections.py -------------

import streamlit as st
from openpyxl import Workbook
import openpyxl
import pandas as pd
from io import BytesIO
from utils.load_company_data_helpers import (
    format_sales_report, write_salesreport_to_snowflake,
    format_customers_report, write_customers_to_snowflake,
    format_product_workbook, write_products_to_snowflake,
    format_supplier_by_county, write_supplier_by_county_to_snowflake,
    download_workbook,

     # NEW added these imports 11/22/2025 ------
     # NEW CUSTOMERS UPLOAD/VALIDATION HELPERS
    generate_customers_template,
    format_customers_upload,
    validate_customers_upload,
    validate_customers_against_existing_chains,

    # NEW SALES_REPORT UPLOAD/VALIDATION HELPERS
    generate_sales_template,
    format_sales_upload,
    validate_sales_upload,
    validate_sales_against_customers,


    # NEW PRODUCTS UPLOAD/VALIDATION HELPERS  ⬅️ ADD THIS BLOCK
    generate_products_template,
    validate_products_upload,

      # ➕ NEW Supplier by County
    generate_supplier_county_template,
    validate_supplier_county_upload,
)

import inspect

# st.write(
#     "DEBUG PRODUCTS VALIDATOR LOCATION:",
#     inspect.getsourcefile(validate_products_upload)
# )





# ------------------- SALES -------------------
def render_sales_section():
    """
    Sales Report Upload Section

    New flow (recommended):
      1) Download Sales template (CSV)
      2) Paste / map Source data into template
      3) Upload for validation + preview
      4) On success, upload to Snowflake SALES_REPORT

    Legacy flow (temporary):
      - Old Excel formatter + upload path kept in an expander.
    """
    st.subheader("Sales Report Validator (Recommended)")
    st.caption(
        "Use the template-based flow below to ensure clean, repeatable uploads "
        "for the multi-tenant app."
    )



        # --- Template download ---
    template_df = generate_sales_template()

    # Build a real XLSX using openpyxl (no xlsxwriter dependency)
    wb = Workbook()
    ws = wb.active
    ws.title = "Sales_Template"

    # Write headers
    ws.append(list(template_df.columns))

    # Write rows
    for row in template_df.itertuples(index=False, name=None):
        ws.append(list(row))

    # Save workbook to memory
    xlsx_buffer = BytesIO()
    wb.save(xlsx_buffer)
    xlsx_buffer.seek(0)

    st.download_button(
        label="📥 Download Sales Report Template (xlsx)",
        data=xlsx_buffer.getvalue(),
        file_name="sales_report_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="sales_template_download",
    )


    st.markdown("### Upload Completed Sales Template")
    uploaded = st.file_uploader(
        "Upload Sales Report file based on the template",
        type=["xlsx"],  # keep XLSX-only for now
        key="sales_validator_upload",
    )

    cleaned_df = None
  

    if uploaded is not None:
        # --- Step 1: Load raw file ---
        try:
            # XLSX only (user opens XLSX template in Excel, saves as xlsx, uploads)
            raw_df = pd.read_excel(uploaded, engine="openpyxl")
        except Exception as e:
            st.error(f"❌ Failed to read Sales file: {e}")
            return

        # --- UPC normalization to avoid Excel scientific notation issues ---
        if "UPC" in raw_df.columns:
            def _normalize_excel_upc(val):
                """
                Normalize UPC values coming from Excel:
                - If numeric (float/int): drop decimals, keep full integer (no sci-notation)
                - If string: strip whitespace
                - If NaN: return None
                """
                if pd.isna(val):
                    return None
                if isinstance(val, (int, float)):
                    # Excel may give 8.50001E+11; int() keeps all digits
                    return f"{int(val):d}"
                return str(val).strip()

            raw_df["UPC"] = raw_df["UPC"].apply(_normalize_excel_upc)

        st.write("Raw uploaded data (first 10 rows):")
        # st.dataframe(raw_df.head(10), width='strtch')

        # --- Step 2: Light formatting / normalization ---
        try:
            formatted_df = format_sales_upload(raw_df)
        except Exception as e:
            st.error(f"❌ Error during Sales normalization: {e}")
            return

        # --- Step 3: Schema validation (types, required fields, etc.) ---
        result = validate_sales_upload(formatted_df)

        all_errors: list[str] = []
        all_warnings: list[str] = []

        if result.errors:
            all_errors.extend(result.errors)
        if result.warnings:
            all_warnings.extend(result.warnings)

        # --- Step 4: Cross-check against CUSTOMERS (store_number consistency) ---
        if result.cleaned_df is not None:
            cross_errors, cross_warnings = validate_sales_against_customers(
                result.cleaned_df
            )
            if cross_errors:
                all_errors.extend(cross_errors)
            if cross_warnings:
                all_warnings.extend(cross_warnings)

        # --- Show validation messages ---
        if all_warnings:
            st.info("⚠️ Validation warnings (non-fatal):")
            for msg in all_warnings:
                st.markdown(f"- {msg}")

        if all_errors:
            st.error("❌ Validation failed. Please fix these issues and re-upload:")
            for msg in all_errors:
                st.markdown(f"- {msg}")
            return

        # If we're here, validation passed
        cleaned_df = result.cleaned_df
        st.success("✅ Validation passed. Preview of cleaned Sales data:")
        st.dataframe(cleaned_df.head(20), width='stretch')

        # --- Final upload action ---
        if st.button(
            "Upload validated Sales Report to Snowflake",
            key="upload_sales_validated",
        ):
            try:
                write_salesreport_to_snowflake(cleaned_df)
            except Exception as e:
                st.error(f"❌ Sales upload failed during write step: {e}")

    # ------------------------------------------------------------------
    # Legacy path kept for rollback / comparison (Excel formatter)
    # ------------------------------------------------------------------
    st.markdown("---")
    with st.expander("Legacy: Excel Sales Formatter (Encompass)", expanded=False):
        st.subheader("Format Sales Report (Legacy)")

        legacy_file = st.file_uploader(
            "Upload legacy Sales Report Excel (Encompass export)",
            type=["xlsx"],
            key="sales_upload_legacy",
        )
    
        if legacy_file:
            try:
                wb = openpyxl.load_workbook(legacy_file)
                with st.spinner("Formatting Sales Report (legacy path)..."):
                    formatted = format_sales_report(wb)
                if formatted:
                    download_workbook(formatted, "Formatted_Sales_Report.xlsx")
            except Exception as e:
                st.error(f"Error formatting Sales Report (legacy): {e}")

        st.markdown("---")
        st.subheader("Upload Legacy Formatted Sales Report to Database")

        sales_final = st.file_uploader(
            "Upload legacy formatted Sales Report",
            type=["xlsx"],
            key="sales_final_upload_legacy",
        )

        if sales_final:
            try:
                df_legacy = pd.read_excel(
                    sales_final,
                    engine="openpyxl",
                    sheet_name="SALES REPORT",
                )
                st.dataframe(df_legacy.head(), width='stretch')
                if st.button(
                    "Upload legacy Sales to Database",
                    key="upload_sales_legacy_btn",
                ):
                    write_salesreport_to_snowflake(df_legacy)
            except Exception as e:
                st.error(f"Error uploading legacy Sales Report: {e}")




# ------------------- CUSTOMERS -------------------
def render_customers_section():
    """
    Customers Upload / Validation Section

    Overview for future devs:
    - Primary (recommended) flow:
        1) User downloads the official Customers template.
        2) User copies/pastes Source data into the template.
        3) App normalizes headers/strings and validates using CUSTOMERS_SCHEMA.
        4) App cross-checks CHAIN_NAME against existing CUSTOMERS for this tenant.
        5) On success, data is uploaded to Snowflake via write_customers_to_snowflake().

    - Legacy helper:
        - Optional legacy Excel formatter for raw source exports.
        - Produces a cleaned workbook that the user can still paste into the template.
        - Does NOT bypass the validator; final uploads must still go through the template flow.
    """
    # ------------------------------------------------------------------
    # Recommended: Customer Table Validator
    # ------------------------------------------------------------------
    st.subheader("Customer Table Validator (Recommended)")

    # Step 0: Download template
    st.markdown("**Step 0:** Download the official Customers template and copy your source data into it.")
    template_df = generate_customers_template()
    tmpl_csv = template_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download Customers Template (CSV)",
        data=tmpl_csv,
        file_name="customers_template.csv",
        mime="text/csv",
        key="customers_template_download",
    )

    st.markdown(
        """
        <small>
        Paste columns from Source into the template as follows:<br>
        • <b>Customer ID</b> → <b>CUSTOMER_ID</b><br>
        • <b>Chain</b> → <b>CHAIN_NAME</b><br>
        • <b>Customer Name</b> (no <code>#XXXX</code> suffix) → <b>STORE_NAME</b><br>
        • <b>Chain Store Number</b> → <b>STORE_NUMBER</b><br>
        • <b>Shipping Address</b> → <b>ADDRESS</b><br>
        • <b>City</b> → <b>CITY</b><br>
        • <b>County</b> → <b>COUNTY</b><br>
        • <b>Salesman</b> → <b>SALESPERSON</b><br>
        • <b>Account Status</b> → <b>ACCOUNT_STATUS</b>
        </small>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Step 1: Upload completed template file
    st.markdown("**Step 1:** Upload Customers template (CSV or Excel) for validation and load.")
    customers_final = st.file_uploader(
        "Upload Customers template (CSV or XLSX)",
        type=["csv", "xlsx"],
        key="customers_final_upload",
    )

    if customers_final:
        try:
            # Read into DataFrame
            if customers_final.name.lower().endswith(".csv"):
                raw_df = pd.read_csv(customers_final)
            else:
                raw_df = pd.read_excel(customers_final, engine="openpyxl")

            # Normalize headers and string fields (light touch – assumes template layout)
            formatted_df = format_customers_upload(raw_df)

            # Schema-level validation (required cols, dtypes, non-blanks)
            schema_result = validate_customers_upload(formatted_df)
            errors = list(schema_result.errors)
            warnings = list(schema_result.warnings)
            cleaned_df = schema_result.cleaned_df

            # Cross-check CHAIN_NAME against existing tenant data if we have a clean frame
            chain_errors: list[str] = []
            if cleaned_df is not None and "CHAIN_NAME" in cleaned_df.columns:
                db_errors, db_warnings = validate_customers_against_existing_chains(cleaned_df)
                # Separate "Chain 'X' does not match..." errors so we can allow override
                for msg in db_errors:
                    if msg.startswith("Chain '") and "does not match any existing CHAIN_NAME" in msg:
                        chain_errors.append(msg)
                    else:
                        errors.append(msg)
                warnings.extend(db_warnings)

            # If there are unknown-chain errors, offer an explicit override
            allow_new_chains = False
            if chain_errors:
                st.error("The following chain names do not exist in the current Customers table:")
                for msg in chain_errors:
                    st.write(f"- {msg}")
                allow_new_chains = st.checkbox(
                    "I confirm these new chain names are correct and want to allow them for this upload.",
                    key="allow_new_customer_chains",
                )

                if allow_new_chains:
                    # Downgrade chain errors to warnings if user explicitly confirms
                    warnings.extend(chain_errors)
                    chain_errors = []

            # Merge back any remaining chain_errors (if user did NOT allow them)
            if chain_errors:
                errors.extend(chain_errors)

            # Hard-stop errors
            if errors:
                st.error("Validation failed. Please fix these issues and re-upload:")
                for msg in errors:
                    st.write(f"- {msg}")
                st.dataframe(formatted_df.head(50), width='stretch')
            else:
                # Non-fatal warnings
                if warnings:
                    st.warning("Validation warnings (non-fatal):")
                    for msg in warnings:
                        st.write(f"- {msg}")

                # Show preview of what will be loaded
                st.success("Validation passed. Preview of cleaned Customers data:")
                st.dataframe(cleaned_df.head(50), width='stretch')

                # Final upload button
                if st.button("Upload Customers to Snowflake", key="upload_customers_btn"):
                    with st.spinner("Uploading Customers to Snowflake..."):
                        write_customers_to_snowflake(cleaned_df)
                    st.success("Customers table updated successfully.")

        except Exception as e:
            st.error(f"Error validating/uploading Customers: {e}")

    st.markdown("---")

    # ------------------------------------------------------------------
    # Legacy Formatter (Optional helper for raw Encompass exports)
    # ------------------------------------------------------------------
    st.subheader("Legacy Formatter (Optional)")
    with st.expander("Click to use the legacy Encompass Excel cleaner"):
        st.markdown(
            """
            <small>
            This legacy formatter cleans a raw Encompass Customers export and produces
            a workbook with standardized headers and store numbers.<br>
            You can download the formatted file and then copy/paste into the template
            above to go through the validator.
            </small>
            """,
            unsafe_allow_html=True,
        )

        customers_file = st.file_uploader(
            "Upload raw Customers Excel from Encompass (XLSX)",
            type=["xlsx"],
            key="customers_legacy_upload",
        )

        if customers_file:
            try:
                wb = openpyxl.load_workbook(customers_file)
                with st.spinner("Formatting Customers (legacy helper)..."):
                    formatted = format_customers_report(wb)
                if formatted:
                    download_workbook(formatted, "Formatted_Customers.xlsx")
                    st.success("Legacy Customers file formatted. Download and/or paste into the template above.")
            except Exception as e:
                st.error(f"Error formatting Customers (legacy): {e}")




# ------------------- PRODUCTS -------------------
def render_products_section():
    """
    Products Upload / Validation Section

    Overview:
    - Recommended flow (template-based):
        1) Download the Products template (CSV).
        2) Map/paste Source data into template.
        3) Upload for validation via validate_products_upload().
        4) If clean, upload to Snowflake with write_products_to_snowflake().

    - Legacy helper:
        - Optional Excel formatter for raw exports.
        - Produces a cleaned workbook that can be used as input
          into the template/validator flow.
    """

    # ------------------------------------------------------------------
    # Recommended: Products Validator (NEW flow)
    # ------------------------------------------------------------------
    st.subheader("Products Table Validator (Recommended)")

    # Step 0: Download template
    st.markdown("**Step 0:** Download the official Products template and copy your source data into it.")
    products_template_df = generate_products_template()
    tmpl_csv = products_template_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download Products Template (CSV)",
        data=tmpl_csv,
        file_name="products_template.csv",
        mime="text/csv",
        key="products_template_download",
    )

    st.markdown(
        """
        <small>
        Paste columns from Source into the template as follows:<br>
        • <b>Product ID</b> → <b>PRODUCT_ID</b> (required, numeric, unique)<br>
        • <b>Supplier</b> → <b>SUPPLIER</b><br>
        • <b>Product Name</b> → <b>PRODUCT_NAME</b><br>
        • <b>Package</b> → <b>PACKAGE</b><br>
        • <b>Carrier UPC</b> → <b>CARRIER_UPC</b> (digits only, no spaces or dashes)<br>
        • <b>Product Manager</b> → <b>PRODUCT_MANAGER</b> (optional)
        </small>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Step 1: Upload completed template file for validation
    st.markdown("**Step 1:** Upload Products template (CSV or Excel) for validation and load.")
    products_final = st.file_uploader(
        "Upload Products template (CSV or XLSX)",
        type=["csv", "xlsx"],
        key="products_final_upload_new",
    )

    if products_final:
        try:
            # Read into DataFrame with dtype=str to avoid UPC/ID mangling
            if products_final.name.lower().endswith(".csv"):
                raw_df = pd.read_csv(products_final, dtype=str)
            else:
                # NOTE: no sheet_name hardcoding – just take the first sheet
                raw_df = pd.read_excel(products_final, engine="openpyxl", dtype=str)

            # Debug hook if needed:
            # st.write("DEBUG PRODUCT_ID RAW:", raw_df.get("PRODUCT_ID", []))

            # Validate/clean: handles header normalization, PRODUCT_ID + UPC rules, etc.
            cleaned_df, errors = validate_products_upload(raw_df)

            if errors:
                st.error("Validation failed. Please fix these issues and re-upload:")
                for msg in errors:
                    st.write(f"- {msg}")
                st.dataframe(cleaned_df.head(50), width='stretch')
            else:
                st.success("Validation passed. Preview of cleaned Products data:")
                st.dataframe(cleaned_df.head(50),  width='stretch')

                if st.button("Upload Products to Snowflake", key="upload_products_btn_new"):
                    with st.spinner("Uploading Products to Snowflake..."):
                        write_products_to_snowflake(cleaned_df)
                    st.success("Products table updated successfully.")

        except Exception as e:
            st.error(f"Error validating/uploading Products: {e}")

    st.markdown("---")

    # ------------------------------------------------------------------
    # Legacy Formatter (Optional helper for raw Excel exports)
    # ------------------------------------------------------------------
    st.subheader("Legacy Products Formatter (Optional)")
    with st.expander("Click to use the legacy Products Excel cleaner"):
        st.markdown(
            """
            <small>
            This legacy formatter cleans a raw Products Excel export and produces
            a workbook with standardized columns.<br>
            You can download the formatted file and then paste its data into the template
            above to go through the validator.
            </small>
            """,
            unsafe_allow_html=True,
        )

        products_file = st.file_uploader(
            "Upload raw Products Excel (XLSX)",
            type=["xlsx"],
            key="products_upload_legacy",
        )

        if products_file:
            try:
                wb = openpyxl.load_workbook(products_file)
                with st.spinner("Formatting Products (legacy helper)..."):
                    formatted = format_product_workbook(wb)
                if formatted:
                    download_workbook(formatted, "Formatted_Products.xlsx")
                    st.success("Legacy Products file formatted. Download and/or paste into the template above.")
            except Exception as e:
                st.error(f"Error formatting Products (legacy): {e}")






# ---------------------------------------------------------------------------------------------------------------------------------------
# New Section Added 11/22/2024 for products Uploads
# Randy Griggs 11/23/2025
# ---------------------------------------------------------------------------------------------------------------------------------------







# ------------------- SUPPLIER BY COUNTY -------------------
def render_supplier_county_section():
    """
    Supplier by County Upload / Validation Section

    - Accepts EITHER:
        • Template-based file with columns: SUPPLIER, COUNTY, STATUS
        • Raw pivot export (sheet 'Report', 'Supplier / County' + county columns)
          which is auto-transformed via format_supplier_by_county().
    - Then validates and uploads to SUPPLIER_COUNTY.
    """

    st.subheader("Supplier by County Validator (Recommended)")

    # -----------------------------------------------------
    # Step 0 – Template Download
    # -----------------------------------------------------
    st.markdown("**Step 0:** Download Supplier by County template and paste your data into it.")

    tmpl = generate_supplier_county_template()
    tmpl_csv = tmpl.to_csv(index=False).encode("utf-8")

    st.download_button(
        "📥 Download Supplier by County Template (CSV)",
        data=tmpl_csv,
        file_name="supplier_by_county_template.csv",
        mime="text/csv",
        key="supplier_cty_template_download",
    )

    st.markdown(
        """
        <small>
        Required columns:<br>
        • <b>SUPPLIER</b> – must not be blank<br>
        • <b>COUNTY</b> – must not be blank<br>
        • <b>STATUS</b> – must be <b>Yes</b> or <b>No</b><br>
        <br>
        You can either:
        <br>• Use this template directly, or
        <br>• Upload the original pivot export (with 'Supplier / County' + county columns);
             the app will auto-transform it to the 3-column layout.
        </small>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # -----------------------------------------------------
    # Step 1 – Upload + Auto Validation
    # -----------------------------------------------------
    st.markdown("**Step 1:** Upload Supplier by County file for validation & load.")

    supplier_file = st.file_uploader(
        "Upload Supplier by County file (Template CSV/XLSX or Pivot XLSX)",
        type=["csv", "xlsx"],
        key="supplier_cty_upload",
    )

    cleaned_df = None

    if supplier_file:
        try:
            # First, try to read as a normal table (template case)
            if supplier_file.name.lower().endswith(".csv"):
                raw_df = pd.read_csv(supplier_file)
            else:
                raw_df = pd.read_excel(supplier_file, engine="openpyxl")

            # Detect template vs pivot based on columns
            norm_cols = [str(c).strip().upper() for c in raw_df.columns]
            col_set = set(norm_cols)

            required_template_cols = {"SUPPLIER", "COUNTY", "STATUS"}

            if required_template_cols.issubset(col_set):
                # Template format: validate directly
                df_for_validation = raw_df
            else:
                # Not obvious template: check for legacy pivot shape
                # (Supplier / County + county columns)
                # Use our existing formatter to melt it.
                formatted_df = format_supplier_by_county(supplier_file)
                if formatted_df is None:
                    # format_supplier_by_county already emitted errors
                    return
                df_for_validation = formatted_df

            # Run through validator (handles both cases)
            cleaned_df, errors = validate_supplier_county_upload(df_for_validation)

            if errors:
                st.error("❌ Validation failed:")
                for e in errors:
                    st.markdown(f"- {e}")
                st.dataframe(cleaned_df.head(50),  width='stretch')

            else:
                st.success("✅ Validation passed. Preview:")
                st.dataframe(cleaned_df.head(50),  width='stretch')

                if st.button("Upload Supplier by County to Snowflake",
                             key="supplier_cty_upload_btn"):
                    with st.spinner("Uploading Supplier by County..."):
                        write_supplier_by_county_to_snowflake(cleaned_df)

        except Exception as e:
            st.error(f"❌ Error validating Supplier by County: {e}")

    st.markdown("---")

    # -----------------------------------------------------
    # Legacy Pivot Formatter (Optional – manual helper)
    # -----------------------------------------------------
    st.subheader("Legacy Supplier by County Formatter (Optional)")
    with st.expander("Click to use legacy pivot → normalized formatter"):

        legacy_file = st.file_uploader(
            "Upload raw Supplier by County pivot Excel (XLSX)",
            type=["xlsx"],
            key="supplier_cty_legacy_upload",
        )

        if legacy_file:
            try:
                with st.spinner("Formatting legacy Supplier by County pivot..."):
                    legacy_df = format_supplier_by_county(legacy_file)

                if legacy_df is not None:
                    st.dataframe(legacy_df.head(50),  width='stretch')
                    st.success("Legacy file formatted. Paste results into the template above if needed.")
            except Exception as e:
                st.error(f"❌ Error formatting legacy file: {e}")
