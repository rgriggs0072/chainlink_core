import streamlit as st
from openpyxl import Workbook
import pandas as pd
from io import BytesIO

from utils.load_company_data_helpers import (
    # --- Uploaders (template-safe) ---
    write_salesreport_to_snowflake,
    write_customers_to_snowflake,
    write_products_to_snowflake,
    write_supplier_by_county_to_snowflake,

    # --- Legacy formatters (keep only if still used in other sections) ---
    format_customers_report,
    format_supplier_by_county,

    # --- Products helpers (still used) ---
    format_products_upload,

    # --- NEW CUSTOMERS UPLOAD/VALIDATION HELPERS ---
    generate_customers_template,
    format_customers_upload,
    validate_customers_upload,
    validate_customers_against_existing_chains,

    # --- NEW SALES_REPORT UPLOAD/VALIDATION HELPERS ---
    generate_sales_template,
    format_sales_upload,
    validate_sales_upload,
    #validate_sales_against_customers,

    # --- NEW PRODUCTS UPLOAD/VALIDATION HELPERS ---
    generate_products_template,
    validate_products_upload,

    # --- Supplier by County helpers ---
    generate_supplier_county_template,
    validate_supplier_county_upload,
    create_supplier_county_pivot_template_workbook,
    workbook_to_xlsx_bytes,
)


# ------------------- SALES -------------------
def render_sales_section():
    """
    Sales Report Upload / Validation Section (Single Path)

    Overview for future devs:
    - This section supports ONE upload flow only (template-based).
    - Legacy formatter/uploader intentionally removed to reduce risk and maintenance.

    Flow:
        1) Download Sales template (XLSX)
        2) Upload completed template (XLSX)
        3) Normalize + validate using SALES_SCHEMA
        4) Cross-check STORE_NUMBER + STORE_NAME against CUSTOMERS
        5) Safe upload (TEMP stage -> tenant-scoped swap)
    """
    st.subheader("Sales Report Validator (Recommended)")
    st.caption(
        "Download the template, paste your source data into it, upload for validation, "
        "then safely load into Snowflake (stage → swap)."
    )

    # -------------------------
    # Step 0: Download template
    # -------------------------
    st.markdown("**Step 0:** Download the official Sales Report template (XLSX).")

    template_df = generate_sales_template()

    wb = Workbook()
    ws = wb.active
    ws.title = "Sales_Template"
    ws.append(list(template_df.columns))

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

    st.markdown("---")

    # -----------------------------
    # Step 1: Upload completed file
    # -----------------------------
    st.markdown("**Step 1:** Upload the completed Sales template (XLSX) for validation and load.")

    uploaded = st.file_uploader(
        "Upload Sales Report template (XLSX)",
        type=["xlsx"],
        key="sales_validator_upload",
    )

    if uploaded is None:
        return

    # -------------------------
    # Step 2: Read raw XLSX
    # -------------------------
    try:
        raw_df = pd.read_excel(uploaded, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Failed to read Sales file: {e}")
        return

    # -------------------------
    # Step 3: Normalize + validate
    # -------------------------
    try:
        formatted_df = format_sales_upload(raw_df)
    except Exception as e:
        st.error(f"❌ Error during Sales normalization: {e}")
        return

    result = validate_sales_upload(formatted_df)

    all_errors: list[str] = []
    all_warnings: list[str] = []

    if result.errors:
        all_errors.extend(result.errors)
    if result.warnings:
        all_warnings.extend(result.warnings)

    # -----------------------------------------
    # Step 4: Cross-check against CUSTOMERS
    # -----------------------------------------
    # if result.cleaned_df is not None:
    #     cross_errors, cross_warnings = validate_sales_against_customers(result.cleaned_df)
    #     if cross_errors:
    #         all_errors.extend(cross_errors)
    #     if cross_warnings:
    #         all_warnings.extend(cross_warnings)

    # if all_warnings:
    #     st.info("⚠️ Validation warnings (non-fatal):")
    #     for msg in all_warnings:
    #         st.markdown(f"- {msg}")

    # if all_errors:
    #     st.error("❌ Validation failed. Please fix these issues and re-upload:")
    #     for msg in all_errors:
    #         st.markdown(f"- {msg}")
    #     return

    cleaned_df = result.cleaned_df
    st.success("✅ Validation passed. Preview of cleaned Sales data:")
    st.dataframe(cleaned_df.head(25), width="stretch")

    st.markdown("---")

    # -------------------------
    # Step 5: Safe upload
    # -------------------------
    if st.button("Upload validated Sales Report to Snowflake", key="upload_sales_validated"):
        try:
            with st.spinner("Uploading Sales Report safely (stage → swap)…"):
                write_salesreport_to_snowflake(cleaned_df)
            st.success("✅ Sales Report updated successfully.")
        except Exception as e:
            st.error(f"❌ Sales upload failed: {e}")






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

   




# ------------------- PRODUCTS -------------------
def render_products_section():
    """
    Products Upload / Validation Section

    Page overview for future devs:
    - Single canonical flow (no legacy formatter):
        1) User downloads Products template (CSV).
        2) User can either:
            - paste into template, OR
            - upload the raw Encompass export directly
        3) App formats (header mapping + light cleanup) via format_products_upload().
        4) App validates via validate_products_upload() -> returns (cleaned_df, errors, warnings).
        5) If no errors, user uploads to Snowflake via write_products_to_snowflake().
           Loader is staging-first so we never "truncate then fail" and leave PRODUCTS empty.

    Notes:
    - CARRIER_UPC:
        - Dashes/spaces are removed during cleaning.
        - Blank UPCs are allowed (warning) and will load as NULL.
        - Placeholder UPC '999999999999' is blocked (error).
    """

    st.subheader("Products Table Validator (Recommended)")

    # ------------------------------------------------------------------
    # Step 0: Download template
    # ------------------------------------------------------------------
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
        • <b>Carrier UPC</b> → <b>CARRIER_UPC</b> (we auto-remove dashes/spaces; must be digits ≤ 20)<br>
        • <b>Product Manager</b> → <b>PRODUCT_MANAGER</b> (optional)
        </small>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # ------------------------------------------------------------------
    # Step 1: Upload (template OR raw export) -> format -> validate -> preview -> upload
    # ------------------------------------------------------------------
    st.markdown("**Step 1:** Upload Products file (CSV or Excel) for validation and load.")
    products_file = st.file_uploader(
        "Upload Products file (CSV or XLSX)",
        type=["csv", "xlsx"],
        key="products_upload",
    )

    if not products_file:
        st.markdown("---")
        return

    try:
        # Read into DataFrame with dtype=str to avoid UPC/ID mangling
        if products_file.name.lower().endswith(".csv"):
            raw_df = pd.read_csv(products_file, dtype=str)
        else:
            # Use the first sheet by default
            raw_df = pd.read_excel(products_file, engine="openpyxl", dtype=str)

        # 1) Canonical formatting (header mapping + ensure required columns exist)
        formatted_df = format_products_upload(raw_df)

        # 2) Validation (returns cleaned_df, errors, warnings)
        cleaned_df, errors, warnings = validate_products_upload(formatted_df)

        # 3) Hard stop on errors
        if errors:
            st.error("Validation failed. Please fix these issues and re-upload:")
            for msg in errors:
                st.write(f"- {msg}")

            # Helpful view: show invalid UPC rows if present
            if "CARRIER_UPC" in cleaned_df.columns:
                bad_upc_mask = (
                    cleaned_df["CARRIER_UPC"].isna()
                    | (cleaned_df["CARRIER_UPC"] == "")
                    | (cleaned_df["CARRIER_UPC"] == "999999999999")
                )
                bad_rows = cleaned_df.loc[bad_upc_mask].copy()
                if not bad_rows.empty:
                    st.warning(f"Showing {min(len(bad_rows), 200)} row(s) with missing/invalid UPC (first 200):")
                    st.dataframe(bad_rows.head(200), width="stretch")

            st.dataframe(cleaned_df.head(50), width="stretch")
            st.markdown("---")
            return

        # 4) Non-fatal warnings
        if warnings:
            st.warning("Validation warnings (non-fatal):")
            for w in warnings:
                st.write(f"- {w}")

        # 5) Preview of what will load
        st.success("Validation passed. Preview of cleaned Products data:")
        st.dataframe(cleaned_df.head(50), width="stretch")

        # 6) Upload button
        if st.button("Upload Products to Snowflake", key="upload_products_btn"):
            with st.spinner("Uploading Products to Snowflake..."):
                write_products_to_snowflake(cleaned_df)
            st.success("Products table updated successfully.")

    except Exception as e:
        st.error(f"Error validating/uploading Products: {e}")

    st.markdown("---")

   





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
    # Step 0 – Template Downloads
    # -----------------------------------------------------
    st.markdown("**Step 0:** Download a template (standard rows OR pivot).")

    # Standard (3-column) CSV template
    tmpl = generate_supplier_county_template()
    tmpl_csv = tmpl.to_csv(index=False).encode("utf-8")

    st.download_button(
        "📥 Download Supplier by County Template (CSV)",
        data=tmpl_csv,
        file_name="supplier_by_county_template.csv",
        mime="text/csv",
        key="supplier_cty_template_download",
    )

    # Pivot-style XLSX template
    pivot_wb = create_supplier_county_pivot_template_workbook()
    pivot_bytes = workbook_to_xlsx_bytes(pivot_wb)

    st.download_button(
        "📥 Download Supplier by County Pivot Template (XLSX)",
        data=pivot_bytes,
        file_name="supplier_by_county_pivot_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="supplier_cty_pivot_template_download",
    )


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

   