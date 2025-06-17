# utils/load_company_data_helpers.py

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl.styles import numbers
from io import BytesIO
from sf_connector.service_connector import connect_to_tenant_snowflake


# -----------------------------
# 🔧 Format Functions
# -----------------------------

def format_sales_report(workbook):
    """
    Cleans and formats the 'SALES REPORT' worksheet for Snowflake upload.
    - Keeps only the SALES REPORT sheet
    - Parses store name, cleans UPCs and strings
    - Handles 'Is Null' and numeric formats
    """
    try:
        for sheet_name in workbook.sheetnames:
            if sheet_name != 'SALES REPORT':
                workbook.remove(workbook[sheet_name])

        ws = workbook['SALES REPORT']
        ws.delete_rows(2)
        ws.delete_cols(8)

        for cell in ws['F']:
            if cell.value is not None:
                cell.value = str(cell.value).replace('-', '')

        ws.insert_cols(2)
        ws.cell(row=1, column=2, value='STORE NAME')

        for row in ws.iter_rows(min_row=2, min_col=3, max_col=3):
            for cell in row:
                store_name = str(cell.value).split('#')[0].replace("'", "") if '#' in str(cell.value) else str(cell.value).replace("'", "")
                ws.cell(row=cell.row, column=2).value = store_name

        ws.delete_cols(3)

        for col in ['B', 'E', 'C']:
            for cell in ws[col]:
                if cell.value and isinstance(cell.value, str):
                    cell.value = cell.value.replace(',', ' ').replace(" 's", "").replace("'", "")

        for cell in ws['F']:
            if cell.value is not None:
                cell.value = str(cell.value).replace('Is Null', '0')

        for cell in ws['G'][1:]:
            try:
                cell.value = float(cell.value.replace(",", "")) if isinstance(cell.value, str) else cell.value
                cell.number_format = numbers.FORMAT_NUMBER
            except:
                pass

        return workbook

    except Exception as e:
        st.error(f"Error formatting sales report: {str(e)}")
        return None


def format_customers_report(workbook):
    """
    Formats the 'Customers' worksheet for upload.
    - Removes filters and extra sheets
    - Renames headers and validates Store Number
    """
    try:
        for sheet_name in workbook.sheetnames:
            if sheet_name != 'Customers':
                workbook.remove(workbook[sheet_name])

        ws = workbook['Customers']
        ws.auto_filter.ref = None
        ws.insert_cols(3)

        for row in ws.iter_rows(min_row=2, min_col=4, max_col=4):
            for cell in row:
                if '#' in str(cell.value):
                    ws.cell(row=cell.row, column=4).value = str(cell.value).split('#')[0]

        for row in ws.iter_rows(min_row=2, min_col=5, max_col=5):
            ws.cell(row=row[0].row, column=3).value = row[0].value

        ws.delete_cols(5)

        headers = ['Customer_id', 'Chain_Name', 'Store_Number', 'Store_Name', 'Address', 'City', 'County', 'Salesperson', 'Account_Status']
        for col_idx, header in enumerate(headers, start=1):
            ws.cell(row=1, column=col_idx, value=header)

        for col in ['B', 'E']:
            for cell in ws[col]:
                if cell.value and isinstance(cell.value, str):
                    cell.value = cell.value.replace("'", "")

        invalid_rows = []
        for cell in ws['C'][1:]:
            if cell.value and not str(cell.value).isdigit():
                invalid_rows.append((cell.row, cell.value))

        if invalid_rows:
            st.error("Non-numeric Store Numbers found:")
            for row_num, val in invalid_rows:
                st.warning(f"Row {row_num}: '{val}'")
            st.stop()

        return workbook

    except Exception as e:
        st.error(f"Error formatting Customers sheet: {str(e)}")
        return None


def format_supplier_by_county(file_content) -> pd.DataFrame:
    """
    Formats the uploaded Supplier by County pivot table Excel file into a normalized DataFrame
    ready for upload to the Snowflake SUPPLIER_COUNTY table.

    - Expects a sheet named 'Report'
    - Drops 'TOTAL' column if present
    - Renames 'Supplier / County' to 'Supplier'
    - Unpivots county columns into a single 'County' column
    - Converts values (1 → 'Yes', NaN → 'No')

    Parameters:
        file_content (BytesIO or similar): The raw Excel file content uploaded via Streamlit

    Returns:
        pd.DataFrame: A normalized and cleaned DataFrame with columns: ['Supplier', 'County', 'Status']
    """
    try:
        xls = pd.ExcelFile(file_content)

        if "Report" not in xls.sheet_names:
            st.error("❌ Sheet named 'Report' not found in the Excel file.")
            st.info("Please rename the sheet you want formatted to 'Report' and try again.")
            return None

        df = xls.parse("Report")

        if "Supplier / County" not in df.columns:
            st.error("❌ Column 'Supplier / County' not found in 'Report' sheet.")
            return None

        if "TOTAL" in df.columns:
            df = df.drop(columns=["TOTAL"])

        df.rename(columns={"Supplier / County": "Supplier"}, inplace=True)

        df_melted = pd.melt(df, id_vars=["Supplier"], var_name="County", value_name="Status")

        df_melted["Status"] = df_melted["Status"].apply(
            lambda x: "Yes" if x == 1 else "No" if pd.isna(x) else str(x)
        )

        st.success("✅ Supplier by County formatting complete.")
        return df_melted

    except Exception as e:
        st.error(f"❌ Failed to format Supplier by County report: {str(e)}")
        return None



def format_product_workbook(workbook):
    """
    Cleans and normalizes the 'Products' worksheet for upload.
    - Moves, cleans, and renames columns
    - Removes commas, apostrophes, hyphens
    """
    try:
        ws = workbook['Products']

        col_g_data = [cell.value for cell in ws['G']]
        ws.insert_cols(2)
        for cell, value in zip(ws['B'], col_g_data):
            cell.value = value
        for cell in ws['G']:
            cell.value = None

        col_e_data = [cell.value for cell in ws['E']]
        for cell, value in zip(ws['D'], col_e_data):
            cell.value = value
        ws.delete_cols(5)

        for cell in ws['E']:
            if isinstance(cell.value, str):
                cell.value = cell.value.replace('-', '')

        for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
            for cell in row:
                if isinstance(cell.value, str):
                    cell.value = cell.value.replace(',', '').replace("'", '')

        ws['F1'].value = 'PRODUCT_MANAGER'
        for cell in ws['F'][1:]:
            cell.value = None

        if ws.max_column >= 7:
            ws.delete_cols(7)

        for cell in ws['E'][1:]:
            if cell.value is None:
                cell.value = 999999999999

        return workbook

    except Exception as e:
        st.error(f"❌ Error formatting product data: {str(e)}")
        return None


# -----------------------------
# 📤 Upload Functions
# -----------------------------

def download_workbook(workbook, filename):
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    st.download_button(
        label="Download formatted file",
        data=stream.read(),
        file_name=filename,
        mime='application/vnd.ms-excel'
    )



def _get_conn_and_cursor():
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")
    if not toml_info or not tenant_id:
        st.error("❌ Missing tenant configuration.")
        return None, None, None

    conn = connect_to_tenant_snowflake(toml_info)
    if not conn:
        st.error("❌ Failed to connect to Snowflake.")
        return None, None, None

    return conn, conn.cursor(), tenant_id

def _finalize_transaction(cursor, conn, success_msg):
    conn.commit()
    cursor.close()
    conn.close()
    st.success(success_msg)

def _rollback_transaction(conn, cursor):
    if conn: conn.rollback()
    if cursor: cursor.close()
    if conn: conn.close()

def _build_audit_fields():
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_date = datetime.today().strftime("%Y-%m-%d")
    return now_ts, today_date

def write_salesreport_to_snowflake(df: pd.DataFrame):
    """
    Uploads sales report to Snowflake.
    Uses TRUNCATE + INSERT with audit fields wrapped in a transaction.
    """
    df.fillna("NULL", inplace=True)
    conn, cursor, tenant_id = _get_conn_and_cursor()
    if not conn: return

    now_ts, today_date = _build_audit_fields()

    try:
        cursor.execute("BEGIN;")
        cursor.execute("TRUNCATE TABLE SALES_REPORT;")

        records = [
            (
                row.STORE_NUMBER, row.STORE_NAME, row.ADDRESS, row.SALESPERSON,
                row.PRODUCT_NAME, row.UPC, row.PURCHASED_YES_NO,
                tenant_id, now_ts, now_ts, today_date
            )
            for row in df.itertuples(index=False)
        ]

        insert_sql = """
            INSERT INTO SALES_REPORT (
                STORE_NUMBER, STORE_NAME, ADDRESS, SALESPERSON,
                PRODUCT_NAME, UPC, PURCHASED_YES_NO,
                TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, records)
        _finalize_transaction(cursor, conn, "✅ Sales report uploaded.")

    except Exception as e:
        _rollback_transaction(conn, cursor)
        st.error(f"❌ Sales report upload failed: {str(e)}")

def write_customers_to_snowflake(df: pd.DataFrame):
    """
    Uploads customer data to Snowflake with full audit fields.
    Wraps operation in transaction to prevent partial inserts.
    """
    df.fillna("NULL", inplace=True)
    df = df.applymap(lambda x: str(x).strip().upper() if pd.notna(x) else x)

    conn, cursor, tenant_id = _get_conn_and_cursor()
    if not conn: return

    now_ts, today_date = _build_audit_fields()

    try:
        cursor.execute("BEGIN;")
        cursor.execute("TRUNCATE TABLE CUSTOMERS;")

        records = [
            (
                row.CUSTOMER_ID, row.CHAIN_NAME, row.STORE_NUMBER, row.STORE_NAME,
                row.ADDRESS, row.CITY, row.COUNTY, row.SALESPERSON, row.ACCOUNT_STATUS,
                tenant_id, now_ts, now_ts, today_date
            )
            for row in df.itertuples(index=False)
        ]

        insert_sql = """
            INSERT INTO CUSTOMERS (
                CUSTOMER_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME,
                ADDRESS, CITY, COUNTY, SALESPERSON, ACCOUNT_STATUS,
                TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, records)
        _finalize_transaction(cursor, conn, "✅ Customers uploaded.")

    except Exception as e:
        _rollback_transaction(conn, cursor)
        st.error(f"❌ Customer upload failed: {str(e)}")

def write_products_to_snowflake(df: pd.DataFrame):
    """
    Uploads cleaned products data to Snowflake with proper type handling and auditing.
    """
    # Clean the DataFrame
    df.replace("NAN", np.nan, inplace=True)
    df.replace({np.nan: None}, inplace=True)  # ✅ Insert NULLs, not string 'NULL'
    df["CARRIER_UPC"] = df["CARRIER_UPC"].astype(str).str.strip()  # ✅ Preserve UPC formatting

    conn, cursor, tenant_id = _get_conn_and_cursor()
    if not conn: return

    now_ts, today_date = _build_audit_fields()

    try:
        cursor.execute("BEGIN;")
        cursor.execute("TRUNCATE TABLE PRODUCTS;")

        records = [
            (
                row.PRODUCT_ID, row.SUPPLIER, row.PRODUCT_NAME, row.PACKAGE,
                row.CARRIER_UPC, row.PRODUCT_MANAGER,
                tenant_id, now_ts, now_ts, today_date
            )
            for row in df.itertuples(index=False)
        ]

        insert_sql = """
            INSERT INTO PRODUCTS (
                PRODUCT_ID, SUPPLIER, PRODUCT_NAME, PACKAGE, CARRIER_UPC, PRODUCT_MANAGER,
                TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, records)
        _finalize_transaction(cursor, conn, "✅ Products uploaded.")

    except Exception as e:
        _rollback_transaction(conn, cursor)
        st.error(f"❌ Product upload failed: {str(e)}")











def write_supplier_by_county_to_snowflake(df: pd.DataFrame):
    """
    Uploads formatted Supplier by County DataFrame to the SUPPLIER_COUNTY table in Snowflake.

    - Ensures secure tenant-specific connection
    - Wraps TRUNCATE + INSERT in a transaction
    - Adds audit fields: TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
    - Prevents partial uploads on failure

    Parameters:
        df (pd.DataFrame): Must contain columns 'Supplier', 'County', 'Status'

    Returns:
        None. Displays Streamlit messages for success or error.
    """
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")

    if not toml_info or not tenant_id:
        st.error("❌ Tenant configuration is missing.")
        return

    try:
        conn = connect_to_tenant_snowflake(toml_info)
        if not conn:
            st.error("❌ Failed to connect to Snowflake.")
            return

        cursor = conn.cursor()
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today_date = datetime.today().strftime("%Y-%m-%d")

        # Start transaction
        cursor.execute("BEGIN;")

        # Truncate table
        cursor.execute("TRUNCATE TABLE SUPPLIER_COUNTY;")

        # Prepare records
        records = []
        for row in df.itertuples(index=False):
            record = (
                row.Supplier,
                row.County,
                row.Status,
                tenant_id,
                now_ts,
                now_ts,
                today_date
            )
            records.append(record)

        insert_sql = """
            INSERT INTO SUPPLIER_COUNTY (
                SUPPLIER,
                COUNTY,
                STATUS,
                TENANT_ID,
                CREATED_AT,
                UPDATED_AT,
                LAST_LOAD_DATE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        cursor.executemany(insert_sql, records)
        conn.commit()
        st.success("✅ Supplier by County data uploaded successfully to Snowflake.")

    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        st.error(f"❌ Supplier by County upload failed: {str(e)}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()







