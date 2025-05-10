# ----------- reset_schedule_helpers.py -----------

import streamlit as st
import pandas as pd
import numpy as np
import openpyxl
import re
from datetime import datetime, date, time
from openpyxl.styles import NamedStyle
from sf_connector.service_connector import connect_to_tenant_snowflake

def format_reset_schedule(workbook):
    """
    Validates and formats a reset schedule workbook for upload.
    Ensures required fields are present, renames headers, and applies consistent formats.

    Returns:
        workbook (Workbook): Formatted openpyxl workbook object, or None if validation fails.
    """
    ws = workbook['RESET_SCHEDULE_TEMPLATE']

    # Validate and rename headers
    header_names = [
        'CHAIN_NAME', 'STORE_NUMBER', 'STORE_NAME', 'PHONE_NUMBER', 'CITY', 'ADDRESS',
        'STATE', 'COUNTY', 'TEAM_LEAD', 'RESET_DATE', 'RESET_TIME', 'STATUS', 'NOTES'
    ]
    for idx, header in enumerate(header_names, 1):
        ws.cell(row=1, column=idx, value=header)

    # Check for empty values in required columns
    required_columns = {
        'B': 'STORE_NUMBER',
        'C': 'STORE_NAME',
        'J': 'RESET_DATE',
        'K': 'RESET_TIME'
    }
    for col_letter, col_name in required_columns.items():
        for cell in ws[col_letter][1:]:
            if not cell.value:
                st.warning(f"Missing value in column {col_letter} ({col_name}). Please correct before upload.")
                return None

    # Validate RESET_DATE format
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    for cell in ws['J'][1:]:
        if isinstance(cell.value, str) and not re.match(date_pattern, cell.value):
            st.warning("Invalid date format in column J (RESET_DATE). Must be mm/dd/yyyy.")
            return None

    # Apply date formatting
    date_style = NamedStyle(name='date_format', number_format='mm/dd/yyyy')
    for cell in ws['J'][1:]:
        cell.style = date_style

    # Standardize store names
    for cell in ws['C'][1:]:
        if isinstance(cell.value, str):
            cell.value = cell.value.strip().upper()

    return workbook

def upload_reset_data(df: pd.DataFrame, selected_chain: str):
    """
    Uploads reset schedule data to Snowflake after deleting existing entries for the selected chain.

    Args:
        df (pd.DataFrame): Formatted reset schedule data.
        selected_chain (str): Chain name to target for deletion + upload.
    """
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")
    if not toml_info or not tenant_id:
        st.error("TOML or tenant ID missing from session state.")
        return

    conn = connect_to_tenant_snowflake(toml_info)
    selected_chain = selected_chain.upper()

    if df['CHAIN_NAME'].isnull().any() or df['STORE_NAME'].isnull().any():
        st.warning("CHAIN_NAME and STORE_NAME cannot be null. Please correct and try again.")
        return

    mismatches = df.loc[df['CHAIN_NAME'].str.upper() != selected_chain]
    if not mismatches.empty:
        st.warning(f"CHAIN_NAME mismatch: Found {len(mismatches)} rows not matching '{selected_chain}'.")
        return

    try:
        now = datetime.now()
        today = date.today()

        df['TENANT_ID'] = tenant_id
        df['CREATED_AT'] = now
        df['UPDATED_AT'] = now
        df['LAST_LOAD_DATE'] = today
        df['RESET_DATE'] = pd.to_datetime(df['RESET_DATE'], errors='coerce').dt.date
        df['RESET_TIME'] = pd.to_datetime(df['RESET_TIME'], errors='coerce').dt.time

        df.replace({np.nan: None, '': None}, inplace=True)

        expected_columns = [
            'CHAIN_NAME', 'STORE_NUMBER', 'STORE_NAME', 'PHONE_NUMBER', 'CITY', 'ADDRESS',
            'STATE', 'COUNTY', 'TEAM_LEAD', 'RESET_DATE', 'RESET_TIME', 'STATUS', 'NOTES',
            'TENANT_ID', 'CREATED_AT', 'UPDATED_AT', 'LAST_LOAD_DATE'
        ]

        df = df[expected_columns]

        placeholders = ', '.join(['%s'] * len(expected_columns))

        delete_query = f"DELETE FROM RESET_SCHEDULE WHERE CHAIN_NAME = '{selected_chain}'"
        insert_query = f"INSERT INTO RESET_SCHEDULE ({', '.join(expected_columns)}) VALUES ({placeholders})"

        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            st.info(f"Removing existing RESET_SCHEDULE records for: {selected_chain}")
            cur.execute(delete_query)

            st.info("Inserting new records into RESET_SCHEDULE...")
            cur.executemany(insert_query, df.values.tolist())
            conn.commit()

        st.success(f"✅ Reset schedule uploaded for chain: {selected_chain}")

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Upload failed: {e}")

    finally:
        conn.close()
