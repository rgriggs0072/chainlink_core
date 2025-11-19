# --------------------------------------- Sales_ingest.py ----------------------------------------------------
# --------------------------------------- Sales Data Ingestion Utility ---------------------------------------


"""
Page overview:
- Ingest daily sales from CSV/XLSX via Streamlit upload (no Snowflake stage needed).
- Validate & normalize (e.g., UPC normalization), write to SALES_RAW_IMPORT.
- Aggregate to SALES_WEEKLY (2-year rolling) via MERGE.
- Log lineage in SALES_UPLOAD_LOGS.

Notes for devs:
- Tenant-aware: uses st.session_state['tenant_config'] for DB/SCHEMA/ROLE/WH context.
- Uses st.rerun() (not deprecated experimental call).
"""

import re
import uuid
import pandas as pd
import streamlit as st
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from sf_connector.service_connector import connect_to_tenant_snowflake

REQUIRED_COLUMNS = ["TX_DATE", "UPC", "PRODUCT_ID", "PRODUCT_NAME", "UNITS_SOLD", "REVENUE"]
OPTIONAL_COLUMNS = ["STORE_NUMBER", "CHAIN_NAME", "CATEGORY", "SEGMENT", "CURRENCY", "VENDOR_DOC_ID"]

def _normalize_upc(s: str) -> str:
    """Strip all non-digits so formats like '8-10273-03038-9' -> '810273030389'."""
    return re.sub(r"\D", "", str(s or ""))

def _coerce_and_validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce types, enforce minimal integrity.
    - Ensures required columns exist
    - TX_DATE -> datetime.date, UNITS_SOLD/REVENUE -> numeric >= 0
    - UPC normalized but also stored as UPC_RAW
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    df["TX_DATE"] = pd.to_datetime(df["TX_DATE"], errors="coerce").dt.date
    df["UNITS_SOLD"] = pd.to_numeric(df["UNITS_SOLD"], errors="coerce")
    df["REVENUE"] = pd.to_numeric(df["REVENUE"], errors="coerce")

    # Normalize text fields
    df["UPC_RAW"] = df["UPC"].astype(str)
    df["UPC"] = df["UPC_RAW"].map(_normalize_upc)

    for col in ["PRODUCT_ID", "PRODUCT_NAME", "STORE_NUMBER", "CHAIN_NAME", "CATEGORY", "SEGMENT", "CURRENCY", "VENDOR_DOC_ID"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Drop bad rows
    df = df.dropna(subset=["TX_DATE", "UPC", "PRODUCT_ID", "UNITS_SOLD"])
    df = df[df["UNITS_SOLD"] >= 0]
    df = df[df["REVENUE"].fillna(0) >= 0]

    return df

def load_sales_file(file, *, source: str = "CSV") -> str:
    """
    Load a CSV/XLSX into SALES_RAW_IMPORT and aggregate into SALES_WEEKLY.
    Returns the IMPORT_ID (UUID) used for lineage and aggregation.
    """
    tenant = st.session_state["tenant_config"]
    conn = connect_to_tenant_snowflake(tenant)

    # Ensure DB/SCHEMA context for safety (if your connector doesn't already set it)
    with conn.cursor() as cur:
        cur.execute(f"USE DATABASE {tenant['database']}")
        cur.execute(f"USE SCHEMA {tenant['schema']}")

    # Read file
    df = (pd.read_excel(file) if file.name.lower().endswith((".xlsx", ".xls"))
          else pd.read_csv(file))
    df = _coerce_and_validate(df)

    import_id = str(uuid.uuid4())
    tenant_id = tenant.get("tenant_id", "unknown")

    # Attach required lineage columns
    df["IMPORT_ID"]   = import_id
    df["IMPORTED_AT"] = datetime.utcnow()
    df["SOURCE"]      = source
    df["SOURCE_FILE"] = file.name
    df["TENANT_ID"]   = tenant_id
    df["RAW_JSON"]    = None  # reserved; can store original payload if needed

    # Write to RAW (no stage)
    write_pandas(conn, df, "SALES_RAW_IMPORT", auto_create_table=False)

    # Log upload
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO SALES_UPLOAD_LOGS (
              UPLOAD_ID, TENANT_ID, SOURCE, FILE_NAME, IMPORT_DATE, ROW_COUNT, STATUS, NOTES
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            import_id, tenant_id, source, file.name,
            datetime.utcnow(), int(len(df)), "LOADED", "OK"
        ))

        # Make import_id visible in-session for MERGE
        cur.execute(f"SET import_id = '{import_id}'")

        # Weekly aggregation (tenant-wide; preserves store columns if present)
        cur.execute("""
            MERGE INTO SALES_WEEKLY tgt
            USING (
              SELECT
                DATE_TRUNC('week', TX_DATE)::DATE AS WEEK_START_DATE,
                COALESCE(STORE_NUMBER, 'ALL')     AS STORE_NUMBER,
                UPC,
                ANY_VALUE(PRODUCT_ID)             AS PRODUCT_ID,
                ANY_VALUE(PRODUCT_NAME)           AS PRODUCT_NAME,
                SUM(UNITS_SOLD)                   AS TOTAL_UNITS,
                SUM(REVENUE)                      AS TOTAL_REVENUE,
                ANY_VALUE(CHAIN_NAME)             AS CHAIN_NAME,
                ANY_VALUE(CATEGORY)               AS CATEGORY,
                ANY_VALUE(SEGMENT)                AS SEGMENT
              FROM SALES_RAW_IMPORT
              WHERE IMPORT_ID = $import_id
              GROUP BY 1,2,3
            ) s
            ON  tgt.WEEK_START_DATE = s.WEEK_START_DATE
            AND tgt.STORE_NUMBER    = s.STORE_NUMBER
            AND tgt.UPC             = s.UPC
            WHEN MATCHED THEN UPDATE SET
              tgt.PRODUCT_ID    = s.PRODUCT_ID,
              tgt.PRODUCT_NAME  = s.PRODUCT_NAME,
              tgt.TOTAL_UNITS   = s.TOTAL_UNITS,
              tgt.TOTAL_REVENUE = s.TOTAL_REVENUE,
              tgt.CHAIN_NAME    = s.CHAIN_NAME,
              tgt.CATEGORY      = s.CATEGORY,
              tgt.SEGMENT       = s.SEGMENT,
              tgt.AGGREGATED_AT = CURRENT_TIMESTAMP,
              tgt.IMPORT_ID     = $import_id
            WHEN NOT MATCHED THEN INSERT (
              WEEK_START_DATE, STORE_NUMBER, UPC, PRODUCT_ID, PRODUCT_NAME,
              TOTAL_UNITS, TOTAL_REVENUE, CHAIN_NAME, CATEGORY, SEGMENT, IMPORT_ID
            ) VALUES (
              s.WEEK_START_DATE, s.STORE_NUMBER, s.UPC, s.PRODUCT_ID, s.PRODUCT_NAME,
              s.TOTAL_UNITS, s.TOTAL_REVENUE, s.CHAIN_NAME, s.CATEGORY, s.SEGMENT, $import_id
            );
        """)

        cur.execute("""
            UPDATE SALES_UPLOAD_LOGS
            SET STATUS='AGGREGATED', NOTES='Weekly merge complete'
            WHERE UPLOAD_ID=%s
        """, (import_id,))

    conn.close()
    return import_id

