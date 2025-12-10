# ---------------- utils/sales_contacts.py ----------------
"""
Sales Contacts helpers

Overview for future devs:
- Centralized helpers around the SALES_CONTACTS table.
- Used by:
    * Email Gap Report (to resolve salesperson + manager emails)
    * Sales Contacts Admin page (CRUD for contacts)

Assumed table structure (per-tenant DB):
    SALES_CONTACTS (
        TENANT_ID         NUMBER,
        SALESPERSON_ID    NUMBER,
        SALESPERSON_NAME  VARCHAR,
        SALESPERSON_EMAIL VARCHAR,
        MANAGER_ID        NUMBER,
        MANAGER_NAME      VARCHAR,
        MANAGER_EMAIL     VARCHAR,
        IS_ACTIVE         BOOLEAN,
        CREATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
        UPDATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
    )

Notes:
- All functions expect a tenant-level Snowflake connection (st.session_state["conn"]).
- TENANT_ID is taken from st.session_state["tenant_id"] when not passed explicitly.
"""

from __future__ import annotations

from typing import List, Dict, Optional

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------
# Core fetchers
# ---------------------------------------------------------------------

def get_tenant_id() -> int:
    """Return the current tenant_id from session_state or raise if missing."""
    tenant_id = st.session_state.get("tenant_id")
    if tenant_id is None:
        raise RuntimeError("tenant_id not found in session_state.")
    return tenant_id


def fetch_sales_contacts(conn, tenant_id: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch all SALES_CONTACTS rows for the current tenant.

    Args:
        conn:      Tenant Snowflake connection (st.session_state["conn"]).
        tenant_id: Optional explicit tenant_id; if None, pulled from session.

    Returns:
        DataFrame of contacts ordered by SALESPERSON_NAME.
    """
    if tenant_id is None:
        tenant_id = get_tenant_id()

    sql = """
        SELECT
            TENANT_ID,
            SALESPERSON_ID,
            SALESPERSON_NAME,
            SALESPERSON_EMAIL,
            MANAGER_ID,
            MANAGER_NAME,
            MANAGER_EMAIL,
            IS_ACTIVE,
            CREATED_AT,
            UPDATED_AT
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
        ORDER BY SALESPERSON_NAME;
    """
    df = pd.read_sql(sql, conn, params=[tenant_id])
    return df


def lookup_contact_by_salesperson_name(
    conn,
    salesperson_name: str,
    tenant_id: Optional[int] = None,
) -> Optional[Dict]:
    """
    Get a single SALES_CONTACTS row by SALESPERSON_NAME (case-insensitive).

    Returns:
        dict(row) or None if not found.
    """
    if tenant_id is None:
        tenant_id = get_tenant_id()

    sql = """
        SELECT
            TENANT_ID,
            SALESPERSON_ID,
            SALESPERSON_NAME,
            SALESPERSON_EMAIL,
            MANAGER_ID,
            MANAGER_NAME,
            MANAGER_EMAIL,
            IS_ACTIVE
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND UPPER(SALESPERSON_NAME) = UPPER(%s)
          AND IS_ACTIVE = TRUE
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, [tenant_id, salesperson_name])
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


# ---------------------------------------------------------------------
# Insert / Update / Deactivate
# ---------------------------------------------------------------------

def upsert_sales_contact(
    conn,
    tenant_id: int,
    salesperson_id: int,
    salesperson_name: str,
    salesperson_email: str,
    manager_id: int,
    manager_name: str,
    manager_email: str,
    is_active: bool = True,
):
    """
    Insert or update a SALES_CONTACTS row keyed by (TENANT_ID, SALESPERSON_ID).

    If a row already exists for that composite key, it will be updated; otherwise,
    a new row is inserted.
    """
    sql = """
        MERGE INTO SALES_CONTACTS tgt
        USING (
            SELECT
                %s AS TENANT_ID,
                %s AS SALESPERSON_ID
        ) src
        ON tgt.TENANT_ID = src.TENANT_ID
       AND tgt.SALESPERSON_ID = src.SALESPERSON_ID
        WHEN MATCHED THEN UPDATE SET
            SALESPERSON_NAME  = %s,
            SALESPERSON_EMAIL = %s,
            MANAGER_ID        = %s,
            MANAGER_NAME      = %s,
            MANAGER_EMAIL     = %s,
            IS_ACTIVE         = %s,
            UPDATED_AT        = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            TENANT_ID,
            SALESPERSON_ID,
            SALESPERSON_NAME,
            SALESPERSON_EMAIL,
            MANAGER_ID,
            MANAGER_NAME,
            MANAGER_EMAIL,
            IS_ACTIVE,
            CREATED_AT,
            UPDATED_AT
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        );
    """
    params = [
        tenant_id,
        salesperson_id,
        # UPDATE
        salesperson_name,
        salesperson_email,
        manager_id,
        manager_name,
        manager_email,
        is_active,
        # INSERT
        tenant_id,
        salesperson_id,
        salesperson_name,
        salesperson_email,
        manager_id,
        manager_name,
        manager_email,
        is_active,
    ]
    with conn.cursor() as cur:
        cur.execute(sql, params)


def deactivate_sales_contact(
    conn,
    tenant_id: int,
    salesperson_id: int,
):
    """
    Soft-deactivate a SALES_CONTACTS row by setting IS_ACTIVE = FALSE.
    """
    sql = """
        UPDATE SALES_CONTACTS
           SET IS_ACTIVE = FALSE,
               UPDATED_AT = CURRENT_TIMESTAMP()
         WHERE TENANT_ID = %s
           AND SALESPERSON_ID = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, [tenant_id, salesperson_id])

