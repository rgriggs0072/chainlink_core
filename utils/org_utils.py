# utils/org_utils.py
"""
Org/Client helpers
- get_business_name(): resolve a tenant's display name from CLIENTS.
"""

import streamlit as st
from sf_connector.service_connector import get_service_account_connection

@st.cache_data(ttl=300, show_spinner=False)
def get_business_name(tenant_id: str | int) -> str | None:
    """
    Return BUSINESS_NAME for tenant, falling back to TENANT_NAME if BUSINESS_NAME is NULL/empty.
    Looks in TENANTUSERDB.CHAINLINK_SCH.CLIENTS (central registry).
    """
    if not tenant_id:
        return None
    try:
        with get_service_account_connection() as con, con.cursor() as cur:
            cur.execute("""
                SELECT
                    NULLIF(TRIM(COALESCE(BUSINESS_NAME, '')), '') AS BUSINESS_NAME,
                    NULLIF(TRIM(COALESCE(TENANT_NAME,   '')), '') AS TENANT_NAME
                FROM TENANTUSERDB.CHAINLINK_SCH.CLIENTS
                WHERE TENANT_ID = %s
                LIMIT 1
            """, (tenant_id,))
            row = cur.fetchone()
        if not row:
            return None
        business, tenant = row[0], row[1]
        return business or tenant
    except Exception as e:
        # Keep it quiet in UI; return None so callers can fallback gracefully
        return None

