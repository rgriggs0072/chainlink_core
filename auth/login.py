import streamlit as st
from sf_connector.service_connector import get_service_account_connection

# auth/login.py
def fetch_user_credentials():
    """
    Build streamlit_authenticator credentials from USERDATA.
    Only include active + not-locked users.
    """
    creds = {"usernames": {}}
    with get_service_account_connection() as con, con.cursor() as cur:
        cur.execute("""
            SELECT LOWER(EMAIL) AS EMAIL,
                   COALESCE(HASHED_PASSWORD, '') AS HASHED_PASSWORD,
                   TENANT_ID,
                   COALESCE(FIRST_NAME,''), COALESCE(LAST_NAME,''),
                   COALESCE(ROLE,'USER')
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
              AND COALESCE(IS_LOCKED, FALSE) = FALSE
        """)
        for email, hashed, tenant_id, first, last, role in cur.fetchall():
            # streamlit_authenticator expects hashed passwords already bcrypt-hashed
            creds["usernames"][email] = {
                "email": email,
                "name": f"{first} {last}".strip() or email,
                "password": hashed,           # keep as hashed; do NOT plaintext here
                "tenant_id": str(tenant_id),
                "role": role.strip().upper(),
            }
    return creds


