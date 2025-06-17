# ----------- tenants/tenant_management.py ------------------------

import streamlit as st
from cryptography.fernet import Fernet
from sf_connector.service_connector import get_service_account_connection

def load_tenant_config(tenant_id):
    conn = get_service_account_connection()
    cur = conn.cursor()

    # --- 1. Fetch tenant TOML metadata ---
    cur.execute("""
        SELECT snowflake_user, account, warehouse, database, schema, tenant_name, logo_path, role
        FROM TENANTUSERDB.CHAINLINK_SCH.TOML
        WHERE TENANT_ID = %s
    """, (tenant_id,))
    row = cur.fetchone()


    if not row:
        st.error(f"No TOML config found for tenant_id {tenant_id}")
        return None

    # --- 2. Fetch Fernet-encrypted private key (base64 string) ---
    cur.execute("""
        SELECT PRIVATE_KEY_ENCRYPTED
        FROM TENANTUSERDB.CHAINLINK_SCH.SERVICE_KEYS
        WHERE TENANT_ID = %s
    """, (tenant_id,))
    encrypted_row = cur.fetchone()

    if not encrypted_row:
        st.error(f"No private key found for tenant_id {tenant_id}")
        return None

    encrypted_key_raw = encrypted_row[0]

    try:
        # 🔐 Decrypt using Fernet key
        fernet_key = st.secrets["encryption"]["fernet_key"]
        fernet = Fernet(fernet_key)

        if isinstance(encrypted_key_raw, str):
            encrypted_key_bytes = encrypted_key_raw.encode()
        elif isinstance(encrypted_key_raw, bytearray):
            encrypted_key_bytes = bytes(encrypted_key_raw)
        else:
            encrypted_key_bytes = encrypted_key_raw  # Already bytes

        decrypted_pem = fernet.decrypt(encrypted_key_bytes).decode()
    except Exception as e:
        st.error("❌ Failed to decrypt private key:")
        st.exception(e)
        return None

   
    tenant_config = {
        "tenant_id": tenant_id,
        "snowflake_user": row[0],  # ✅ rename snowflake_user → user
        "account": row[1],
        "warehouse": row[2],
        "database": row[3],
        "schema": row[4],
        "private_key": decrypted_pem,
        "tenant_name": row[5],
        "logo_path": row[6],
        "role": row[7]  # ✅ now included
    }


    cur.close()
    conn.close()
    return tenant_config
