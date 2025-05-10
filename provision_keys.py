# ------------------- key_uploader.py -------------------
import streamlit as st
import base64, binascii
from cryptography.fernet import Fernet
from sf_connector.service_connector import get_service_account_connection

st.title("🔐 Encrypt and Upload Private Key")

# Upload PEM key
uploaded_key = st.file_uploader("Upload RSA Private Key (.pem)", type=["pem"])

# Enter Tenant ID
tenant_id = st.text_input("Tenant ID")

# Submit button
if st.button("Encrypt and Upload") and uploaded_key and tenant_id:
    try:
        # Load PEM key bytes
        pem_data = uploaded_key.read()

        # Load encryption key from secrets
        fernet_key = st.secrets["encryption"]["fernet_key"]
        fernet = Fernet(fernet_key)

        # Encrypt the key
        encrypted_key = fernet.encrypt(pem_data)  # → bytes
        hex_encoded = binascii.hexlify(encrypted_key).decode()  # → hex str

        # Store in Snowflake
        conn = get_service_account_connection()
        cur = conn.cursor()

        cur.execute(f"""
            MERGE INTO TENANTUSERDB.CHAINLINK_SCH.SERVICE_KEYS target
            USING (SELECT '{tenant_id}' AS tenant_id) source
            ON target.TENANT_ID = source.tenant_id
            WHEN MATCHED THEN UPDATE SET PRIVATE_KEY_ENCRYPTED = %s
            WHEN NOT MATCHED THEN INSERT (TENANT_ID, PRIVATE_KEY_ENCRYPTED) VALUES (%s, %s)
        """, (hex_encoded, tenant_id, hex_encoded))

        conn.commit()
        cur.close()
        conn.close()

        st.success("✅ Private key encrypted and uploaded successfully.")
    except Exception as e:
        st.error("❌ Failed to encrypt or upload key.")
        st.exception(e)
