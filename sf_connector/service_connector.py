#--------------- service_connector.py ---------------#

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")


import os
os.environ["SF_OCSP_CHECK"] = "false"
import streamlit as st
import snowflake.connector as snowflake_connector


from cryptography.hazmat.primitives import serialization

def load_private_key(pem_input):
    if isinstance(pem_input, str):
        pem_input = pem_input.encode()
    return serialization.load_pem_private_key(
        pem_input,
        password=None
    )

def get_service_account_connection():
    try:
        secrets = st.secrets["snowflake_connect"]
        private_key_obj = load_private_key(secrets["sf_private_key"])

        private_key = private_key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake_connector.connect(
            user=secrets["sf_user"],
            account=secrets["sf_account"],
            private_key=private_key,
            warehouse=secrets["sf_warehouse"],
            database=secrets["sf_database"],
            schema=secrets["sf_schema"]
            disable_ocsp_checks=True  # ✅ Add this!
        )

        return conn  # 👈 Single value, not a tuple!

    except Exception as e:
        st.error(f"Snowflake connection failed: {str(e)}")
        return None


def connect_to_tenant_snowflake(tenant_config):
    private_key_obj = load_private_key(tenant_config["private_key"])

    private_key = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    conn = snowflake_connector.connect(
        user=tenant_config["snowflake_user"],  # or "user" if renamed later
        account=tenant_config["account"],
        private_key=private_key,
        warehouse=tenant_config["warehouse"],
        database=tenant_config["database"],
        schema=tenant_config["schema"],
        role=tenant_config["role"]  # ✅ required to avoid PUBLIC default
        disable_ocsp_checks=True  # ✅ Add this!
    )

    # ✅ Optional debug check
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
   # st.write("🔍 Snowflake Context:", cursor.fetchone())
    cursor.close()

    return conn
