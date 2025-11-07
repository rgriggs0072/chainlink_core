import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
import streamlit as st
import snowflake.connector as snowflake_connector
from packaging import version  # ✅ to handle version parsing
from cryptography.hazmat.primitives import serialization

# ============================ Helper: Load PEM Key ============================

def load_private_key(pem_input):
    if isinstance(pem_input, str):
        pem_input = pem_input.encode()
    return serialization.load_pem_private_key(
        pem_input,
        password=None
    )

# ============================ Helper: Build connection args ============================

def build_connection_args(base_args: dict):
    connector_version = snowflake_connector.__version__

    if version.parse(connector_version) >= version.parse("3.14.0"):
        base_args["disable_ocsp_checks"] = True
    else:
        base_args["insecure_mode"] = True

    return base_args

# ============================ Service Account Connector ============================

def get_service_account_connection():
    try:
        secrets = st.secrets["snowflake_connect"]
        private_key_obj = load_private_key(secrets["sf_private_key"])

        private_key = private_key_obj.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        base_args = dict(
            user=secrets["sf_user"],
            account=secrets["sf_account"],
            private_key=private_key,
            warehouse=secrets["sf_warehouse"],
            database=secrets["sf_database"],
            schema=secrets["sf_schema"]
        )

        conn = snowflake_connector.connect(**build_connection_args(base_args))
        return conn

    except Exception as e:
        st.error(f"Snowflake connection failed: {str(e)}")
        return None

# ============================ Tenant Connector ============================

def connect_to_tenant_snowflake(tenant_config):
    private_key_obj = load_private_key(tenant_config["private_key"])

    private_key = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    base_args = dict(
        user=tenant_config["snowflake_user"],
        account=tenant_config["account"],
        private_key=private_key,
        warehouse=tenant_config["warehouse"],
        database=tenant_config["database"],
        schema=tenant_config["schema"],
        role=tenant_config["role"]
    )

    conn = snowflake_connector.connect(**build_connection_args(base_args))

    # Optional context verification
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    # st.write("🔍 Snowflake Context:", cursor.fetchone())
    cursor.close()

    return conn
