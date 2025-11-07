# ----------- tenants/tenant_manager.py ------------------------
from __future__ import annotations

import binascii
import base64
import hashlib
from typing import Any, Dict, Optional

import streamlit as st
from cryptography.fernet import Fernet
from sf_connector.service_connector import get_service_account_connection


# ---------- small helpers ----------

def _is_hex(s: str) -> bool:
    """True if s looks like hex and has even length."""
    if not isinstance(s, str):
        return False
    s = s.strip()
    return len(s) % 2 == 0 and all(c in "0123456789abcdefABCDEF" for c in s)


def _sha8(b: bytes) -> str:
    """Non-sensitive 8-char sha256 fingerprint."""
    try:
        return hashlib.sha256(b).hexdigest()[:8]
    except Exception:
        return "????????"


def _require_fernet_key() -> str:
    """Fetch and validate Fernet key from secrets; raise with a clear message if invalid/missing."""
    fkey = st.secrets.get("encryption", {}).get("fernet_key", "")
    if not fkey:
        raise RuntimeError("Missing [encryption].fernet_key in secrets")
    # Validate: must decode to 32 bytes
    try:
        if len(base64.urlsafe_b64decode(fkey.encode("utf-8"))) != 32:
            raise ValueError("Fernet key does not decode to 32 bytes")
    except Exception as e:
        raise RuntimeError(f"Invalid Fernet key format: {e!r}")
    return fkey


def _decrypt_tenant_key_from_db(raw_value: Any) -> str:
    """
    Accepts PRIVATE_KEY_ENCRYPTED from DB:
      - hex-encoded Fernet token (PREFIX starts '674141...' == 'gAAAA...' in hex)
      - raw Fernet token text (starts with 'gAAAA...')
    Returns plaintext PEM string '-----BEGIN PRIVATE KEY----- ...'
    """
    # Normalize to str
    if isinstance(raw_value, (bytes, bytearray)):
        raw_str = raw_value.decode("utf-8", errors="ignore")
    else:
        raw_str = str(raw_value or "")

    raw_str = raw_str.strip()
    if not raw_str:
        raise RuntimeError("SERVICE_KEYS.PRIVATE_KEY_ENCRYPTED is empty")

    fkey = _require_fernet_key()
    fernet = Fernet(fkey.encode("utf-8"))

    # Convert to bytes for decrypt: hex → bytes; else encode
    cipher_bytes = binascii.unhexlify(raw_str) if _is_hex(raw_str) else raw_str.encode("utf-8")

    try:
        pem = fernet.decrypt(cipher_bytes).decode("utf-8")
    except Exception as e:
        # Provide safe, actionable context without leaking secrets
        ct_prefix = cipher_bytes[:8] if isinstance(cipher_bytes, (bytes, bytearray)) else b""
        raise RuntimeError(
            f"Failed to decrypt tenant private key "
            f"(fernet_sha8={_sha8(fkey.encode())}, "
            f"blob_prefix={ct_prefix!r}, is_hex={_is_hex(raw_str)})"
        ) from e

    if not pem.startswith("-----BEGIN "):
        raise RuntimeError("Decryption succeeded but result does not look like a PEM key")

    return pem


# ---------- main API ----------

def load_tenant_config(tenant_id: str) -> Optional[Dict[str, Any]]:
    """
    Load tenant TOML + decrypt tenant private key from SERVICE_KEYS.
    Uses the service-account connection configured in secrets to read metadata.
    Returns a dict:
      {
        tenant_id, snowflake_user, account, warehouse, database, schema,
        private_key (PEM), tenant_name, logo_path, role
      }
    """
    conn = None
    cur = None
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()

        # --- 1) Fetch tenant TOML metadata ---
        cur.execute(
            """
            SELECT snowflake_user, account, warehouse, database, schema, tenant_name, logo_path, role
            FROM TENANTUSERDB.CHAINLINK_SCH.TOML
            WHERE TENANT_ID = %s
            """,
            (tenant_id,),
        )
        row = cur.fetchone()
        if not row:
            st.error(f"No TOML config found for tenant_id {tenant_id}")
            return None

        # --- 2) Fetch encrypted tenant private key ---
        cur.execute(
            """
            SELECT PRIVATE_KEY_ENCRYPTED
            FROM TENANTUSERDB.CHAINLINK_SCH.SERVICE_KEYS
            WHERE TENANT_ID = %s
            """,
            (tenant_id,),
        )
        encrypted_row = cur.fetchone()
        if not encrypted_row:
            st.error(f"No private key found for tenant_id {tenant_id}")
            return None

        encrypted_key_raw = encrypted_row[0]

        # --- 3) Decrypt → PEM (robust to hex/raw token) ---
        try:
            decrypted_pem = _decrypt_tenant_key_from_db(encrypted_key_raw)
        except Exception as e:
            st.error("❌ Failed to decrypt private key:")
            st.exception(e)
            return None

        # --- 4) Build config dict ---
        tenant_config = {
            "tenant_id": tenant_id,
            "snowflake_user": row[0],
            "account": row[1],
            "warehouse": row[2],
            "database": row[3],
            "schema": row[4],
            "private_key": decrypted_pem,
            "tenant_name": row[5],
            "logo_path": row[6],
            "role": row[7],
        }
        return tenant_config

    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()
