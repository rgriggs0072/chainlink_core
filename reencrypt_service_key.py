import binascii, sys, os
from cryptography.fernet import Fernet
import snowflake.connector

# --- CONFIG ---
TENANT_ID = "9001"
OLD_FERNET = b"dtaOC04VA2ONtogsVxDq4DXam6r8dE4e-tVbK4insPw="   # old key
NEW_FERNET = b"Le2iXDuyX2ZiXdoeESwspAgqIjQMSIupcKwRYvqODAE="   # current key
DRY_RUN = True  # set False to perform UPDATE

# Snowflake connection (fill from your local secrets.toml values)
SF_USER = "CHAINLINK_APP_PROD_SVC"
SF_ACCOUNT = "OEZIERR-PROD"
SF_WAREHOUSE = "PROD_WH"
SF_DATABASE = "TENANTUSERDB"
SF_SCHEMA = "CHAINLINK_SCH"

# Use keypair auth (recommended)
SF_PRIVATE_KEY_PEM = r"""-----BEGIN PRIVATE KEY-----
<YOUR PEM HERE>
-----END PRIVATE KEY-----"""

def connect():
    import tempfile
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    key = serialization.load_pem_private_key(
        SF_PRIVATE_KEY_PEM.encode(), password=None, backend=default_backend()
    )
    pkb = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        user=SF_USER,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        private_key=pkb,
    )

def main():
    con = connect()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT PRIVATE_KEY_ENCRYPTED
            FROM SERVICE_KEYS
            WHERE TENANT_ID=%s
        """, (TENANT_ID,))
        row = cur.fetchone()
        if not row:
            print(f"❌ No SERVICE_KEYS row for TENANT_ID={TENANT_ID}")
            return

        ct_hex = (row[0] or "").strip()
        if not ct_hex:
            print("❌ Empty PRIVATE_KEY_ENCRYPTED")
            return
        if len(ct_hex) % 2 != 0:
            print("❌ Not even-length hex")
            return

        token = binascii.unhexlify(ct_hex)
        # Decrypt with OLD key
        pem = Fernet(OLD_FERNET).decrypt(token)
        print(f"✅ Decrypted with OLD key. PEM length: {len(pem)}")

        # Re-encrypt with NEW key
        new_token = Fernet(NEW_FERNET).encrypt(pem)  # bytes (gAAAAA...)
        new_hex = binascii.hexlify(new_token).decode()
        print(f"Prepared new hex len={len(new_hex)}")

        if DRY_RUN:
            print("DRY_RUN=True -> not updating DB. Toggle DRY_RUN=False to apply.")
            return

        # Update row
        cur.execute("""
            UPDATE SERVICE_KEYS
            SET PRIVATE_KEY_ENCRYPTED = %s
            WHERE TENANT_ID=%s
        """, (new_hex, TENANT_ID))
        con.commit()
        print("✅ Row updated with NEW key.")

    finally:
        cur.close()
        con.close()

if __name__ == "__main__":
    main()

