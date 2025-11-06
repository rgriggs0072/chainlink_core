import binascii
from cryptography.fernet import Fernet, InvalidToken

# --- EDIT THESE ---
OLD_FERNET = b"dtaOC04VA2ONtogsVxDq4DXam6r8dE4e-tVbK4insPw="   # the key used when you uploaded originally
NEW_FERNET = b"Le2iXDuyX2ZiXdoeESwspAgqIjQMSIupcKwRYvqODAE="   # the key your app uses now
CT_HEX = """PASTE_FULL_HEX_FROM_DB_HERE"""                    # the exact value from SERVICE_KEYS

def main():
    ct_hex = CT_HEX.strip()
    if len(ct_hex) % 2 != 0:
        raise SystemExit("Ciphertext hex length is odd — looks corrupted.")

    token = binascii.unhexlify(ct_hex)     # hex -> bytes ("gAAAAA..." token)
    try:
        pem = Fernet(OLD_FERNET).decrypt(token)  # decrypt with OLD
    except InvalidToken:
        raise SystemExit("InvalidToken with OLD key — row was not encrypted with that key.")
    print("✅ Decrypted OK with OLD key. PEM bytes:", len(pem))

    new_token = Fernet(NEW_FERNET).encrypt(pem)  # re-encrypt with NEW
    new_hex = binascii.hexlify(new_token).decode()
    print("\n--- NEW_HEX (copy this to UPDATE the DB) ---\n")
    print(new_hex)

if __name__ == "__main__":
    main()

