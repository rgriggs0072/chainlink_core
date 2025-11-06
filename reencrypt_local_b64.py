import base64, binascii
from cryptography.fernet import Fernet, InvalidToken

# --- EDIT THESE ---
OLD_FERNET = b"dtaOC04VA2ONtogsVxDq4DXam6r8dE4e-tVbK4insPw="   # the key used originally
NEW_FERNET = b"Le2iXDuyX2ZiXdoeESwspAgqIjQMSIupcKwRYvqODAE="   # your current app key
TOKEN_B64  = """PASTE_token_b64_FROM_SQL_HERE"""               # result of the query above

def main():
    # base64 -> raw token bytes ("gAAAAA..." as bytes)
    try:
        token = base64.b64decode(TOKEN_B64.strip())
    except binascii.Error as e:
        raise SystemExit(f"Base64 decode failed: {e}")

    # quick sanity: token should start with b"gAAAAA"
    if not token.startswith(b"gAAAAA"):
        print("⚠️  token doesn't start with gAAAAA; double-check copy.")
    try:
        pem = Fernet(OLD_FERNET).decrypt(token)
    except InvalidToken:
        raise SystemExit("InvalidToken with OLD key — row wasn't encrypted with that key.")

    print(f"✅ Decrypted OK with OLD key. PEM bytes: {len(pem)}")

    new_token = Fernet(NEW_FERNET).encrypt(pem)   # bytes (starts with b"gAAAAA")
    new_hex = binascii.hexlify(new_token).decode()
    print("\n--- NEW_HEX (paste this into your UPDATE) ---\n")
    print(new_hex)

if __name__ == "__main__":
    main()

