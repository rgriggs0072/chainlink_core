# verify_and_connect_prod.py
# ---------------------------------------------------------------------
# PAGE OVERVIEW
# ---------------------------------------------------------------------
# This script validates the prod key path end-to-end:
# 1) Reads the HEX-encoded Fernet ciphertext you copied from SERVICE_KEYS.
# 2) Decrypts using the Fernet key from prod .streamlit/secrets.toml.
# 3) Asserts PEM header, prints the first/last line (no secrets dumped).
# 4) Computes SHA256 fingerprint of the public key (SPKI) so you can compare
#    with Snowflake's DESC USER -> RSA_PUBLIC_KEY_FP.
# 5) Converts PEM -> PKCS#8 DER and (optionally) connects to Snowflake using
#    key-pair auth (no password).
#
# If the fingerprint DOES NOT match DESC USER, stop: you’re decrypting the wrong
# blob or the Snowflake user has the wrong public key set.
#
# Note: st.experimental_rerun() is deprecated; use st.rerun() in Streamlit. Not relevant here.

import os
import base64
import binascii
import snowflake.connector
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend

# ------------------ FILL THESE ------------------
PRIVATE_KEY_ENCRYPTED_HEX = """674141414141426f3959334452564f5576346957376439724867684a75736a46654368786b61787567794230645168713159306d745970796b4a6d73693572487930765543566b35773441634e4b786b6b6c584f5a334247424e574a784e5a4b47695f7a30614568576d7a5972535f7a494b65794c73724a33384a74316c32382d634f4d6e48327933576e4970556d4a545f52784e57647166564c6e764f5141614446435351734e4b37334b6a6656333536796d594c5530715275342d31754b344857636f64656e544a5f6548423363706b763437344e44737670673337495f3661574b74503575776e7836395a54532d377562573973544b6d377743515a646143305f475263596a7131575757466357336c69633558427a2d534d43557457527942494851457456394d5a4742456147347554636d7075467a4761415a756e37697671506e476468734c305a2d6962764871524e6e735745463447556867476f5f6e774c5f38374b497552436e31546f505a32364b464543517441374470434271543164506f696f62736e644c424f6f2d3238497979726a7a5636734f36346835444a71654b496c4b3338336c766d59596e696a77624476713675546c544a314e7435416f45395258685a4c394453685f454c7a4549374a5265314f784d525948476a45797355784d4b4166776d525a6830366b795f7131742d62384c7a6363686f4464456d674a70744255423265627652655558614c30454d52614537327946386a4761696f44795f51575245524c7676306961696b756957756f47585867546f4c4f4234586a45634a79354857586957394949693047744c4b5a415f4c5a4f4e752d4c4b3235625846575f645064576255685765574d6661776349534b6f514c4c62686572396b34444a46747462576e454e6d4d30324b78786b67627345683635654b744e4d475a706b612d4d72776d493669453455786d534b583162795559496e39585452786947554e53524c534b62486b5136686d7939474c585a336955476470547658746d4e354e6e4d33325434494e68344567674b534448335066556f39636241726c3653776634444134634b6a5132694568416d7634632d466130474e514f42796e686a47437978696864394a492d6756333352326f43524e306a4e4673595579735a6341764835645539324a527170584a664d4267764247386a346e636c432d793730475a5a434b344b4863576b4f56707448525a51476148556b6f3956364c384a44323072525464427667646f72375943667255615a434a622d3441474c79647055366c4e56563659763333556d306f4b734236747772327438785563423076543835496b45542d56784133663536486a5f434e6b4b3731533632742d30564c443634664b6b787a6d6452506f56566a6c6e58553854577643775645766e6f5f4851704a2d6d317052534370417049545858364853646a6a4d4c346679517933643056436c546c6c745044314f4d4a546a7a5f7a6c33753462375f473055306a6b53776561382d70613968686d53666c4c746652354f63416355425039646a587a4d4d3847624f59375575366568686a306f547033536b537978347267746448616141597374526c534d71717735475152506b7a3070764a6e65784a73727239763053685f36346747645637706c4e724a574e6a7665415a55664654554d5554685273644a546b6c7636594f465273485a6e5830664b4c384637545f7835354a42625f64395372524e686e5638435f4c4459486e767754303358787a7045337937396f34433034676e7766327576644736394f6e5a545164666541584a4a794462706153454833704f66676e38324f5465367938554c5268306a566d4d494c4d75552d35786c4c596b675275442d7a43666f5f6d45676147617978787858795f5345786b4d71634e58715a5f335859693643593730575f4d655546794f6e585f71354e3061363074753372795f334330646d6a6870735a68737a7867446476324b496d6c36436d575a6b796575694c45414b74786158312d6d726d5568316233454b6b5163436a316c4d714d5f45434d4d6a6644545436486b755177723258786b55525337445679686b63494b312d5a6e5a377a7969615447685f3936587347486c626a476c58454f62664d646766686d416d6a326c58324971366c6865505f4f55435a365f5055796f736d5177684975576733437335454f5a4d4167466b6c724f6c71753658547571367536345a476578484e6d585f65756e794b573647444c4d6a414f6c43435930695a33573370794b4f4449564836757278553155616666654d5a3375674c51627747644141685970364c5f364e54412d495772613142446a4e5839427a4a6e356d7534387a4d764e386952584c5f754e727952744e6864783747584e6736693372734b697169564a4139306d6479304a423230394c5f57515253326966556a705147334546484f39625a7a53514f695f6a6c7130715954724b5049723139503764627a4b704d537a6a5a4a496c5439492d35555247706e336f593549696f6178662d703137574a396d38526836574f3248637648794a593234567642505f4d6675417364396b5148703556644d326b554c35346843786d61444854686643754a4237774e567666614a48725f6e5564683577636a52456c586d56534d48513467383253314a374a6e674b6850557552784850514b6f7055715f42475f70434737733743506d6468614e666d5851787a4762333067666c397854674345345a59512d776c41515f4a6361327531446d6c314f6243364768386741562d4e6d77725459314775715a34557551722d4b624d7357714f5f32723762486d42473648354d6578647467456d77307874517253303872536136587333616b3933366a3549576f3334737279426c7a6c496f71335f77576f30784755537363515748373144773847347770456c414d4e7573584f5057323879366f3758595032523031454e724150495766757a535850784b6d586c2d4f45556762645138674f3147344356616143557a58304159744e5a42714f7a486245504d353438634961644e435f5951583069752d3775355837524e45726c62496e2d2d7a3272726f375234632d6448524e734e4a5544677732734538624c745045572d534e653034496545513250636463705176734c3449762d704376634a39504348435455536f365231447663644d5256416e613536584a7849706d425a67387450484573636950772d6c773958416b383032794d534c446944506478594f68334d7047516379706d6b5f61314a7a65366d6741355152376e734d413d3d"""
FERNET_KEY = "Le2iXDuyX2ZiXdoeESwspAgqIjQMSIupcKwRYvqODAE="
# If your PEM is passphrase-protected, set it; otherwise leave as None
PEM_PASSPHRASE = None

# Optional: only fill if you want to run the connect smoke test.
ACCOUNT   = "OEZIERR-PROD"       # e.g. xy12345.us-west-2
USER      = "CHAINLINK_APP_PROD_SVC"
ROLE      = "CHAINLINK_OPERATOR_ROLE"
WAREHOUSE = "PROD_WH"
DATABASE  = "TENANTUSERDB"
SCHEMA    = "CHAINLINK_SCH"
# -----------------------------------------------

def decrypt_pem_from_hex(hex_str: str, fernet_key: str) -> bytes:
    cipher_bytes = binascii.unhexlify(hex_str.strip())
    pem_bytes = Fernet(fernet_key.encode()).decrypt(cipher_bytes)
    if not pem_bytes.startswith(b"-----BEGIN PRIVATE KEY-----"):
        raise RuntimeError("Decrypted content is not a PKCS#8 PEM (missing header).")
    return pem_bytes

def calc_fp_from_pem(pem_bytes: bytes, passphrase: str | None) -> str:
    """
    Loads PEM (encrypted or not), derives public key, returns SHA256 FP.
    """
    try:
        key = serialization.load_pem_private_key(
            pem_bytes,
            password=(passphrase.encode() if passphrase else None),
            backend=default_backend()
        )
    except Exception as e:
        # Helpful diagnostics
        sample = pem_bytes.decode(errors="ignore").splitlines()[:6]
        hint = ""
        text = "\n".join(sample)
        if "ENCRYPTED" in text or "Proc-Type: 4,ENCRYPTED" in text:
            hint = "This PEM appears ENCRYPTED. Set PEM_PASSPHRASE to the correct passphrase."
        raise ValueError(f"Could not load PEM. {hint}\nTop lines:\n{text}") from e

    pub = key.public_key()
    spki_der = pub.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo
    )
    digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
    digest.update(spki_der)
    fp_b64 = base64.b64encode(digest.finalize()).decode()
    return "SHA256:" + fp_b64


def pem_to_pkcs8_der(pem_bytes: bytes, passphrase: str | None) -> bytes:
    """
    Normalizes any supported PEM (PKCS#1/PKCS#8, encrypted or not) into unencrypted PKCS#8 DER.
    """
    key = serialization.load_pem_private_key(
        pem_bytes,
        password=(passphrase.encode() if passphrase else None),
        backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def connect_with_der(der_bytes: bytes):
    conn = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=der_bytes,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        authenticator="SNOWFLAKE_JWT",  # explicit; connector infers from private_key
    )
    cur = conn.cursor()
    cur.execute("""
      SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(),
             CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
    """)
    print("CTX:", cur.fetchone())
    cur.close(); conn.close()

def main():
    # 1) Decrypt
    pem = decrypt_pem_from_hex(PRIVATE_KEY_ENCRYPTED_HEX, FERNET_KEY)
    lines = pem.decode(errors="ignore").splitlines()
    print(lines[0])
    print("...")
    print(lines[-1])

    # 2) Fingerprint
    fp = calc_fp_from_pem(pem, PEM_PASSPHRASE)
    print("Computed FP:", fp)
    print("Expected FP (from DESC USER): SHA256:mgzqO8l0EnG07m2SOO5FvMV8vtIJoxYI5A2We3hklQ8=")

    # 3) If matches, connect (optional)
    if fp.strip() == "SHA256:mgzqO8l0EnG07m2SOO5FvMV8vtIJoxYI5A2We3hklQ8=":
        print("Fingerprint matches Snowflake. Proceeding to connection smoke test...")
        der = pem_to_pkcs8_der(pem, PEM_PASSPHRASE)
        print("DER length:", len(der), "bytes")
        # Comment out the next line if you only want to verify the fingerprint
        connect_with_der(der)
    else:
        print("Fingerprint MISMATCH. Fix your stored ciphertext / Fernet key / Snowflake public key before connecting.")

if __name__ == "__main__":
    main()
