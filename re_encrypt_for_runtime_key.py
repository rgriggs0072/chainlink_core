# encrypt_pkcs8_to_hex.py
import binascii
from cryptography.fernet import Fernet
FERNET_KEY = "Le2iXDuyX2ZiXdoeESwspAgqIjQMSIupcKwRYvqODAE="  # your prod key
pem = open(r"E:\Development\chainlink_core\pkcs8_unenc.pem","rb").read()
hex_out = binascii.hexlify(Fernet(FERNET_KEY.encode()).encrypt(pem)).decode()
print(hex_out)

