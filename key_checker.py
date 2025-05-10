
# key_checker.py (Streamlit version)

import streamlit as st
import binascii

st.title("🔍 Hex Decoder Test")

hex_string = st.text_area("Paste your HEX string here:")

if st.button("Decode Hex"):
    try:
        key_bytes = binascii.unhexlify(hex_string)
        st.success(f"✅ HEX decoding successful. Length = {len(key_bytes)} bytes")
    except Exception as e:
        st.error("❌ Error decoding hex:")
        st.exception(e)
