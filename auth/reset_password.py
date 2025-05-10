import streamlit as st
import bcrypt
import binascii
from datetime import datetime, timezone
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from sf_connector.service_connector import get_service_account_connection
from tenants.tenant_manager import load_tenant_config

# --- Password Strength Meter ---
def show_password_strength_meter(password):
    st.components.v1.html(f"""
        <script>
            function checkStrength(pw) {{
                let strength = 0;
                if (pw.length > 7) strength++;
                if (pw.match(/[a-z]/)) strength++;
                if (pw.match(/[A-Z]/)) strength++;
                if (pw.match(/[0-9]/)) strength++;
                if (pw.match(/[^a-zA-Z0-9]/)) strength++;

                let meter = document.getElementById("strengthMeter");
                let text = document.getElementById("strengthText");

                if (strength < 2) {{
                    meter.value = 1;
                    text.innerText = "Weak";
                    text.style.color = "red";
                }} else if (strength < 4) {{
                    meter.value = 2;
                    text.innerText = "Moderate";
                    text.style.color = "orange";
                }} else {{
                    meter.value = 3;
                    text.innerText = "Strong";
                    text.style.color = "green";
                }}
            }}
        </script>

        <div>
            <progress id="strengthMeter" max="3" value="0" style="width: 100%; height: 10px;"></progress>
            <p id="strengthText" style="margin: 0; font-weight: bold;">Strength</p>
        </div>

        <script>
            const input = window.parent.document.querySelector('input[type="password"]');
            if (input) {{
                input.addEventListener('input', () => checkStrength(input.value));
            }}
        </script>
    """, height=60)

# --- Reset Password Logic ---
def reset_password():
    st.title("Reset Your Password")

    reset_token = st.query_params.get("token")

    if not reset_token:
        st.error("This page can only be accessed via a valid reset link.")
        return

    # Track success state
    if "reset_successful" not in st.session_state:
        st.session_state.reset_successful = False

    if st.session_state.reset_successful:
        st.success("✅ Your password has been reset successfully.")
        if st.button("🔙 Return to Login"):
            st.query_params.clear()
            st.session_state.reset_successful = False
            st.rerun()
        return

    with st.form("reset_password_form"):
        email = st.text_input("Email")
        new_password = st.text_input("New Password", type="password")
        show_password_strength_meter(new_password)
        confirm_password = st.text_input("Confirm Password", type="password")
        submitted = st.form_submit_button("Reset Password")

        if submitted:
            if not email or not new_password or not confirm_password:
                st.warning("Please fill out all fields.")
                return

            if new_password != confirm_password:
                st.error("Passwords do not match.")
                return

            try:
                conn = get_service_account_connection()
                cur = conn.cursor()

                cur.execute("""
                    SELECT USER_ID, TENANT_ID FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    WHERE EMAIL = %s AND RESET_TOKEN = %s AND TOKEN_EXPIRY > CURRENT_TIMESTAMP
                """, (email, reset_token))
                user_info = cur.fetchone()

                if not user_info:
                    st.error("Invalid email or expired token.")
                    return

                user_id, tenant_id = user_info

                hashed_pw = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

                cur.execute("""
                    UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    SET HASHED_PASSWORD = %s,
                        RESET_TOKEN = NULL,
                        TOKEN_EXPIRY = NULL
                    WHERE EMAIL = %s AND RESET_TOKEN = %s
                """, (hashed_pw, email, reset_token))
                conn.commit()

                try:
                    cur.execute("""
                        INSERT INTO TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
                            (EMAIL, RESET_TOKEN, SUCCESS, IP_ADDRESS, REASON, TENANT_ID)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        email,
                        reset_token,
                        True,
                        "unknown",
                        "Password reset successful",
                        tenant_id
                    ))
                    conn.commit()
                except Exception as e:
                    st.warning(f"⚠️ Failed to write to RESET_LOGS: {e}")

                st.session_state.reset_successful = True
                st.rerun()

            except Exception as e:
                st.error("Error resetting password:")
                st.exception(e)
                try:
                    cur.execute("""
                        INSERT INTO TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
                            (EMAIL, RESET_TOKEN, SUCCESS, IP_ADDRESS, REASON, TENANT_ID)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        email,
                        reset_token,
                        False,
                        "unknown",
                        str(e),
                        tenant_id if 'tenant_id' in locals() else None
                    ))
                    conn.commit()
                except:
                    pass
            finally:
                cur.close()
                conn.close()

# Auto-run
if "token" in st.query_params:
    reset_password()
