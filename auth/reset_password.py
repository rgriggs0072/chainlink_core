"""
auth/reset_password.py

Reset Password Page (Chainlink Core)

Overview:
- This page is reached via a reset link that includes a `token` query param:
    https://<your-app>/reset_password?token=<RESET_TOKEN>

Flow:
1) Read `token` from st.query_params.
2) Look up USERDATA row by RESET_TOKEN.
3) Verify TOKEN_EXPIRY is in the future (UTC).
4) Prompt the user for a new password + confirmation.
5) Hash and save the new password, clear RESET_TOKEN + TOKEN_EXPIRY.
6) Log the outcome in RESET_LOGS.

Important:
- We do NOT ask for the email again. The reset token uniquely identifies the user.
- This avoids all email casing issues and extra failure modes.
"""

import streamlit as st
import bcrypt
from datetime import datetime, timezone

from sf_connector.service_connector import get_service_account_connection


# ---------------------------------------------------------------------
# Password Strength Meter
# ---------------------------------------------------------------------
def show_password_strength_meter(password: str) -> None:
    """
    Render a client-side password strength meter.

    Implementation notes:
    - Injects a small JS snippet via Streamlit components.
    - Watches the first <input type="password"> in the parent document.
    - Updates a progress bar + label (Weak / Moderate / Strong) as the user types.
    """
    st.components.v1.html(
        """
        <script>
            function checkStrength(pw) {
                let strength = 0;
                if (pw.length > 7) strength++;
                if (pw.match(/[a-z]/)) strength++;
                if (pw.match(/[A-Z]/)) strength++;
                if (pw.match(/[0-9]/)) strength++;
                if (pw.match(/[^a-zA-Z0-9]/)) strength++;

                let meter = document.getElementById("strengthMeter");
                let text = document.getElementById("strengthText");

                if (strength < 2) {
                    meter.value = 1;
                    text.innerText = "Weak";
                    text.style.color = "red";
                } else if (strength < 4) {
                    meter.value = 2;
                    text.innerText = "Moderate";
                    text.style.color = "orange";
                } else {
                    meter.value = 3;
                    text.innerText = "Strong";
                    text.style.color = "green";
                }
            }
        </script>

        <div>
            <progress id="strengthMeter" max="3" value="0"
                      style="width: 100%; height: 10px;"></progress>
            <p id="strengthText" style="margin: 0; font-weight: bold;">Strength</p>
        </div>

        <script>
            const input = window.parent.document.querySelector('input[type="password"]');
            if (input) {
                input.addEventListener('input', () => checkStrength(input.value));
            }
        </script>
        """,
        height=60,
    )


# ---------------------------------------------------------------------
# Reset Password Logic (token-only)
# ---------------------------------------------------------------------
def reset_password() -> None:
    """
    Main reset password handler (token-only, no email input).

    Token-based logic:
    - Reads `token` from query params.
    - Validates that a USERDATA row exists with this RESET_TOKEN.
    - Ensures TOKEN_EXPIRY > current UTC time.
    - Prompts for a new password + confirmation.
    - Hashes and saves the new password, clears the token, logs to RESET_LOGS.
    """
    st.title("Reset Your Password – TOKEN ONLY")

    # --- 1) Get reset token from URL query params (handle list vs str) ---
    raw_token = st.query_params.get("token")
    if isinstance(raw_token, list):
        reset_token = raw_token[0] if raw_token else None
    else:
        reset_token = raw_token

    if not reset_token:
        st.error("This page can only be accessed via a valid reset link.")
        return

    # Track success state so we can show a clean confirmation screen
    if "reset_successful" not in st.session_state:
        st.session_state.reset_successful = False

    if st.session_state.reset_successful:
        st.success("✅ Your password has been reset successfully.")
        if st.button("🔐 Return to Login"):
            # Clear query params and reset state, then rerun
            st.query_params.clear()
            st.session_state.reset_successful = False
            st.rerun()
        return

    # --- 2) Connect and look up the user by reset token ---
    conn = get_service_account_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT USER_ID, TENANT_ID, EMAIL, TOKEN_EXPIRY
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE RESET_TOKEN = %s
            """,
            (reset_token,),
        )
        user_row = cur.fetchone()

        if not user_row:
            st.error("Invalid or expired reset link.")
            return

        user_id, tenant_id, email_db, token_expiry = user_row

        # --- 3) Token expiry check (normalize naive vs aware) ---
        now_utc = datetime.now(timezone.utc)

        if token_expiry is None:
            expired = True
        else:
            # Snowflake usually returns naive timestamps (no tzinfo).
            # Normalize everything to UTC-aware for a safe comparison.
            if token_expiry.tzinfo is None:
                token_expiry_cmp = token_expiry.replace(tzinfo=timezone.utc)
            else:
                token_expiry_cmp = token_expiry.astimezone(timezone.utc)

            expired = now_utc > token_expiry_cmp

        if expired:
            st.error("This reset link has expired. Please request a new one.")
            return

        # --- 4) Password entry form (NO email field) ---
        with st.form("reset_password_form"):
            new_password = st.text_input("New Password", type="password")
            show_password_strength_meter(new_password)
            confirm_password = st.text_input("Confirm Password", type="password")
            submitted = st.form_submit_button("Reset Password")

        if not submitted:
            return

        if not new_password or not confirm_password:
            st.warning("Please fill out both password fields.")
            return

        if new_password != confirm_password:
            st.error("Passwords do not match.")
            return

        # TODO: enforce your password strength policy here (min length, complexity, etc.)

        # --- 5) Hash password and update USERDATA ---
        hashed_pw = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

        cur.execute(
            """
            UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
            SET HASHED_PASSWORD = %s,
                RESET_TOKEN = NULL,
                TOKEN_EXPIRY = NULL
            WHERE USER_ID = %s
              AND RESET_TOKEN = %s
            """,
            (hashed_pw, user_id, reset_token),
        )
        conn.commit()

        # --- 6) Log success to RESET_LOGS ---
        try:
            cur.execute(
                """
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
                    (EMAIL, RESET_TOKEN, SUCCESS, IP_ADDRESS, REASON, TENANT_ID)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    email_db,
                    reset_token,
                    True,
                    "unknown",  # TODO: wire real IP if/when you capture IP
                    "Password reset successful",
                    tenant_id,
                ),
            )
            conn.commit()
        except Exception as log_err:
            st.warning(f"⚠️ Failed to write to RESET_LOGS: {log_err}")

        st.session_state.reset_successful = True
        st.rerun()

    except Exception as e:
        st.error("Error resetting password:")
        st.exception(e)

        # Attempt to log failure (best-effort; don't break UX if this fails)
        try:
            cur.execute(
                """
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
                    (EMAIL, RESET_TOKEN, SUCCESS, IP_ADDRESS, REASON, TENANT_ID)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    email_db if "email_db" in locals() else None,
                    reset_token,
                    False,
                    "unknown",
                    str(e),
                    tenant_id if "tenant_id" in locals() else None,
                ),
            )
            conn.commit()
        except Exception:
            pass

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Auto-run if token is present in URL
# ---------------------------------------------------------------------
if "token" in st.query_params:
    reset_password()
