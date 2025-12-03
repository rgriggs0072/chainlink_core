# ---------- forgot_password.py ----------

"""
Forgot Password Page (Chainlink Core)

Overview:
- User enters email address (case-insensitive).
- If email exists in USERDATA:
  - Generate a time-limited reset token.
  - Store RESET_TOKEN and TOKEN_EXPIRY in USERDATA.
  - Send reset email via utils.email_utils.send_reset_email.
- If email does not exist:
  - Show generic info message (avoid leaking account existence).
- All work happens inside a Streamlit form to avoid full-page reruns.

Notes for future devs:
- EMAIL matching is intentionally case-insensitive using LOWER(EMAIL) = LOWER(%s)
  so 'User@x.com' and 'user@x.com' behave the same.
- Consider logging email send result into RESET_LOGS so you can
  audit whether reset emails were actually sent, and why they failed.
"""

import streamlit as st
import secrets
from datetime import datetime, timedelta, timezone

from utils.email_utils import send_reset_email
from sf_connector.service_connector import get_service_account_connection


def forgot_password() -> None:
    """Render the forgot password form and handle reset link generation."""
    st.title("🔑 Forgot Password")

    with st.form("forgot_password_form"):
        email_input = st.text_input("Enter your email address")
        submit = st.form_submit_button("Send Reset Link")

        if not submit:
            return

        # Normalize user input (trim). Case-insensitivity is handled in SQL via LOWER().
        email = email_input.strip()

        if not email:
            st.warning("Please enter your email address.")
            return

        conn = get_service_account_connection()
        cur = conn.cursor()

        try:
            # Look up user by email (case-insensitive).
            # Assumes EMAIL is unique across tenants.
            cur.execute(
                """
                SELECT TENANT_ID, FIRST_NAME
                FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                WHERE LOWER(EMAIL) = LOWER(%s)
                """,
                (email,),
            )
            row = cur.fetchone()

            # If no account found, respond generically and exit.
            if not row:
                st.info("If that email is registered, a reset link will be sent.")
                return

            tenant_id, row_first_name = row
            if not row_first_name:
                row_first_name = "User"

            # Generate token + expiry
            token = secrets.token_urlsafe()
            expiry = datetime.now(timezone.utc) + timedelta(hours=1)

            # Store token + expiry in USERDATA (case-insensitive match)
            cur.execute(
                """
                UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                SET RESET_TOKEN = %s,
                    TOKEN_EXPIRY = %s
                WHERE LOWER(EMAIL) = LOWER(%s)
                """,
                (token, expiry, email),
            )
            conn.commit()

            # Send the reset email and inspect result
            email_result = send_reset_email(email, token, first_name=row_first_name)

            if not email_result["success"]:
                # We don't reveal whether the email exists, but we *do* tell the user
                # that email delivery failed in a generic way.
                st.error(
                    "We were unable to send the reset email. "
                    "Please try again in a few minutes or contact support."
                )
                # TODO: insert a row into RESET_LOGS with EMAIL_SENT = FALSE, EMAIL_ERROR = email_result["error"]
            else:
                st.success("If the email exists, a reset link has been sent.")

        except Exception as e:
            st.error("❌ Failed to process password reset.")
            st.exception(e)

        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
