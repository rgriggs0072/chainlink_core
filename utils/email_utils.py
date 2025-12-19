# ---------- email_utils.py ----------

"""
Email utilities for Chainlink Core.

Overview:
- Provides helpers to send transactional emails via Mailjet SMTP:
  - Password reset emails
  - Unlock notifications
- Uses Mailjet credentials from st.secrets["mailjet"].
- All send functions return a structured result so callers can log and react.

Notes for future devs:
- If you migrate to Mailjet's HTTP API, you can keep the same
  `send_reset_email` / `send_unlock_notification` interface by
  changing only the internals of `send_email`.
"""

import streamlit as st
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.templates import email_templates
from email.mime.application import MIMEApplication
from typing import Optional, List, Union



def get_mailjet_server() -> smtplib.SMTP:
    """
    Initialize and return an authenticated Mailjet SMTP server instance.

    - Reads API_KEY and SECRET_KEY from st.secrets["mailjet"].
    - Uses STARTTLS on port 587.

    Raises:
        Any smtplib or socket-related exceptions if connection/login fails.
    """
    creds = st.secrets["mailjet"]
    server = smtplib.SMTP("in-v3.mailjet.com", 587)
    server.starttls()
    server.login(creds["API_KEY"], creds["SECRET_KEY"])
    return server


def send_email(to_email: str, subject: str, html: str, from_email: str) -> dict:
    """
    Low-level helper to send an HTML email via Mailjet SMTP.

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        html: Full HTML body of the email.
        from_email: Sender email address (must be authorized in Mailjet).

    Returns:
        dict with:
          - success (bool): True if no exception occurred during send.
          - error (str | None): Error message if any.
    """
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html"))

    try:
        server = get_mailjet_server()
        server.send_message(msg)
        server.quit()

        logging.info(f"Email sent to {to_email} with subject '{subject}'")
        return {"success": True, "error": None}

    except Exception as e:
        # User-facing warning in Streamlit
        st.warning(f"⚠️ Failed to send email to {to_email}")
        st.exception(e)

        # Also log for backend debugging
        logging.exception(f"Error sending email to {to_email}: {e}")

        return {"success": False, "error": str(e)}


def send_email_with_attachment(
    to_email: str,
    subject: str,
    html: str,
    from_email: str,
    cc_email: Optional[str] = None,
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: str = "attachment.pdf",
    attachment_mime: str = "application/pdf",
) -> dict:
    """
    Send an HTML email via Mailjet SMTP with optional CC + single attachment.

    Args:
        to_email: Primary recipient.
        cc_email: Optional CC recipient (e.g., manager).
        attachment_bytes: Raw bytes for the attachment.
        attachment_filename: Filename shown in the email client.
        attachment_mime: MIME type, default application/pdf.

    Returns:
        dict: {"success": bool, "error": str|None}
    """
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    if cc_email:
        msg["Cc"] = cc_email
    msg["Subject"] = subject

    msg.attach(MIMEText(html, "html"))

    if attachment_bytes:
        part = MIMEApplication(attachment_bytes, _subtype=attachment_mime.split("/")[-1])
        part.add_header("Content-Disposition", "attachment", filename=attachment_filename)
        msg.attach(part)

    try:
        server = get_mailjet_server()
        recipients = [to_email] + ([cc_email] if cc_email else [])
        server.sendmail(from_email, recipients, msg.as_string())
        server.quit()

        logging.info(f"Email sent to {to_email} (cc={cc_email}) subject='{subject}' attach={bool(attachment_bytes)}")
        return {"success": True, "error": None}

    except Exception as e:
        st.warning(f"⚠️ Failed to send email to {to_email}")
        st.exception(e)
        logging.exception(f"Error sending email to {to_email}: {e}")
        return {"success": False, "error": str(e)}



def send_reset_email(email: str, token: str, first_name: str = "User") -> dict:
    """
    Send a password reset email with a time-limited token.

    - Uses st.secrets["app"]["base_url"] as the deployed Streamlit base URL.
    - Builds a reset link that hits the /reset_password route with ?token=...
    - Returns the result dict from send_email() so callers can log outcome.

    Args:
        email: Recipient email address.
        token: Time-limited reset token stored in USERDATA.
        first_name: Optional first name for personalization.

    Returns:
        dict: {"success": bool, "error": str | None}
    """
    sender_email = "randy@chainlinkanalytics.com"

    # Base URL of the deployed app, e.g. "https://chainlinkcore-main.streamlit.app"
    app_base_url = st.secrets["app"]["base_url"].rstrip("/")

    # If your reset page is /reset_password:
    reset_link = f"{app_base_url}/reset_password?token={token}"

    # If your router uses /?page=reset_password&token=..., swap to:
    # reset_link = f"{app_base_url}/?page=reset_password&token={token}"

    subject = "Chainlink Analytics Password Reset"
    html = email_templates.reset_password_template(first_name, reset_link)

    result = send_email(email, subject, html, sender_email)

    # Optional: in dev you can surface the link + result for debugging.
    # st.write("DEBUG reset link:", reset_link, "email result:", result)

    return result


def send_unlock_notification(
    email: str,
    first_name: str = "User",
    unlocker_name: str = "an admin",
) -> dict:
    """
    Send an account unlock notification email.

    Args:
        email: Recipient email address.
        first_name: Optional recipient first name.
        unlocker_name: Name/role of the admin who unlocked the account.

    Returns:
        dict: {"success": bool, "error": str | None}
    """
    sender_email = "randy@chainlinkanalytics.com"
    subject = "Your Chainlink Account Has Been Unlocked"
    html = email_templates.unlock_notification_template(first_name, unlocker_name)

    return send_email(email, subject, html, sender_email)
