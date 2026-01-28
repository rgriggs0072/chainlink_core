# ---------- utils/email_utils.py ----------
# -*- coding: utf-8 -*-
"""
Email utilities for Chainlink Core.

Page overview
-------------
Centralized helpers for sending transactional emails via Mailjet SMTP:
- Password reset emails
- Unlock notifications
- Generic HTML emails (optionally with attachment + CC list)

Key rules / gotchas
-------------------
- SMTP sendmail() requires *a list of recipient addresses*.
  If you pass a comma-separated string as one entry, only the first address
  may work (or behavior can be inconsistent).
- This module normalizes CC into a clean list, dedupes, and always passes a
  proper recipient list to SMTP.

Design notes
------------
- This module is Streamlit-aware (uses st.secrets for Mailjet creds).
- It avoids crashing callers: send functions return a structured result dict.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Sequence, Union

import streamlit as st

from utils.templates import email_templates


# =============================================================================
# Mailjet SMTP connection
# =============================================================================
def get_mailjet_server() -> smtplib.SMTP:
    """
    Initialize and return an authenticated Mailjet SMTP server instance.

    - Reads API_KEY and SECRET_KEY from st.secrets["mailjet"].
    - Uses STARTTLS on port 587.

    Returns:
        Authenticated smtplib.SMTP instance.

    Raises:
        Any smtplib/socket-related exception if connection/login fails.
    """
    creds = st.secrets["mailjet"]
    server = smtplib.SMTP("in-v3.mailjet.com", 587)
    server.starttls()
    server.login(creds["API_KEY"], creds["SECRET_KEY"])
    return server


# =============================================================================
# Recipient normalization
# =============================================================================
def _split_emails(value: str) -> List[str]:
    """
    Split a string that may contain multiple emails separated by commas/semicolons/spaces.

    Examples:
        "a@x.com,b@y.com" -> ["a@x.com","b@y.com"]
        "a@x.com; b@y.com" -> ["a@x.com","b@y.com"]
        "a@x.com b@y.com" -> ["a@x.com","b@y.com"]  (best-effort)

    Notes:
        We keep validation light; SMTP server will reject truly invalid addresses.
    """
    if not value:
        return []

    s = str(value).strip()
    if not s:
        return []

    # Normalize separators to commas
    s = s.replace(";", ",").replace("\n", ",").replace("\r", ",").replace("\t", " ")
    # Some users paste with spaces; if there are no commas but spaces exist, split on spaces
    if "," not in s and " " in s:
        parts = [p.strip() for p in s.split(" ") if p.strip()]
    else:
        parts = [p.strip() for p in s.split(",") if p.strip()]

    # Remove empty and obvious garbage
    return [p for p in parts if p and p.lower() not in {"none", "null", "nan"}]


def _normalize_cc(
    cc: Optional[Union[str, Sequence[str]]],
) -> List[str]:
    """
    Normalize CC input into a clean list of unique emails.

    Supports:
      - None
      - Single string (may be comma-separated)
      - List/tuple of strings (each may be comma-separated)

    Returns:
      Deduped list, preserving order.
    """
    if not cc:
        return []

    items: List[str] = []
    if isinstance(cc, str):
        items.extend(_split_emails(cc))
    else:
        for x in cc:
            if x is None:
                continue
            items.extend(_split_emails(str(x)))

    # Deduplicate while preserving order
    seen = set()
    out: List[str] = []
    for e in items:
        e_norm = e.strip()
        if not e_norm:
            continue
        key = e_norm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(e_norm)

    return out


# =============================================================================
# Core senders
# =============================================================================
def send_email(to_email: str, subject: str, html: str, from_email: str) -> Dict[str, object]:
    """
    Send an HTML email via Mailjet SMTP.

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        html: Full HTML body of the email.
        from_email: Sender email address (must be authorized in Mailjet).

    Returns:
        dict with:
          - success (bool)
          - error (str | None)
          - to (str)
          - cc (list[str])
          - recipients (list[str])  # actual SMTP recipients
    """
    to_email = str(to_email or "").strip()
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html or "", "html"))

    server = None
    try:
        server = get_mailjet_server()
        recipients = [to_email]
        server.sendmail(from_email, recipients, msg.as_string())
        logging.info("Email sent to=%s subject=%s", to_email, subject)
        return {"success": True, "error": None, "to": to_email, "cc": [], "recipients": recipients}
    except Exception as e:
        logging.exception("Error sending email to %s: %s", to_email, e)
        # keep Streamlit surface, but don't hard-crash callers
        try:
            st.warning(f"⚠️ Failed to send email to {to_email}")
            st.exception(e)
        except Exception:
            pass
        return {"success": False, "error": str(e), "to": to_email, "cc": [], "recipients": [to_email]}
    finally:
        try:
            if server:
                server.quit()
        except Exception:
            pass


def send_email_with_attachment(
    to_email: str,
    subject: str,
    html: str,
    from_email: str,
    cc_email: Optional[Union[str, Sequence[str]]] = None,
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: str = "attachment.pdf",
    attachment_mime: str = "application/pdf",
) -> Dict[str, object]:
    """
    Send an HTML email via Mailjet SMTP with optional CC list + single attachment.

    Args:
        to_email: Primary recipient.
        subject: Subject line.
        html: HTML body.
        from_email: Sender.
        cc_email: Optional CC recipient(s):
            - None
            - string (single or comma-separated)
            - list/tuple of strings (each may be comma-separated)
        attachment_bytes: Raw bytes for the attachment (optional).
        attachment_filename: Filename shown in email client.
        attachment_mime: MIME type (default "application/pdf").

    Returns:
        dict with:
          - success (bool)
          - error (str | None)
          - to (str)
          - cc (list[str])
          - recipients (list[str])  # actual SMTP recipients
    """
    to_email = str(to_email or "").strip()
    cc_list = _normalize_cc(cc_email)

    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    if cc_list:
        # Display header: comma-separated
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject

    msg.attach(MIMEText(html or "", "html"))

    if attachment_bytes:
        subtype = (attachment_mime.split("/")[-1] or "octet-stream").strip()
        part = MIMEApplication(attachment_bytes, _subtype=subtype)
        part.add_header("Content-Disposition", "attachment", filename=attachment_filename)
        msg.attach(part)

   
    server = None
    recipients = [to_email] + cc_list  # already normalized/deduped

    try:
        server = get_mailjet_server()

        # IMPORTANT: sendmail returns a dict of refused recipients (if any)
        refused = server.sendmail(from_email, recipients, msg.as_string()) or {}

        # If anyone was refused, treat as partial failure and report truth
        refused_list = sorted(list(refused.keys()))
        ok = (len(refused_list) == 0)

        logging.info(
            "Email send result ok=%s to=%s cc=%s refused=%s subject=%s",
            ok, to_email, cc_list, refused_list, subject
        )

        return {
            "success": ok,
            "error": None if ok else f"Some recipients were refused: {refused_list}",
            "to": to_email,
            "cc": cc_list,
            "recipients": recipients,
            "refused": refused_list,
        }

    except Exception as e:
        logging.exception("Error sending email to %s (cc=%s): %s", to_email, cc_list, e)
        try:
            st.warning(f"⚠️ Failed to send email to {to_email}")
            st.exception(e)
        except Exception:
            pass
        return {
            "success": False,
            "error": str(e),
            "to": to_email,
            "cc": cc_list,
            "recipients": recipients,
            "refused": [],
        }
    finally:
        try:
            if server:
                server.quit()
        except Exception:
            pass



# =============================================================================
# App-specific emails
# =============================================================================
def send_reset_email(email: str, token: str, first_name: str = "User") -> Dict[str, object]:
    """
    Send a password reset email with a time-limited token.

    - Uses st.secrets["app"]["base_url"] as the deployed base URL.
    - Builds reset link to /reset_password?token=...

    Returns:
        dict from send_email()
    """
    sender_email = "randy@chainlinkanalytics.com"

    app_base_url = st.secrets["app"]["base_url"].rstrip("/")
    reset_link = f"{app_base_url}/reset_password?token={token}"

    subject = "Chainlink Analytics Password Reset"
    html = email_templates.reset_password_template(first_name, reset_link)

    return send_email(str(email or "").strip(), subject, html, sender_email)


def send_unlock_notification(
    email: str,
    first_name: str = "User",
    unlocker_name: str = "an admin",
) -> Dict[str, object]:
    """
    Send an account unlock notification email.

    Returns:
        dict from send_email()
    """
    sender_email = "randy@chainlinkanalytics.com"
    subject = "Your Chainlink Account Has Been Unlocked"
    html = email_templates.unlock_notification_template(first_name, unlocker_name)

    return send_email(str(email or "").strip(), subject, html, sender_email)
