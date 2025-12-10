from __future__ import annotations

"""
Email Gap Report – email & logging helpers (SMTP / Mailjet).

Overview
--------
- Uses utils.email_utils.get_mailjet_server() to create an authenticated
  Mailjet SMTP server.
- Sends:
    * One HTML email per salesperson (their gap report).
    * One HTML summary email per manager (summary table only).
- Logs every send attempt into EMAIL_GAP_LOGS in the tenant DB.

EMAIL_GAP_LOGS schema (per tenant DB) should look roughly like:

CREATE TABLE IF NOT EXISTS EMAIL_GAP_LOGS (
    ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    TENANT_ID       NUMBER           NOT NULL,
    SALESPERSON_ID  NUMBER,
    SALESPERSON_NAME VARCHAR(255),
    RECIPIENT_EMAIL VARCHAR(320)     NOT NULL,
    RECIPIENT_ROLE  VARCHAR(20),      -- 'SALESPERSON' or 'MANAGER'
    STATUS          VARCHAR(20)       NOT NULL, -- 'SUCCESS' / 'FAILED'
    ERROR_MESSAGE   VARCHAR(1000),
    SENT_AT         TIMESTAMP_LTZ    DEFAULT CURRENT_TIMESTAMP
);
"""

from typing import Dict, Any, List, Tuple

import pandas as pd
import streamlit as st
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from utils.email_utils import get_mailjet_server  # your existing SMTP helper


# -------------------------------------------------------------------
# Low-level SMTP sender (Mailjet via get_mailjet_server)
# -------------------------------------------------------------------
def _send_mailjet_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_body: str,
    sender_email: str,
    sender_name: str = "Chainlink Analytics",
) -> Tuple[bool, str | None]:
    """
    Send a single HTML email using the shared Mailjet SMTP server.

    Returns:
        (success_flag, error_message_or_None)
    """
    try:
        server = get_mailjet_server()  # already logged in via secrets.toml

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = to_email

        html_part = MIMEText(html_body, "html")
        msg.attach(html_part)

        server.sendmail(sender_email, [to_email], msg.as_string())
        server.quit()
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


# -------------------------------------------------------------------
# Data access helpers
# -------------------------------------------------------------------
def fetch_sales_contacts(conn, tenant_id: int) -> pd.DataFrame:
    """
    Fetch active SALES_CONTACTS rows for a tenant.

    Expected columns:
    - TENANT_ID
    - SALESPERSON_ID
    - SALESPERSON_NAME
    - SALESPERSON_EMAIL
    - MANAGER_ID
    - MANAGER_NAME
    - MANAGER_EMAIL
    - IS_ACTIVE
    """
    sql = """
        SELECT
            TENANT_ID,
            SALESPERSON_ID,
            SALESPERSON_NAME,
            SALESPERSON_EMAIL,
            MANAGER_ID,
            MANAGER_NAME,
            MANAGER_EMAIL,
            IS_ACTIVE
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND IS_ACTIVE = TRUE
    """
    return conn.cursor().execute(sql, (tenant_id,)).fetch_pandas_all()


def log_email_gap(
    conn,
    tenant_id: int,
    salesperson_id: int | None,
    salesperson_name: str | None,
    recipient_email: str,
    recipient_role: str,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Insert a row into EMAIL_GAP_LOGS.

    recipient_role: 'SALESPERSON' or 'MANAGER'
    status: 'SUCCESS' or 'FAILED'
    """
    insert_sql = """
        INSERT INTO EMAIL_GAP_LOGS (
            TENANT_ID,
            SALESPERSON_ID,
            SALESPERSON_NAME,
            RECIPIENT_EMAIL,
            RECIPIENT_ROLE,
            STATUS,
            ERROR_MESSAGE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    conn.cursor().execute(
        insert_sql,
        (
            tenant_id,
            salesperson_id,
            salesperson_name,
            recipient_email,
            recipient_role,
            status,
            error_message,
        ),
    )


# -------------------------------------------------------------------
# High-level orchestrator
# -------------------------------------------------------------------
def send_all_gap_emails(
    conn,
    tenant_id: int,
    html_by_salesperson: Dict[str, str],
    sender_email: str,
    manager_html_by_manager: Dict[str, str] | None = None,
    email_subject_salesperson: str = "Your Weekly Gap Report",
    email_subject_manager: str = "Team Weekly Gap Reports",
) -> dict[str, Any]:
    """
    Send all gap emails for a run.

    Flow:
    -----
    1) Fetch SALES_CONTACTS for this tenant.
    2) For each salesperson in html_by_salesperson:
       - Match on SALESPERSON_NAME (case-insensitive).
       - Send individual HTML email to SALESPERSON_EMAIL.
       - Log result to EMAIL_GAP_LOGS.
       - Track which managers are involved (MANAGER_EMAIL + MANAGER_NAME).
    3) For each manager involved:
       - If manager_html_by_manager is provided and has an entry for that
         manager_name, send THAT summary HTML (no line items).
       - Otherwise, fallback to a combined-detail email.

    Args
    ----
    conn:
        Tenant-scoped Snowflake connection.
    tenant_id:
        Tenant ID (matches SALES_CONTACTS.TENANT_ID).
    html_by_salesperson:
        Dict of {SALESPERSON_NAME: html_string}.
    sender_email:
        From address (e.g. 'randy@chainlinkanalytics.com').
    manager_html_by_manager:
        Optional dict of {MANAGER_NAME: summary_html} built on the page
        via build_manager_summaries. If supplied, these are used as the
        manager email bodies (summary-only).

    Returns
    -------
    dict:
        {
            "salesperson_success": int,
            "salesperson_fail": int,
            "manager_success": int,
            "manager_fail": int,
            "skipped_salespeople": [names_not_found_in_contacts]
        }
    """
    contacts_df = fetch_sales_contacts(conn, tenant_id)

    if contacts_df.empty:
        st.warning("No SALES_CONTACTS found for this tenant.")
        return {
            "salesperson_success": 0,
            "salesperson_fail": 0,
            "manager_success": 0,
            "manager_fail": 0,
            "skipped_salespeople": list(html_by_salesperson.keys()),
        }

    # Normalize salesperson names
    contacts_df["SP_NORM"] = (
        contacts_df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    )
    contact_map = {row["SP_NORM"]: row for _, row in contacts_df.iterrows()}

    salesperson_success = 0
    salesperson_fail = 0
    manager_success = 0
    manager_fail = 0
    skipped_salespeople: List[str] = []

    # Track which managers are actually involved in this run
    # mgr_email -> {"manager_name": str}
    managers_seen: Dict[str, dict] = {}

    # -----------------------------
    # Pass 1: salesperson emails
    # -----------------------------
    for sp_name, html_body in html_by_salesperson.items():
        sp_key = sp_name.strip().upper()
        row = contact_map.get(sp_key)

        if row is None:
            skipped_salespeople.append(sp_name)
            continue

        sp_email = row["SALESPERSON_EMAIL"]
        sp_id = row["SALESPERSON_ID"]
        mgr_email = row["MANAGER_EMAIL"]
        mgr_name = row["MANAGER_NAME"]

        # Send salesperson email
        ok, err = _send_mailjet_email(
            to_email=sp_email,
            to_name=sp_name,
            subject=email_subject_salesperson,
            html_body=html_body,
            sender_email=sender_email,
        )

        if ok:
            salesperson_success += 1
            log_email_gap(
                conn,
                tenant_id,
                sp_id,
                sp_name,
                sp_email,
                "SALESPERSON",
                "SUCCESS",
            )
        else:
            salesperson_fail += 1
            log_email_gap(
                conn,
                tenant_id,
                sp_id,
                sp_name,
                sp_email,
                "SALESPERSON",
                "FAILED",
                err,
            )

        # Track manager for this salesperson
        if pd.notna(mgr_email) and mgr_email:
            managers_seen[mgr_email] = {
                "manager_name": mgr_name,
            }

    # -----------------------------
    # Pass 2: manager summary emails
    # -----------------------------
    for mgr_email, meta in managers_seen.items():
        mgr_name = meta.get("manager_name") or "Manager"

        # Prefer summary HTML if provided
        html_to_send: str | None = None
        if manager_html_by_manager:
            html_to_send = manager_html_by_manager.get(mgr_name)

        if not html_to_send:
            # Fallback: simple generic message if no summary HTML available
            html_to_send = f"""
            <html>
              <body style="font-family: Arial, sans-serif;">
                <h1>Team Weekly Gap Reports</h1>
                <p>Hello {mgr_name},</p>
                <p>Gap reports were generated for your team in Chainlink.</p>
                <p>Please log into Chainlink to view full details.</p>
              </body>
            </html>
            """

        ok, err = _send_mailjet_email(
            to_email=mgr_email,
            to_name=mgr_name,
            subject=email_subject_manager,
            html_body=html_to_send,
            sender_email=sender_email,
        )

        if ok:
            manager_success += 1
            log_email_gap(
                conn,
                tenant_id,
                None,
                None,
                mgr_email,
                "MANAGER",
                "SUCCESS",
            )
        else:
            manager_fail += 1
            log_email_gap(
                conn,
                tenant_id,
                None,
                None,
                mgr_email,
                "MANAGER",
                "FAILED",
                err,
            )

    return {
        "salesperson_success": salesperson_success,
        "salesperson_fail": salesperson_fail,
        "manager_success": manager_success,
        "manager_fail": manager_fail,
        "skipped_salespeople": skipped_salespeople,
    }
