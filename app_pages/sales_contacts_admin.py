# ---------------- app_pages/sales_contacts_admin.py ----------------
"""
Sales Contacts Admin Page

Overview for future devs
------------------------
This page lets tenant admins manage the SALES_CONTACTS table used by
Email Gap Report and future auto-email flows.

Table schema (per tenant)
-------------------------
CREATE OR REPLACE TABLE SALES_CONTACTS (
    TENANT_ID         NUMBER(10,0)   NOT NULL,
    SALESPERSON_ID    NUMBER(10,0),
    SALESPERSON_NAME  STRING         NOT NULL,
    SALESPERSON_EMAIL STRING         NOT NULL,
    MANAGER_ID        NUMBER(10,0),
    MANAGER_NAME      STRING,
    MANAGER_EMAIL     STRING,
    IS_ACTIVE         BOOLEAN        DEFAULT TRUE,
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT        TIMESTAMP_NTZ
);

Key decisions
-------------
- We always filter by TENANT_ID from st.session_state["tenant_id"].
- We do NOT require SALESPERSON_ID or MANAGER_ID for now (left NULL).
- Manual add + bulk upload both effectively behave as UPSERT:
    - MATCH on (TENANT_ID, UPPER(SALESPERSON_NAME))
    - UPDATE SALESPERSON_EMAIL / MANAGER_NAME / MANAGER_EMAIL / IS_ACTIVE / UPDATED_AT
    - INSERT when no match.
"""

from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

import pandas as pd
import streamlit as st


# -------------------------------------------------------------------
# Core helpers: connection + current data
# -------------------------------------------------------------------

def _get_tenant_conn_and_id():
    """
    Fetch the tenant Snowflake connection and TENANT_ID from session state.

    Returns
    -------
    tuple[conn, tenant_id] or (None, None) if missing.
    """
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if conn is None:
        st.error("❌ No tenant Snowflake connection found in session.")
        return None, None
    if tenant_id is None:
        st.error("❌ TENANT_ID missing from session. Cannot scope SALES_CONTACTS.")
        return None, None

    return conn, tenant_id


def _fetch_sales_contacts(conn, tenant_id: int) -> pd.DataFrame:
    """
    Load all sales contacts for the current tenant.

    We keep the view minimal and tenant-scoped:
    - Only rows where TENANT_ID = :tenant_id
    - Show core identity + email + active flag + timestamps
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    SALESPERSON_ID,
                    SALESPERSON_NAME,
                    SALESPERSON_EMAIL,
                    MANAGER_NAME,
                    MANAGER_EMAIL,
                    IS_ACTIVE,
                    CREATED_AT,
                    UPDATED_AT
                FROM SALES_CONTACTS
                WHERE TENANT_ID = %s
                ORDER BY UPPER(SALESPERSON_NAME)
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    except Exception as e:
        st.error(
            "❌ Could not query SALES_CONTACTS. "
            "Please ensure the table exists and you have SELECT on it.\n\n"
            f"Error: {e}"
        )
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=cols)


# -------------------------------------------------------------------
# Template + parsing helpers for bulk upload
# -------------------------------------------------------------------

def _build_contacts_template_df() -> pd.DataFrame:
    """
    Build a minimal Excel template for SALES_CONTACTS bulk upload.

    Columns:
    - SALESPERSON_NAME   (required; must match GAP_REPORT.SALESPERSON)
    - SALESPERSON_EMAIL  (required)
    - MANAGER_NAME       (optional)
    - MANAGER_EMAIL      (optional)
    - IS_ACTIVE          (optional; defaults to TRUE if blank)

    Example row is included for guidance.
    """
    data = [
        {
            "SALESPERSON_NAME": "Jane Doe",
            "SALESPERSON_EMAIL": "jane.doe@example.com",
            "MANAGER_NAME": "John Manager",
            "MANAGER_EMAIL": "john.manager@example.com",
            "IS_ACTIVE": "Y",  # accepted: Y/YES/TRUE/1 for active
        }
    ]
    return pd.DataFrame(data)


def _build_contacts_template_xlsx() -> BytesIO:
    """
    Build an in-memory .xlsx template file for SALES_CONTACTS bulk upload.
    """
    df = _build_contacts_template_df()
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf


def _normalize_active_flag(val) -> bool:
    """
    Convert various string/numeric representations into a boolean IS_ACTIVE flag.

    Rules:
    - Blank / NaN -> True (default to active unless explicitly turned off)
    - 'Y', 'YES', 'TRUE', '1' (case-insensitive) -> True
    - 'N', 'NO', 'FALSE', '0' (case-insensitive) -> False
    - Any other non-empty -> True (treat unknown markers as active)
    """
    if pd.isna(val):
        return True

    s = str(val).strip().upper()
    if s == "":
        return True

    if s in {"Y", "YES", "TRUE", "1"}:
        return True
    if s in {"N", "NO", "FALSE", "0"}:
        return False

    return True


def _parse_contacts_upload(uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parse an uploaded Excel file into a normalized DataFrame.

    Returns
    -------
    df : DataFrame
        Columns: SALESPERSON_NAME, SALESPERSON_EMAIL, MANAGER_NAME, MANAGER_EMAIL, IS_ACTIVE (bool)
    warnings : list[str]
        Any non-fatal data quality messages (dropped rows, etc.).
    """
    warnings: List[str] = []

    try:
        df_raw = pd.read_excel(uploaded_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Could not read uploaded Excel file: {e}")
        return pd.DataFrame(), warnings

    df = df_raw.copy()
    df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]

    required_cols = {"SALESPERSON_NAME", "SALESPERSON_EMAIL"}
    missing = required_cols - set(df.columns)
    if missing:
        st.error(
            "❌ Uploaded file is missing required columns: "
            + ", ".join(sorted(missing))
        )
        return pd.DataFrame(), warnings

    # Normalize core fields
    df["SALESPERSON_NAME"] = df["SALESPERSON_NAME"].astype(str).str.strip()
    df["SALESPERSON_EMAIL"] = df["SALESPERSON_EMAIL"].astype(str).str.strip()

    # Optional manager fields
    if "MANAGER_NAME" not in df.columns:
        df["MANAGER_NAME"] = ""
    else:
        df["MANAGER_NAME"] = df["MANAGER_NAME"].fillna("").astype(str).str.strip()

    if "MANAGER_EMAIL" not in df.columns:
        df["MANAGER_EMAIL"] = ""
    else:
        df["MANAGER_EMAIL"] = df["MANAGER_EMAIL"].fillna("").astype(str).str.strip()

    # IS_ACTIVE normalization
    if "IS_ACTIVE" not in df.columns:
        df["IS_ACTIVE"] = True
    else:
        df["IS_ACTIVE"] = df["IS_ACTIVE"].apply(_normalize_active_flag)

    # Drop rows with missing key fields
    before = len(df)
    df = df[(df["SALESPERSON_NAME"] != "") & (df["SALESPERSON_EMAIL"] != "")]
    dropped = before - len(df)
    if dropped > 0:
        warnings.append(f"Dropped {dropped} row(s) with blank name or email.")

    df = df[
        ["SALESPERSON_NAME", "SALESPERSON_EMAIL", "MANAGER_NAME", "MANAGER_EMAIL", "IS_ACTIVE"]
    ]

    return df, warnings


# -------------------------------------------------------------------
# Manual add flow (implemented as an upsert)
# -------------------------------------------------------------------

def _upsert_single_contact(conn, tenant_id: int,
                           name: str,
                           email: str,
                           manager_name: str | None,
                           manager_email: str | None,
                           is_active: bool):
    """
    Upsert a single salesperson into SALES_CONTACTS using MERGE.

    MATCH key:
    - TENANT_ID
    - UPPER(SALESPERSON_NAME)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            MERGE INTO SALES_CONTACTS AS tgt
            USING (
                SELECT
                    %s AS TENANT_ID,
                    %s AS SALESPERSON_NAME,
                    %s AS SALESPERSON_EMAIL,
                    %s AS MANAGER_NAME,
                    %s AS MANAGER_EMAIL,
                    %s AS IS_ACTIVE
            ) AS src
            ON  tgt.TENANT_ID = src.TENANT_ID
            AND UPPER(tgt.SALESPERSON_NAME) = UPPER(src.SALESPERSON_NAME)
            WHEN MATCHED THEN UPDATE SET
                tgt.SALESPERSON_EMAIL = src.SALESPERSON_EMAIL,
                tgt.MANAGER_NAME      = src.MANAGER_NAME,
                tgt.MANAGER_EMAIL     = src.MANAGER_EMAIL,
                tgt.IS_ACTIVE         = src.IS_ACTIVE,
                tgt.UPDATED_AT        = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                TENANT_ID,
                SALESPERSON_NAME,
                SALESPERSON_EMAIL,
                MANAGER_NAME,
                MANAGER_EMAIL,
                IS_ACTIVE
            ) VALUES (
                src.TENANT_ID,
                src.SALESPERSON_NAME,
                src.SALESPERSON_EMAIL,
                src.MANAGER_NAME,
                src.MANAGER_EMAIL,
                src.IS_ACTIVE
            )
            """,
            (
                tenant_id,
                name,
                email,
                manager_name,
                manager_email,
                is_active,
            ),
        )


def _render_manual_add_form(conn, tenant_id: int):
    """
    Render the 'Manual Add' form for a single salesperson contact.
    """
    st.markdown("### ➕ Add / Update Single Salesperson")

    with st.form("sales_contact_manual_add_form", clear_on_submit=True):
        name = st.text_input("Salesperson Name (as it appears in GAP_REPORT.SALESPERSON)")
        email = st.text_input("Salesperson Email")
        mgr_name = st.text_input("Manager Name (optional)", value="")
        mgr_email = st.text_input("Manager Email (optional)", value="")
        is_active = st.checkbox("Is Active", value=True)

        submitted = st.form_submit_button("Save Salesperson")

    if not submitted:
        return

    # Basic validation
    name_clean = name.strip()
    email_clean = email.strip()
    mgr_name_clean = mgr_name.strip()
    mgr_email_clean = mgr_email.strip()

    if not name_clean:
        st.error("Salesperson Name is required.")
        return
    if not email_clean:
        st.error("Salesperson Email is required.")
        return

    try:
        _upsert_single_contact(
            conn,
            tenant_id,
            name_clean,
            email_clean,
            mgr_name_clean or None,
            mgr_email_clean or None,
            is_active,
        )
        st.success(f"✅ Saved salesperson contact for: {name_clean}")
    except Exception as e:
        st.error(f"❌ Failed to save salesperson contact: {e}")


# -------------------------------------------------------------------
# Bulk upsert flow
# -------------------------------------------------------------------

def _apply_contacts_bulk_upsert(conn, tenant_id: int, df: pd.DataFrame):
    """
    Apply bulk upsert of sales contacts into SALES_CONTACTS using MERGE.

    Key:
    - TENANT_ID
    - UPPER(SALESPERSON_NAME)
    """
    if df.empty:
        st.warning("No valid rows to upsert.")
        return

    rows = df.to_dict(orient="records")

    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    MERGE INTO SALES_CONTACTS AS tgt
                    USING (
                        SELECT
                            %s AS TENANT_ID,
                            %s AS SALESPERSON_NAME,
                            %s AS SALESPERSON_EMAIL,
                            %s AS MANAGER_NAME,
                            %s AS MANAGER_EMAIL,
                            %s AS IS_ACTIVE
                    ) AS src
                    ON  tgt.TENANT_ID = src.TENANT_ID
                    AND UPPER(tgt.SALESPERSON_NAME) = UPPER(src.SALESPERSON_NAME)
                    WHEN MATCHED THEN UPDATE SET
                        tgt.SALESPERSON_EMAIL = src.SALESPERSON_EMAIL,
                        tgt.MANAGER_NAME      = src.MANAGER_NAME,
                        tgt.MANAGER_EMAIL     = src.MANAGER_EMAIL,
                        tgt.IS_ACTIVE         = src.IS_ACTIVE,
                        tgt.UPDATED_AT        = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        TENANT_ID,
                        SALESPERSON_NAME,
                        SALESPERSON_EMAIL,
                        MANAGER_NAME,
                        MANAGER_EMAIL,
                        IS_ACTIVE
                    ) VALUES (
                        src.TENANT_ID,
                        src.SALESPERSON_NAME,
                        src.SALESPERSON_EMAIL,
                        src.MANAGER_NAME,
                        src.MANAGER_EMAIL,
                        src.IS_ACTIVE
                    )
                    """,
                    (
                        tenant_id,
                        r["SALESPERSON_NAME"],
                        r["SALESPERSON_EMAIL"],
                        r["MANAGER_NAME"] or None,
                        r["MANAGER_EMAIL"] or None,
                        bool(r["IS_ACTIVE"]),
                    ),
                )
        st.success(f"✅ Upserted {len(rows)} salesperson contact(s).")
    except Exception as e:
        st.error(f"❌ Bulk upsert into SALES_CONTACTS failed: {e}")


def _render_bulk_upload_section(conn, tenant_id: int):
    """
    Render the bulk upload section:
    - Download template
    - Upload filled template
    - Preview + apply upsert
    """
    st.markdown("### 📥 Bulk Upload / Update Sales Contacts")

    # Template download
    tmpl_buf = _build_contacts_template_xlsx()
    st.download_button(
        "Download Sales Contacts Template",
        data=tmpl_buf,
        file_name="sales_contacts_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         width='stretch',
        key="sales_contacts_template_dl",
    )

    st.markdown("#### Upload Completed Template")

    with st.form("sales_contacts_bulk_upload_form", clear_on_submit=False):
        uploaded_file = st.file_uploader(
            "Upload sales_contacts_template.xlsx",
            type=["xlsx"],
            key="sales_contacts_upload",
        )
        submitted = st.form_submit_button("Preview Upload")

    if not submitted or uploaded_file is None:
        return

    df, warnings = _parse_contacts_upload(uploaded_file)
    if df.empty:
        st.warning("No valid rows found in uploaded file.")
        return

    # Show warnings + preview
    for w in warnings:
        st.warning(w)

    st.markdown("##### Preview (normalized)")
    st.dataframe(df,  width='stretch')

    # Confirm apply
    if st.button("Apply Upload (Insert/Update Contacts)", type="primary"):
        _apply_contacts_bulk_upsert(conn, tenant_id, df)


# -------------------------------------------------------------------
# Main page entry
# -------------------------------------------------------------------

def render():
    """
    Sales Contacts Admin main entry point.

    Layout width='stretch'
    ------
    - Top: explanation + current tenant contacts.
    - Bottom: two columns:
        left  -> manual add/update
        right -> bulk upload wizard
    """
    st.title("Sales Contacts Admin")

    conn, tenant_id = _get_tenant_conn_and_id()
    if conn is None or tenant_id is None:
        return

    st.markdown(
        """
        Manage the **salesperson email directory**.

        - `SALESPERSON_NAME` must match `GAP_REPORT.SALESPERSON`.
        - `SALESPERSON_EMAIL` will be used when sending individual gap emails.
        - `MANAGER_NAME` / `MANAGER_EMAIL` are optional and will drive manager
          summary emails later.
        - `IS_ACTIVE` controls whether a contact is included in email sends.
        """
    )

    # Current contacts table (tenant scoped)
    st.markdown("### 👥 Current Sales Contacts")
    contacts_df = _fetch_sales_contacts(conn, tenant_id)
    if contacts_df.empty:
        st.info("No contacts found. Use the forms below to add some.")
    else:
        st.dataframe(contacts_df,  width='stretch', height=300)

    st.markdown("---")

    # Two-column layout: manual add vs bulk upload
    col_left, col_right = st.columns(2)

    with col_left:
        _render_manual_add_form(conn, tenant_id)

    with col_right:
        _render_bulk_upload_section(conn, tenant_id)


if __name__ == "__main__":
    render()
