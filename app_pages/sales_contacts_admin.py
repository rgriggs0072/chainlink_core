# ---------------- app_pages/sales_contacts_admin.py ----------------
"""
Sales Contacts Admin Page

Overview for future devs
------------------------
Tenant admin UI to manage SALES_CONTACTS used by Email Gap Report and future auto-email flows.

Key decisions
-------------
- Tenant-scoped: always filter by TENANT_ID from st.session_state["tenant_id"].
- IDs (SALESPERSON_ID / MANAGER_ID) are optional for now (left NULL).
- Manual add + bulk upload behave as UPSERT via MERGE:
    - MATCH on (TENANT_ID, UPPER(SALESPERSON_NAME))
    - UPDATE emails/names/is_active + UPDATED_AT
    - INSERT when no match
- Bulk upload supports two formats:
    A) "Template" upload (sales_contacts_template.xlsx)
    B) "Delta raw" upload (inconsistent export with section headers)
  Both normalize into the same canonical DataFrame before upsert.

Notes
-----
- We do NOT truncate/delete table contents on upload. We upsert only.
  This avoids accidental loss when someone uploads a partial file.
- If you later want "replace mode", implement:
    - Option to deactivate contacts not present in the upload (safe cleanup)
"""

from __future__ import annotations

from io import BytesIO
import re
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st

from utils.sales_contacts import upsert_contact_by_name


# =============================================================================
# Constants + small utilities
# =============================================================================

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)



CANON_COLS = [
    "SALESPERSON_NAME",
    "SALESPERSON_EMAIL",
    "MANAGER_NAME",
    "MANAGER_EMAIL",
    "MANAGER_EMAIL_2",
    "EXTRA_CC_EMAIL",
    "IS_ACTIVE",
]


def _clean_str(x) -> str:
    """Normalize any Excel cell value to a safe string (never NaN)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def _normalize_active_flag(val) -> bool:
    """
    Convert string/numeric flags into boolean.

    Rules:
    - Blank/NaN -> True (default active)
    - Y/YES/TRUE/1 -> True
    - N/NO/FALSE/0 -> False
    - Anything else -> True
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


def _first_email_in_row(row_vals: List[str]) -> str:
    """Return the first email found in any cell of a row."""
    for v in row_vals:
        if not v:
            continue
        m = EMAIL_RE.search(v)
        if m:
            return m.group(0).strip()
    return ""


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase + underscore column names."""
    df = df.copy()
    df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]
    return df


def _ensure_optional_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all optional canonical columns exist, with safe defaults."""
    df = df.copy()

    for col in ["MANAGER_NAME", "MANAGER_EMAIL", "MANAGER_EMAIL_2", "EXTRA_CC_EMAIL"]:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").astype(str).str.strip()

    if "IS_ACTIVE" not in df.columns:
        df["IS_ACTIVE"] = True
    else:
        df["IS_ACTIVE"] = df["IS_ACTIVE"].apply(_normalize_active_flag)

    return df


def _assert_no_method_objects(df: pd.DataFrame) -> None:
    """
    Guardrail: prevent writing bound method strings to Snowflake.

    This catches the classic bug:
      name.strip().upper    (missing parentheses)
    """
    if df.empty or "SALESPERSON_NAME" not in df.columns:
        return

    s = df["SALESPERSON_NAME"].astype(str)
    bad = s.str.contains("built-in method upper", case=False, na=False) | s.str.contains(
        "method upper", case=False, na=False
    )
    if bad.any():
        examples = df.loc[bad, "SALESPERSON_NAME"].head(5).tolist()
        raise ValueError(
            "SALESPERSON_NAME contains method objects (missing .upper()). "
            f"Examples: {examples}"
        )


def _finalize_canonical_df(df: pd.DataFrame, warnings: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Final canonical cleanup:
    - Trim name/email
    - Force SALESPERSON_NAME uppercase (to match SALES_REPORT / GAP_REPORT expectation)
    - Drop blanks
    - De-dupe by SALESPERSON_EMAIL (keep first)
    - Return canonical column order
    """
    df = df.copy()

    df["SALESPERSON_NAME"] = (
        df["SALESPERSON_NAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    df["SALESPERSON_EMAIL"] = df["SALESPERSON_EMAIL"].fillna("").astype(str).str.strip()

    before = len(df)
    df = df[(df["SALESPERSON_NAME"] != "") & (df["SALESPERSON_EMAIL"] != "")]
    dropped = before - len(df)
    if dropped > 0:
        warnings.append(f"Dropped {dropped} row(s) with blank name or email.")

    before = len(df)
    df = df.drop_duplicates(subset=["SALESPERSON_EMAIL"], keep="first").reset_index(drop=True)
    deduped = before - len(df)
    if deduped > 0:
        warnings.append(f"Removed {deduped} duplicate email row(s) (kept first).")

    df = _ensure_optional_cols(df)

    _assert_no_method_objects(df)
    return df[CANON_COLS].reset_index(drop=True), warnings


# =============================================================================
# Core helpers: tenant connection + current data
# =============================================================================

def _get_tenant_conn_and_id():
    """Fetch tenant Snowflake connection and TENANT_ID from session state."""
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if conn is None:
        st.error("❌ No tenant Snowflake connection found in session.")
        return None, None
    if tenant_id is None:
        st.error("❌ TENANT_ID missing from session. Cannot scope SALES_CONTACTS.")
        return None, None

    return conn, int(tenant_id)


def _fetch_sales_contacts(conn, tenant_id: int) -> pd.DataFrame:
    """Load all sales contacts for the current tenant."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                SALESPERSON_ID,
                SALESPERSON_NAME,
                SALESPERSON_EMAIL,
                MANAGER_NAME,
                MANAGER_EMAIL,
                MANAGER_EMAIL_2,
                EXTRA_CC_EMAIL,
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
    return pd.DataFrame(rows, columns=cols)


# =============================================================================
# Template builder (download)
# =============================================================================

def _build_contacts_template_df() -> pd.DataFrame:
    """Canonical Sales Contacts template (one row per salesperson)."""
    return pd.DataFrame(
        [
            {
                "SALESPERSON_NAME": "JANE DOE",
                "SALESPERSON_EMAIL": "jane.doe@example.com",
                "MANAGER_NAME": "Mike Ramirez",
                "MANAGER_EMAIL": "mike.ramirez@deltapacificbev.com",
                "MANAGER_EMAIL_2": "alex.velazquez@deltapacificbev.com",
                "EXTRA_CC_EMAIL": "",
                "IS_ACTIVE": "Y",
            }
        ]
    )


def _build_contacts_template_xlsx() -> BytesIO:
    """Build an in-memory .xlsx template file for bulk upload."""
    df = _build_contacts_template_df()
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf


# =============================================================================
# Parsers
# =============================================================================

def _parse_contacts_template_upload(uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
    """Parse a completed template upload into canonical columns."""
    warnings: List[str] = []
    df_raw = pd.read_excel(uploaded_file, engine="openpyxl")
    df = _normalize_columns(df_raw)

    required = {"SALESPERSON_NAME", "SALESPERSON_EMAIL"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Uploaded file is missing required columns: " + ", ".join(sorted(missing)))

    df = _ensure_optional_cols(df)
    df, warnings = _finalize_canonical_df(df, warnings)
    return df, warnings



# =============================================================================
# Write logic (upsert + commit + proof)
# =============================================================================

def _apply_contacts_bulk_upsert(conn, tenant_id: int, df: pd.DataFrame) -> Tuple[int, int]:
    """
    Upsert all rows in df, then commit.

    Returns:
        (count_before, count_after) for tenant_id
    """
    if df.empty:
        return 0, 0

    _assert_no_method_objects(df)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM SALES_CONTACTS WHERE TENANT_ID = %s", (tenant_id,))
        count_before = int(cur.fetchone()[0])

    # Upsert using helper (name is already uppercased in finalize, but keep it safe)
    for r in df.itertuples(index=False):
        salesperson_name = str(r.SALESPERSON_NAME).strip().upper()  # ✅ parentheses
        salesperson_email = str(r.SALESPERSON_EMAIL).strip()

        upsert_contact_by_name(
            conn,
            tenant_id=tenant_id,
            salesperson_name=salesperson_name,
            salesperson_email=salesperson_email,
            manager_name=(str(getattr(r, "MANAGER_NAME", "") or "").strip() or None),
            manager_email=(str(getattr(r, "MANAGER_EMAIL", "") or "").strip() or None),
            manager_email_2=(str(getattr(r, "MANAGER_EMAIL_2", "") or "").strip() or None),
            extra_cc_email=(str(getattr(r, "EXTRA_CC_EMAIL", "") or "").strip() or None),
            is_active=bool(getattr(r, "IS_ACTIVE", True)),
        )

    # Commit once
    try:
        conn.commit()
    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM SALES_CONTACTS WHERE TENANT_ID = %s", (tenant_id,))
        count_after = int(cur.fetchone()[0])

    return count_before, count_after


# =============================================================================
# UI sections
# =============================================================================

def _render_manual_add_form(conn, tenant_id: int):
    """Manual add/update a single salesperson contact."""
    st.markdown("### ➕ Add / Update Single Salesperson")

    with st.form("sales_contact_manual_add_form", clear_on_submit=True):
        name = st.text_input("Salesperson Name (must match GAP_REPORT.SALESPERSON)")
        email = st.text_input("Salesperson Email")

        mgr_name = st.text_input("Manager Name (optional)", value="")
        mgr_email = st.text_input("Manager Email (optional)", value="")
        mgr_email_2 = st.text_input("Manager Email 2 (optional)", value="")
        extra_cc = st.text_input("Extra CC Email (optional)", value="")

        is_active = st.checkbox("Is Active", value=True)
        submitted = st.form_submit_button("Save Salesperson")

    if not submitted:
        return

    name_clean = name.strip().upper()  # ✅ parentheses
    email_clean = email.strip()

    if not name_clean:
        st.error("Salesperson Name is required.")
        return
    if not email_clean:
        st.error("Salesperson Email is required.")
        return

    try:
        upsert_contact_by_name(
            conn,
            tenant_id=tenant_id,
            salesperson_name=name_clean,
            salesperson_email=email_clean,
            manager_name=(mgr_name.strip() or None),
            manager_email=(mgr_email.strip() or None),
            manager_email_2=(mgr_email_2.strip() or None),
            extra_cc_email=(extra_cc.strip() or None),
            is_active=is_active,
        )

        try:
            conn.commit()
        except Exception:
            pass

        st.success(f"✅ Saved salesperson contact for: {name_clean}")
        st.rerun()

    except Exception as e:
        st.error(f"❌ Failed to save salesperson contact: {e}")


def _render_bulk_upload_section(conn, tenant_id: int):
    """Bulk upload (template only) -> preview -> apply upsert."""
    st.markdown("### 📥 Bulk Upload / Update Sales Contacts")

    tmpl_buf = _build_contacts_template_xlsx()
    st.download_button(
        "Download Sales Contacts Template",
        data=tmpl_buf,
        file_name="sales_contacts_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="sales_contacts_template_dl",
    )

    st.markdown("#### Upload Template File")

    with st.form("sales_contacts_bulk_upload_form", clear_on_submit=False):
        uploaded_file = st.file_uploader("Upload .xlsx", type=["xlsx"], key="sales_contacts_upload")
        submitted = st.form_submit_button("Preview Upload")

    if submitted and uploaded_file is not None:
        try:
            df, warnings = _parse_contacts_template_upload(uploaded_file)  # ✅ template only
        except Exception as e:
            st.error(f"❌ Preview failed: {e}")
            return

        if df.empty:
            st.warning("No valid rows found in uploaded file.")
            return

        st.session_state["sales_contacts_preview_df"] = df
        st.session_state["sales_contacts_preview_warnings"] = warnings

    df_preview = st.session_state.get("sales_contacts_preview_df")
    warnings = st.session_state.get("sales_contacts_preview_warnings", [])

    if df_preview is None or df_preview.empty:
        return

    for w in warnings:
        st.warning(w)

    st.markdown("##### Preview (normalized)")
    st.dataframe(df_preview, use_container_width=True)

    if st.button("Apply Upload (Insert/Update Contacts)", type="primary"):
        try:
            count_before, count_after = _apply_contacts_bulk_upsert(conn, tenant_id, df_preview)
        except Exception as e:
            st.error(f"❌ Apply failed: {e}")
            return

        st.success(f"✅ Applied {len(df_preview)} row(s). Count before={count_before}, after={count_after}")

        st.session_state.pop("sales_contacts_preview_df", None)
        st.session_state.pop("sales_contacts_preview_warnings", None)

        st.rerun()



# =============================================================================
# Main entry
# =============================================================================

def render():
    """Main page entry."""
    st.title("Sales Contacts Admin")

    conn, tenant_id = _get_tenant_conn_and_id()
    if conn is None or tenant_id is None:
        return

    st.markdown(
        """
Manage the **salesperson email directory** used by gap emails.

- `SALESPERSON_NAME` must match `GAP_REPORT.SALESPERSON`.
- `SALESPERSON_EMAIL` is the TO recipient.
- `MANAGER_EMAIL`, `MANAGER_EMAIL_2`, and `EXTRA_CC_EMAIL` are optional CC recipients.
- `IS_ACTIVE` controls whether a contact is included in email sends.
        """.strip()
    )

    st.markdown("### 👥 Current Sales Contacts")
    try:
        contacts_df = _fetch_sales_contacts(conn, tenant_id)
    except Exception as e:
        st.error(f"❌ Failed to load SALES_CONTACTS: {e}")
        return

    if contacts_df.empty:
        st.info("No contacts found. Use the forms below to add some.")
    else:
        st.dataframe(contacts_df, use_container_width=True, height=320)

    st.markdown("---")

    col_left, col_right = st.columns(2)

    with col_left:
        _render_manual_add_form(conn, tenant_id)

    with col_right:
        _render_bulk_upload_section(conn, tenant_id)


if __name__ == "__main__":
    render()
