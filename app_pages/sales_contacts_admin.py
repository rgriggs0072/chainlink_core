# ---------------- app_pages/sales_contacts_admin.py ----------------
# -*- coding: utf-8 -*-
"""
Sales Contacts Admin Page

Page Overview
-------------
Tenant admin UI to manage SALES_CONTACTS and support salesperson reassignment used by Email Gap Report.

UX Goals
--------
- No giant table by default.
- Dropdown-driven edit/add for a single salesperson.
- Bulk upload with preview + apply (upsert only, safe).
- Reassign workflow:
    1) Optional: create/update "new salesperson" contact fields
    2) Preview impact on operational tables
    3) Apply reassignment across operational tables ONLY
    4) Deactivate old salesperson contact (old stays for history but won't receive emails)

Important design rule
---------------------
Reassignment must NOT rename the old salesperson row in SALES_CONTACTS.
It should deactivate old and ensure new is active.

Notes
-----
- Uses st.rerun() (NOT deprecated st.experimental_rerun()).
"""

from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

import pandas as pd
import streamlit as st

from utils.sales_contacts import (
    fetch_sales_contacts,
    lookup_contact_by_salesperson_name,
    lookup_contact_by_salesperson_email,
    upsert_contact_by_name,
    deactivate_contact_by_name,
    preview_salesperson_reassignment,
    apply_salesperson_reassignment,
)

# =============================================================================
# Constants
# =============================================================================

CANON_COLS = [
    "SALESPERSON_NAME",
    "SALESPERSON_EMAIL",
    "MANAGER_NAME",
    "MANAGER_EMAIL",
    "MANAGER_EMAIL_2",
    "EXTRA_CC_EMAIL",
    "IS_ACTIVE",
]

BULK_PREVIEW_KEY = "sc_bulk_preview_df"
BULK_WARNINGS_KEY = "sc_bulk_preview_warnings"
BULK_FINGERPRINT_KEY = "sc_bulk_preview_fingerprint"


# =============================================================================
# Session helpers
# =============================================================================

def _get_tenant_conn_and_id():
    """
    Get tenant Snowflake connection and tenant_id from session_state.

    This is UI code, so we surface errors here instead of raising.
    """
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if conn is None:
        st.error("❌ No tenant Snowflake connection found in session.")
        return None, None
    if tenant_id is None:
        st.error("❌ tenant_id missing from session.")
        return None, None

    return conn, int(tenant_id)


# =============================================================================
# Normalizers & validators
# =============================================================================

def _normalize_active_flag(val) -> bool:
    """
    Convert string/numeric flags into boolean.

    Rules:
    - Blank/NaN -> True
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


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase + underscore column names."""
    df = df.copy()
    df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]
    return df


def _ensure_optional_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure optional canonical columns exist and are clean strings.
    """
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


def _finalize_canonical_df(df: pd.DataFrame, warnings: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Final cleanup:
    - Uppercase salesperson name
    - Drop blank name/email
    - De-dupe by SALESPERSON_NAME (keep first)
    """
    df = df.copy()

    df["SALESPERSON_NAME"] = df["SALESPERSON_NAME"].fillna("").astype(str).str.strip().str.upper()
    df["SALESPERSON_EMAIL"] = df["SALESPERSON_EMAIL"].fillna("").astype(str).str.strip()

    before = len(df)
    df = df[(df["SALESPERSON_NAME"] != "") & (df["SALESPERSON_EMAIL"] != "")]
    dropped = before - len(df)
    if dropped > 0:
        warnings.append(f"Dropped {dropped} row(s) with blank name or email.")

    before = len(df)
    df = df.drop_duplicates(subset=["SALESPERSON_NAME"], keep="first").reset_index(drop=True)
    deduped = before - len(df)
    if deduped > 0:
        warnings.append(f"Removed {deduped} duplicate name row(s) (kept first).")

    df = _ensure_optional_cols(df)
    return df[CANON_COLS].reset_index(drop=True), warnings


def _clean_field_or_none(s: str) -> str | None:
    """
    For optional fields: return None if blank after stripping.
    """
    v = (s or "").strip()
    return v if v else None


# =============================================================================
# Template download / parsing
# =============================================================================

def _build_contacts_template_df() -> pd.DataFrame:
    """Template dataframe for Sales Contacts."""
    return pd.DataFrame(
        [
            {
                "SALESPERSON_NAME": "JANE DOE",
                "SALESPERSON_EMAIL": "jane.doe@example.com",
                "MANAGER_NAME": "Mike Ramirez",
                "MANAGER_EMAIL": "mike.ramirez@company.com",
                "MANAGER_EMAIL_2": "",
                "EXTRA_CC_EMAIL": "",
                "IS_ACTIVE": "Y",
            }
        ]
    )


def _build_contacts_template_xlsx() -> BytesIO:
    """Build in-memory xlsx template."""
    df = _build_contacts_template_df()
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf


def _parse_contacts_template_upload(uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
    """Parse uploaded template into canonical df."""
    warnings: List[str] = []
    df_raw = pd.read_excel(uploaded_file, engine="openpyxl")
    df = _normalize_columns(df_raw)

    required = {"SALESPERSON_NAME", "SALESPERSON_EMAIL"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(sorted(missing)))

    df = _ensure_optional_cols(df)
    df, warnings = _finalize_canonical_df(df, warnings)
    return df, warnings


# =============================================================================
# Data access for UI
# =============================================================================

def _load_contacts_df(conn, tenant_id: int, *, active_only: bool = False) -> pd.DataFrame:
    """Load contacts via utils helper."""
    return fetch_sales_contacts(conn, tenant_id=tenant_id, active_only=active_only)


def _contact_names(contacts_df: pd.DataFrame, *, active_only: bool = False) -> List[str]:
    """Get sorted list of salesperson names."""
    if contacts_df is None or contacts_df.empty:
        return []
    df = contacts_df.copy()
    if active_only and "IS_ACTIVE" in df.columns:
        df = df[df["IS_ACTIVE"] == True]  # noqa: E712

    return (
        df["SALESPERSON_NAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .sort_values()
        .unique()
        .tolist()
    )


# =============================================================================
# UI sections
# =============================================================================

def _render_contacts_audit_expander(contacts_df: pd.DataFrame):
    """Optional expander to view current contacts."""
    with st.expander("View current contacts (optional)", expanded=False):
        if contacts_df is None or contacts_df.empty:
            st.info("No contacts found.")
            return

        cols = [c for c in contacts_df.columns if c not in {"TENANT_ID"}]
        st.dataframe(contacts_df[cols], use_container_width=True, height=320)


def _render_manage_single_contact(conn, tenant_id: int, contacts_df: pd.DataFrame):
    """
    Dropdown-driven Add/Edit/Deactivate.

    Behavior:
    - Select existing -> fields prefill
    - Select "Add new…" -> blank form
    - Save uses upsert_contact_by_name()
    - Deactivate sets IS_ACTIVE = FALSE for that salesperson
    """
    st.subheader("Manage a salesperson")

    options = ["➕ Add new…"] + _contact_names(contacts_df, active_only=False)
    selected = st.selectbox("Select salesperson", options=options, index=0, key="sc_selected_name")

    existing = None
    if selected != "➕ Add new…":
        existing = lookup_contact_by_salesperson_name(
            conn, salesperson_name=selected, tenant_id=tenant_id, active_only=False
        )

    def _v(field: str, default: str = "") -> str:
        return str((existing or {}).get(field, default) or default)

    def _vb(field: str, default: bool = True) -> bool:
        v = (existing or {}).get(field, default)
        return bool(v) if v is not None else bool(default)

    with st.form("sc_single_contact_form", clear_on_submit=False):
        name = st.text_input("Salesperson Name (matches operational tables)", value=_v("SALESPERSON_NAME", ""))
        email = st.text_input("Salesperson Email", value=_v("SALESPERSON_EMAIL", ""))

        mgr_name = st.text_input("Manager Name (optional)", value=_v("MANAGER_NAME", ""))
        mgr_email = st.text_input("Manager Email (optional)", value=_v("MANAGER_EMAIL", ""))
        mgr_email_2 = st.text_input("Manager Email 2 (optional)", value=_v("MANAGER_EMAIL_2", ""))
        extra_cc = st.text_input("Extra CC Email (optional)", value=_v("EXTRA_CC_EMAIL", ""))

        is_active = st.checkbox("Active", value=_vb("IS_ACTIVE", True))

        col_a, col_b = st.columns(2)
        with col_a:
            do_save = st.form_submit_button("Save", type="primary")
        with col_b:
            do_deactivate = st.form_submit_button("Deactivate", disabled=(existing is None))

    if do_save:
        nm = (name or "").strip().upper()
        em = (email or "").strip()

        if not nm:
            st.error("Salesperson Name is required.")
            return
        if not em:
            st.error("Salesperson Email is required.")
            return

        # block duplicate email assigned to another salesperson
        other = lookup_contact_by_salesperson_email(conn, salesperson_email=em, tenant_id=tenant_id, active_only=False)
        if other and str(other.get("SALESPERSON_NAME", "")).strip().upper() != nm:
            st.error(f"That email is already used by: {other.get('SALESPERSON_NAME')}")
            return

        try:
            upsert_contact_by_name(
                conn,
                tenant_id=tenant_id,
                salesperson_name=nm,
                salesperson_email=em,
                manager_name=_clean_field_or_none(mgr_name),
                manager_email=_clean_field_or_none(mgr_email),
                manager_email_2=_clean_field_or_none(mgr_email_2),
                extra_cc_email=_clean_field_or_none(extra_cc),
                is_active=is_active,
            )
            try:
                conn.commit()
            except Exception:
                pass

            st.success(f"✅ Saved: {nm}")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Save failed: {e}")
            return

    if do_deactivate and existing is not None:
        try:
            deactivate_contact_by_name(conn, tenant_id=tenant_id, salesperson_name=str(existing["SALESPERSON_NAME"]))
            try:
                conn.commit()
            except Exception:
                pass

            st.success(f"✅ Deactivated: {existing['SALESPERSON_NAME']}")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Deactivate failed: {e}")
            return


def _render_bulk_upload(conn, tenant_id: int):
    """
    Bulk upload (template-only) with preview then apply via upsert.

    Notes:
    - Preview persists in session_state until cleared or applied.
    - Fingerprint prevents accidentally applying an old preview for a different file.
    """
    st.subheader("Bulk upload / update (template)")

    st.download_button(
        "Download template",
        data=_build_contacts_template_xlsx(),
        file_name="sales_contacts_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="sc_template_dl",
    )

    with st.form("sc_bulk_form"):
        uploaded = st.file_uploader("Upload completed template (.xlsx)", type=["xlsx"], key="sc_bulk_upload")
        do_preview = st.form_submit_button("Preview upload")

    # Clear preview if requested
    if st.button("Clear preview", key="sc_clear_bulk_preview"):
        st.session_state.pop(BULK_PREVIEW_KEY, None)
        st.session_state.pop(BULK_WARNINGS_KEY, None)
        st.session_state.pop(BULK_FINGERPRINT_KEY, None)
        st.rerun()

    if do_preview:
        if uploaded is None:
            st.error("Upload a template file first.")
            return

        try:
            df_preview, warnings = _parse_contacts_template_upload(uploaded)
        except Exception as e:
            st.error(f"❌ Preview failed: {e}")
            return

        if df_preview.empty:
            st.warning("No valid rows found.")
            return

        fingerprint = f"{getattr(uploaded, 'name', '')}|{len(df_preview)}"
        st.session_state[BULK_PREVIEW_KEY] = df_preview
        st.session_state[BULK_WARNINGS_KEY] = warnings
        st.session_state[BULK_FINGERPRINT_KEY] = fingerprint

    df_preview = st.session_state.get(BULK_PREVIEW_KEY)
    warnings = st.session_state.get(BULK_WARNINGS_KEY, [])
    fingerprint = st.session_state.get(BULK_FINGERPRINT_KEY)

    if df_preview is None or df_preview.empty:
        return

    for w in warnings:
        st.warning(w)

    st.markdown("Preview (normalized)")
    st.dataframe(df_preview, use_container_width=True)

    # Require user confirmation to apply
    confirm = st.text_input("Type APPLY to confirm bulk upsert", value="", key="sc_bulk_apply_confirm")
    apply_disabled = confirm.strip().upper() != "APPLY"

    if st.button("Apply upload (upsert)", type="primary", key="sc_apply_bulk", disabled=apply_disabled):
        # basic safety: ensure preview is present + consistent
        if not fingerprint:
            st.error("Preview session expired. Re-run preview first.")
            return

        try:
            for r in df_preview.itertuples(index=False):
                upsert_contact_by_name(
                    conn,
                    tenant_id=tenant_id,
                    salesperson_name=str(r.SALESPERSON_NAME).strip().upper(),
                    salesperson_email=str(r.SALESPERSON_EMAIL).strip(),
                    manager_name=_clean_field_or_none(getattr(r, "MANAGER_NAME", "")),
                    manager_email=_clean_field_or_none(getattr(r, "MANAGER_EMAIL", "")),
                    manager_email_2=_clean_field_or_none(getattr(r, "MANAGER_EMAIL_2", "")),
                    extra_cc_email=_clean_field_or_none(getattr(r, "EXTRA_CC_EMAIL", "")),
                    is_active=bool(getattr(r, "IS_ACTIVE", True)),
                )

            try:
                conn.commit()
            except Exception:
                pass

            st.success(f"✅ Upserted {len(df_preview)} row(s).")
            st.session_state.pop(BULK_PREVIEW_KEY, None)
            st.session_state.pop(BULK_WARNINGS_KEY, None)
            st.session_state.pop(BULK_FINGERPRINT_KEY, None)
            st.rerun()

        except Exception as e:
            st.error(f"❌ Apply failed: {e}")
            return


def _render_reassignment(conn, tenant_id: int, contacts_df: pd.DataFrame):
    """
    Reassign salesperson labels across operational tables (Bob -> Bill).

    Critical:
    - We DO NOT rename the old SALES_CONTACTS record.
    - After reassignment, we deactivate old salesperson contact.
    - New contact updates are optional, and we do NOT overwrite fields with blanks.
    """
    st.subheader("Reassign salesperson (Bob → Bill)")

    names = _contact_names(contacts_df, active_only=False)
    if not names:
        st.info("Add contacts first.")
        return

    with st.form("sc_reassign_form"):
        col1, col2 = st.columns(2)
        with col1:
            old_name = st.selectbox("Old salesperson", options=names, key="sc_old_name")
        with col2:
            new_name = st.selectbox("New salesperson", options=names, key="sc_new_name")

        st.markdown("#### New salesperson contact update (optional)")
        update_new_contact = st.checkbox("Update the new salesperson contact fields", value=False)

        new_email = st.text_input("New salesperson email (required if updating)", value="")
        new_mgr_name = st.text_input("New manager name (optional)", value="")
        new_mgr_email = st.text_input("New manager email (optional)", value="")
        new_mgr_email_2 = st.text_input("New manager email 2 (optional)", value="")
        new_extra_cc = st.text_input("New extra CC email (optional)", value="")

        do_preview = st.form_submit_button("Preview impact")

    if not do_preview:
        return

    old_norm = (old_name or "").strip().upper()
    new_norm = (new_name or "").strip().upper()

    if not old_norm or not new_norm:
        st.error("Both old and new salesperson are required.")
        return
    if old_norm == new_norm:
        st.error("Old and new are the same.")
        return

    # Preview counts
    try:
        counts = preview_salesperson_reassignment(conn, tenant_id=tenant_id, old_salesperson=old_norm)
    except Exception as e:
        st.error(f"❌ Preview failed: {e}")
        return

    preview_df = (
        pd.DataFrame([{"TABLE": k, "ROWS_TO_UPDATE": v} for k, v in counts.items()])
        .sort_values("ROWS_TO_UPDATE", ascending=False)
        .reset_index(drop=True)
    )

    st.markdown("Preview results")
    st.dataframe(preview_df, use_container_width=True)

    total = int(preview_df["ROWS_TO_UPDATE"].sum()) if not preview_df.empty else 0
    st.info(f"Total rows to update: {total}")

    st.markdown("#### Confirm & apply")
    confirm = st.text_input("Type REASSIGN to confirm", value="", key="sc_reassign_confirm")
    apply_disabled = confirm.strip().upper() != "REASSIGN"

    if st.button("Apply reassignment", type="primary", disabled=apply_disabled, key="sc_apply_reassign"):
        try:
            # Optional: update new contact fields (ONLY if explicitly requested)
            if update_new_contact:
                if not new_email.strip():
                    raise ValueError("New salesperson email is required when updating contact fields.")

                # Prevent duplicate email assigned to a different salesperson
                other = lookup_contact_by_salesperson_email(
                    conn, salesperson_email=new_email.strip(), tenant_id=tenant_id, active_only=False
                )
                if other and str(other.get("SALESPERSON_NAME", "")).strip().upper() != new_norm:
                    raise ValueError(f"That email is already used by: {other.get('SALESPERSON_NAME')}")

                upsert_contact_by_name(
                    conn,
                    tenant_id=tenant_id,
                    salesperson_name=new_norm,
                    salesperson_email=new_email.strip(),
                    manager_name=_clean_field_or_none(new_mgr_name),
                    manager_email=_clean_field_or_none(new_mgr_email),
                    manager_email_2=_clean_field_or_none(new_mgr_email_2),
                    extra_cc_email=_clean_field_or_none(new_extra_cc),
                    is_active=True,
                )

            # Apply operational table updates ONLY
            updated = apply_salesperson_reassignment(
                conn,
                tenant_id=tenant_id,
                old_salesperson=old_norm,
                new_salesperson=new_norm,
                update_sales_contacts=False,  # ✅ CRITICAL
            )

            # Deactivate old salesperson contact (keep record for history)
            deactivate_contact_by_name(conn, tenant_id=tenant_id, salesperson_name=old_norm)

            try:
                conn.commit()
            except Exception:
                pass

            updated_df = (
                pd.DataFrame([{"TABLE": k, "ROWS_UPDATED": v} for k, v in updated.items()])
                .sort_values("ROWS_UPDATED", ascending=False)
                .reset_index(drop=True)
            )

            st.success("✅ Reassignment applied. Old contact deactivated.")
            st.dataframe(updated_df, use_container_width=True)
            st.rerun()

        except Exception as e:
            st.error(f"❌ Apply failed: {e}")
            return


# =============================================================================
# Main entry
# =============================================================================

def render():
    """Main page entry."""
    st.title("Sales Contacts Admin")

    conn, tenant_id = _get_tenant_conn_and_id()
    if conn is None or tenant_id is None:
        return

    st.caption(
        "Maintain the email directory used by gap emails, and reassign salesperson labels across operational tables."
    )

    # Load contacts once per run
    try:
        contacts_df = _load_contacts_df(conn, tenant_id, active_only=False)
    except Exception as e:
        st.error(f"❌ Failed to load SALES_CONTACTS: {e}")
        return

    # Optional audit view (hidden by default)
    _render_contacts_audit_expander(contacts_df)

    st.markdown("---")
    _render_manage_single_contact(conn, tenant_id, contacts_df)

    st.markdown("---")
    _render_bulk_upload(conn, tenant_id)

    st.markdown("---")
    _render_reassignment(conn, tenant_id, contacts_df)


if __name__ == "__main__":
    render()
