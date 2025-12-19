# ------------------------- GAP HISTORY EMAILER PAGE -------------------------
"""
Gap History Emailer (PDF per salesperson)

Page Overview
-------------
Sends the new Gap History / Streaks report as a PDF attachment to each
salesperson and CC's their manager (from SALES_CONTACTS).

What this replaces
------------------
This page replaces the legacy "Email Gap Report" HTML email workflow that
pulled from GAP_REPORT and rendered HTML tables.

Core behavior
-------------
- Pulls active streak rows from GAP_CURRENT_STREAKS (via fetch_current_streaks)
- Lets user filter by chain / supplier / salesperson + min_streak
- Preview:
    - Show selected salesperson rows in a dataframe
    - Download selected salesperson PDF
    - Download ALL PDFs as a ZIP (optional convenience)
- Send:
    - Send selected salesperson only
    - Send ALL salespeople in current result set
- Email:
    - TO  = SALESPERSON_EMAIL
    - CC  = MANAGER_EMAIL (if present)
    - Attachment = PDF built from streak rows for that salesperson

Hard rules
----------
- This page does NOT publish snapshots.
- This page does NOT call snapshot insert logic.
- This page assumes snapshots already exist for the current week.

Notes
-----
- Streamlit deprecated use_container_width=True.
  Use width="stretch" or width="content" instead.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from utils.gap_history_mailer import fetch_current_streaks, send_gap_history_pdfs
from utils.pdf_reports import build_gap_streaks_pdf

PAGE_TITLE = "Gap History Emailer (PDF)"
DEFAULT_SENDER_EMAIL = "randy@chainlinkanalytics.com"


# -----------------------------------------------------------------------------
# Session keys
# -----------------------------------------------------------------------------
DEFAULT_KEYS = {
    "ghm_filters_hash": None,
    "ghm_results": None,         # stores streaks_df + derived lists
    "ghm_selected_sp": None,     # persisted preview selection
}
for k, v in DEFAULT_KEYS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _filters_hash(chains: List[str], suppliers: List[str], salespeople: List[str], min_streak: int):
    """Stable, order-insensitive representation of current filter choices."""
    return (
        tuple(sorted(chains or [])),
        tuple(sorted(suppliers or [])),
        tuple(sorted(salespeople or [])),
        int(min_streak),
    )


def _safe_label(name: str) -> str:
    """Filesystem-safe label for filenames."""
    return str(name).replace("/", "-").replace("\\", "-").strip()


def _get_tenant_id() -> Optional[int]:
    """Try common session keys for tenant_id."""
    # preferred: you typically set tenant_id directly
    tid = st.session_state.get("tenant_id")
    if tid:
        try:
            return int(tid)
        except Exception:
            pass

    # fallback: tenant_config dict/object
    tenant_config = st.session_state.get("tenant_config")
    if isinstance(tenant_config, dict):
        tid = tenant_config.get("tenant_id") or tenant_config.get("TENANT_ID")
    else:
        tid = getattr(tenant_config, "tenant_id", None)

    if tid is None:
        return None
    try:
        return int(tid)
    except Exception:
        return None


def _get_tenant_name() -> str:
    """Tenant display name for PDF headers and email subjects."""
    tenant_config = st.session_state.get("tenant_config") or {}
    if isinstance(tenant_config, dict):
        return tenant_config.get("display_name") or tenant_config.get("tenant_name") or "Client"
    return getattr(tenant_config, "display_name", None) or getattr(tenant_config, "tenant_name", None) or "Client"


def _zip_pdfs(pdf_map: Dict[str, bytes], suffix: str = "gap_history") -> bytes:
    """Create a ZIP of {salesperson: pdf_bytes}."""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for label, pdf_bytes in pdf_map.items():
            safe = _safe_label(label)
            zf.writestr(f"{safe}_{suffix}.pdf", pdf_bytes)
    return bio.getvalue()


def _build_pdf_for_salesperson(streaks_df: pd.DataFrame, tenant_name: str, salesperson: str) -> bytes:
    """Build one PDF for a single salesperson from the streaks DF."""
    sp_df = streaks_df[streaks_df["SALESPERSON_NAME"] == salesperson].copy()

    # Defensive ordering / column subset for the PDF builder
    pdf_cols = [
        "SALESPERSON_NAME",
        "CHAIN_NAME",
        "STORE_NUMBER",
        "STORE_NAME",
        "SUPPLIER_NAME",
        "PRODUCT_NAME",
        "UPC",
        "STREAK_WEEKS",
        "FIRST_GAP_WEEK",
        "LAST_GAP_WEEK",
    ]
    sp_pdf_df = sp_df[[c for c in pdf_cols if c in sp_df.columns]].copy()
    return build_gap_streaks_pdf(sp_pdf_df, tenant_name=tenant_name)


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
def render():
    st.title(PAGE_TITLE)

    con = st.session_state.get("conn")
    if not con:
        st.error("No tenant Snowflake connection in session. Please log in via Chainlink Core.")
        return

    tenant_id = _get_tenant_id()
    if tenant_id is None:
        st.error("tenant_id not found in session_state. Email sending is disabled.")
        return

    tenant_name = _get_tenant_name()

    st.caption(
        "This sends the **Gap History (streaks)** PDF to each salesperson and CCs their manager. "
        "It does **not** publish snapshots."
    )

    # -------------------------------------------------------------------------
    # Filter form
    # -------------------------------------------------------------------------
    # We build filter dropdowns from current streaks so we don't depend on GAP_REPORT.
    # Pull a small baseline set first (min_streak=1), then filter in memory after submit.
    with st.spinner("Loading available streak dimensions…"):
        base_df = fetch_current_streaks(
            con=con,
            tenant_id=int(tenant_id),
            chains=None,
            suppliers=None,
            salespeople=None,
            min_streak=1,
        )

    if base_df.empty:
        st.info("No streak history found yet. Make sure weekly snapshots have been published.")
        return

    # Normalize date fields for display
    for col in ["SNAPSHOT_WEEK_START", "FIRST_GAP_WEEK", "LAST_GAP_WEEK"]:
        if col in base_df.columns:
            base_df[col] = pd.to_datetime(base_df[col], errors="coerce").dt.date

    chains_dim = sorted(base_df["CHAIN_NAME"].dropna().unique().tolist())
    suppliers_dim = sorted(base_df["SUPPLIER_NAME"].dropna().unique().tolist())
    salespeople_dim = sorted(base_df["SALESPERSON_NAME"].dropna().unique().tolist())
    max_streak = int(pd.to_numeric(base_df["STREAK_WEEKS"], errors="coerce").fillna(1).max())

    with st.form("ghm_filters_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        chains = c1.multiselect("Chains", options=chains_dim, placeholder="All")
        suppliers = c2.multiselect("Suppliers", options=suppliers_dim, placeholder="All")
        salespeople = c3.multiselect("Salespeople", options=salespeople_dim, placeholder="All")

        min_streak = st.slider(
            "Minimum streak length (weeks)",
            min_value=1,
            max_value=max_streak if max_streak >= 1 else 1,
            value=1,
            help="Only include items that have been gaps for at least this many consecutive weeks.",
        )

        submitted = st.form_submit_button("Generate PDFs / Email List")

    # -------------------------------------------------------------------------
    # Only recompute on submit + hash change
    # -------------------------------------------------------------------------
    run_needed = False
    if submitted:
        new_hash = _filters_hash(chains, suppliers, salespeople, min_streak)
        old_hash = st.session_state.get("ghm_filters_hash")
        if old_hash is None or new_hash != old_hash:
            st.session_state["ghm_filters_hash"] = new_hash
            run_needed = True

    if run_needed:
        with st.spinner("Filtering streaks…"):
            # We can either refetch with filters (SQL) or filter locally.
            # Local filtering is fast at your current scale; also avoids dynamic SQL edge cases.
            df = base_df.copy()

            if chains:
                df = df[df["CHAIN_NAME"].isin(chains)]
            if suppliers:
                df = df[df["SUPPLIER_NAME"].isin(suppliers)]
            if salespeople:
                df = df[df["SALESPERSON_NAME"].isin(salespeople)]

            df["STREAK_WEEKS"] = pd.to_numeric(df["STREAK_WEEKS"], errors="coerce").fillna(1).astype(int)
            df = df[df["STREAK_WEEKS"] >= int(min_streak)]

            if df.empty:
                st.session_state["ghm_results"] = None
            else:
                sp_list = sorted(df["SALESPERSON_NAME"].dropna().unique().tolist())
                first_sp = sp_list[0] if sp_list else None
                st.session_state["ghm_selected_sp"] = first_sp

                st.session_state["ghm_results"] = {
                    "streaks_df": df,
                    "salespeople": sp_list,
                    "min_streak": int(min_streak),
                    "chains": chains,
                    "suppliers": suppliers,
                }

    # -------------------------------------------------------------------------
    # Render results (cached in session_state)
    # -------------------------------------------------------------------------
    res = st.session_state.get("ghm_results")
    if not res:
        if submitted:
            st.warning("No streak rows matched your filters.")
        return

    streaks_df: pd.DataFrame = res["streaks_df"]
    sp_options: List[str] = res["salespeople"]

    st.markdown("---")
    st.subheader("Preview + Downloads")

    if not sp_options:
        st.warning("No salespeople found in the filtered streak result.")
        return

    default_sp = st.session_state.get("ghm_selected_sp") or sp_options[0]
    if default_sp not in sp_options:
        default_sp = sp_options[0]

    selected_sp = st.selectbox(
        "Preview salesperson",
        options=sp_options,
        index=sp_options.index(default_sp),
        key="ghm_preview_salesperson",
    )
    st.session_state["ghm_selected_sp"] = selected_sp

    sp_df = streaks_df[streaks_df["SALESPERSON_NAME"] == selected_sp].copy()
    sp_df = sp_df.sort_values(
        by=["STREAK_WEEKS", "CHAIN_NAME", "STORE_NUMBER", "PRODUCT_NAME"],
        ascending=[False, True, True, True],
    )

    st.dataframe(
        sp_df,
        width="stretch",
        hide_index=True,
    )
    st.write(f"{selected_sp}: {len(sp_df)} active streak row(s).")

    pdf_bytes = _build_pdf_for_salesperson(streaks_df, tenant_name=tenant_name, salesperson=selected_sp)
    safe_sp = _safe_label(selected_sp)

    dl1, dl2 = st.columns([1, 1])

    dl1.download_button(
        label=f"📄 Download PDF – {selected_sp}",
        data=pdf_bytes,
        file_name=f"{safe_sp}_gap_history.pdf",
        mime="application/pdf",
        width="stretch",
    )

    # Optional: download ALL PDFs in one zip
    all_pdf_map: Dict[str, bytes] = {}
    for sp in sp_options:
        all_pdf_map[sp] = _build_pdf_for_salesperson(streaks_df, tenant_name=tenant_name, salesperson=sp)

    zip_bytes = _zip_pdfs(all_pdf_map, suffix="gap_history")
    dl2.download_button(
        label=f"📦 Download ALL PDFs ({len(sp_options)}) as ZIP",
        data=zip_bytes,
        file_name=f"gap_history_pdfs_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
        mime="application/zip",
        width="stretch",
    )

    # -------------------------------------------------------------------------
    # Sending emails
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Send Emails (PDF Attachment)")

    sender_email = (
        st.secrets.get("mail", {}).get("sender_email")
        if hasattr(st, "secrets")
        else None
    ) or DEFAULT_SENDER_EMAIL

    send_col1, send_col2, send_col3 = st.columns([1, 1, 0.7])

    if send_col1.button(f"Send {selected_sp} Only", type="primary", width="stretch"):
        with st.spinner(f"Sending Gap History PDF for {selected_sp}…"):
            summary = send_gap_history_pdfs(
                con=con,
                tenant_id=int(tenant_id),
                tenant_name=tenant_name,
                sender_email=sender_email,
                chains=res.get("chains"),
                suppliers=res.get("suppliers"),
                salespeople=None,       # we’re controlling with only_salespeople below
                min_streak=int(res.get("min_streak", 1)),
                only_salespeople=[selected_sp],
            )

        st.success(
            f"Sent: {summary.get('salesperson_success', 0)} | "
            f"Failed: {summary.get('salesperson_fail', 0)}"
        )
        skipped = summary.get("skipped_salespeople") or []
        if skipped:
            st.warning("Skipped (no contact match / missing email): " + ", ".join(map(str, skipped)))
        errs = summary.get("errors") or []
        if errs:
            st.error("Errors:")
            st.write(errs)

    if send_col2.button("Send ALL (filtered set)", type="secondary", width="stretch"):
        with st.spinner("Sending Gap History PDFs to all salespeople in this result set…"):
            summary = send_gap_history_pdfs(
                con=con,
                tenant_id=int(tenant_id),
                tenant_name=tenant_name,
                sender_email=sender_email,
                chains=res.get("chains"),
                suppliers=res.get("suppliers"),
                salespeople=None,
                min_streak=int(res.get("min_streak", 1)),
                only_salespeople=sp_options,  # enforce "filtered set"
            )

        st.success(
            f"Sent: {summary.get('salesperson_success', 0)} | "
            f"Failed: {summary.get('salesperson_fail', 0)}"
        )
        skipped = summary.get("skipped_salespeople") or []
        if skipped:
            st.warning("Skipped (no contact match / missing email): " + ", ".join(map(str, skipped)))
        errs = summary.get("errors") or []
        if errs:
            st.error("Errors:")
            st.write(errs)

    if send_col3.button("Clear", width="stretch"):
        st.session_state["ghm_results"] = None
        st.session_state["ghm_filters_hash"] = None
        st.session_state["ghm_selected_sp"] = None
        st.rerun()


if __name__ == "__main__":
    render()
