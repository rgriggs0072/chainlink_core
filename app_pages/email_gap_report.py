# ------------------------- EMAIL_GAP_REPORT.PY ------------------------------

"""
Gap History Emailer (PDF per salesperson)

Page Overview
-------------
Sends the Gap History / Streaks report as a PDF attachment to each salesperson
and CCs their manager (from SALES_CONTACTS).

Hard rules
----------
- Does NOT publish snapshots
- Does NOT insert snapshot data
- Assumes snapshots already exist
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from utils.gap_history_mailer import (
    fetch_current_streaks,
    send_gap_history_pdfs,
)
from utils.pdf_reports import build_gap_streaks_pdf, GAP_HISTORY_PDF_COLUMNS


# -----------------------------------------------------------------------------
# Page constants
# -----------------------------------------------------------------------------
PAGE_TITLE = "Gap History Emailer (PDF)"
DEFAULT_SENDER_EMAIL = "randy@chainlinkanalytics.com"

DATE_COLUMNS = [
    "SNAPSHOT_WEEK_START",
    "FIRST_GAP_WEEK",
    "LAST_GAP_WEEK",
]

PDF_COLUMNS = [
    "CHAIN_NAME",
    "STORE_NUMBER",
    "STORE_NAME",
    "ADDRESS",
    "SUPPLIER_NAME",
    "PRODUCT_NAME",
    "UPC",
    "STREAK_WEEKS",
    "FIRST_GAP_WEEK",
    "LAST_GAP_WEEK",
]


# -----------------------------------------------------------------------------
# Session state initialization
# -----------------------------------------------------------------------------
SESSION_DEFAULTS = {
    "ghm_filters_hash": None,
    "ghm_results": None,
    "ghm_selected_sp": None,
}
for key, value in SESSION_DEFAULTS.items():
    st.session_state.setdefault(key, value)


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def _filters_hash(
    chains: List[str],
    suppliers: List[str],
    salespeople: List[str],
    min_streak: int,
) -> tuple:
    """Stable, order-insensitive filter signature."""
    return (
        tuple(sorted(chains or [])),
        tuple(sorted(suppliers or [])),
        tuple(sorted(salespeople or [])),
        int(min_streak),
    )


def _safe_label(text: str) -> str:
    """Filesystem-safe label."""
    return str(text).replace("/", "-").replace("\\", "-").strip()


def _get_tenant_id() -> Optional[int]:
    """Resolve tenant_id from session_state."""
    tid = st.session_state.get("tenant_id")
    if tid:
        try:
            return int(tid)
        except Exception:
            pass

    tenant_cfg = st.session_state.get("tenant_config")
    if isinstance(tenant_cfg, dict):
        tid = tenant_cfg.get("TENANT_ID") or tenant_cfg.get("tenant_id")
    else:
        tid = getattr(tenant_cfg, "tenant_id", None)

    try:
        return int(tid) if tid is not None else None
    except Exception:
        return None


def _get_tenant_name() -> str:
    """Tenant display name for PDFs and email subject."""
    cfg = st.session_state.get("tenant_config") or {}
    if isinstance(cfg, dict):
        return cfg.get("display_name") or cfg.get("tenant_name") or "Client"
    return getattr(cfg, "display_name", None) or getattr(cfg, "tenant_name", None) or "Client"


def _zip_pdfs(pdf_map: Dict[str, bytes], suffix: str) -> bytes:
    """Bundle PDFs into a ZIP."""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for label, pdf_bytes in pdf_map.items():
            zf.writestr(f"{_safe_label(label)}_{suffix}.pdf", pdf_bytes)
    return bio.getvalue()


def _build_pdf(
    streaks_df: pd.DataFrame,
    tenant_name: str,
    salesperson: str,
) -> bytes:
    """Build a single salesperson PDF (header includes salesperson)."""
    df = streaks_df[streaks_df["SALESPERSON_NAME"] == salesperson].copy()
    df = df[[c for c in PDF_COLUMNS if c in df.columns]]

    return build_gap_streaks_pdf(
        df,
        tenant_name=tenant_name,
        salesperson_name=salesperson,  
    )



# -----------------------------------------------------------------------------
# Page render
# -----------------------------------------------------------------------------
def render() -> None:
    st.title(PAGE_TITLE)

    conn = st.session_state.get("conn")
    if not conn:
        st.error("No tenant Snowflake connection found.")
        return

    tenant_id = _get_tenant_id()
    if tenant_id is None:
        st.error("tenant_id missing from session. Emailing disabled.")
        return

    tenant_name = _get_tenant_name()

    st.caption(
        "Sends the **Gap History (streaks)** PDF to each salesperson "
        "and CCs their manager. Snapshots are NOT modified."
    )

    # -------------------------------------------------------------------------
    # Load baseline data
    # -------------------------------------------------------------------------
    with st.spinner("Loading streak history…"):
        base_df = fetch_current_streaks(
            con=conn,
            tenant_id=tenant_id,
            chains=None,
            suppliers=None,
            salespeople=None,
            min_streak=1,
        )

    if base_df.empty:
        st.info("No streak history found.")
        return

    for col in DATE_COLUMNS:
        if col in base_df.columns:
            base_df[col] = pd.to_datetime(base_df[col], errors="coerce").dt.date

    chains_dim = sorted(base_df["CHAIN_NAME"].dropna().unique())
    suppliers_dim = sorted(base_df["SUPPLIER_NAME"].dropna().unique())
    salespeople_dim = sorted(base_df["SALESPERSON_NAME"].dropna().unique())
    max_streak = int(pd.to_numeric(base_df["STREAK_WEEKS"], errors="coerce").fillna(1).max())

    # -------------------------------------------------------------------------
    # Filters
    # -------------------------------------------------------------------------
    with st.form("ghm_filters", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        chains = c1.multiselect("Chains", chains_dim, placeholder="All")
        suppliers = c2.multiselect("Suppliers", suppliers_dim, placeholder="All")
        salespeople = c3.multiselect("Salespeople", salespeople_dim, placeholder="All")

        min_streak = st.slider(
            "Minimum streak (weeks)",
            min_value=1,
            max_value=max_streak,
            value=1,
        )

        submitted = st.form_submit_button("Generate PDFs / Email List")

    # -------------------------------------------------------------------------
    # Filter execution
    # -------------------------------------------------------------------------
    if submitted:
        new_hash = _filters_hash(chains, suppliers, salespeople, min_streak)
        if new_hash != st.session_state["ghm_filters_hash"]:
            st.session_state["ghm_filters_hash"] = new_hash

            df = base_df.copy()
            if chains:
                df = df[df["CHAIN_NAME"].isin(chains)]
            if suppliers:
                df = df[df["SUPPLIER_NAME"].isin(suppliers)]
            if salespeople:
                df = df[df["SALESPERSON_NAME"].isin(salespeople)]

            df["STREAK_WEEKS"] = pd.to_numeric(df["STREAK_WEEKS"], errors="coerce").fillna(1).astype(int)
            df = df[df["STREAK_WEEKS"] >= min_streak]

            if df.empty:
                st.session_state["ghm_results"] = None
            else:
                sp_list = sorted(df["SALESPERSON_NAME"].unique())
                st.session_state["ghm_selected_sp"] = sp_list[0]
                st.session_state["ghm_results"] = {
                    "df": df,
                    "salespeople": sp_list,
                    "chains": chains,
                    "suppliers": suppliers,
                    "min_streak": min_streak,
                }

    # -------------------------------------------------------------------------
    # Render results
    # -------------------------------------------------------------------------
    res = st.session_state.get("ghm_results")
    if not res:
        return

    df = res["df"]
    sp_list = res["salespeople"]

    st.markdown("---")
    st.subheader("Preview + Downloads")

    selected_sp = st.selectbox(
        "Preview salesperson",
        sp_list,
        index=sp_list.index(st.session_state["ghm_selected_sp"]),
    )
    st.session_state["ghm_selected_sp"] = selected_sp

    sp_df = df[df["SALESPERSON_NAME"] == selected_sp].sort_values(
        ["STREAK_WEEKS", "CHAIN_NAME", "STORE_NUMBER"],
        ascending=[False, True, True],
    )

    st.dataframe(sp_df, width="stretch", hide_index=True)
    st.write(f"{selected_sp}: {len(sp_df)} active gaps")

    pdf_bytes = _build_pdf(df, tenant_name, selected_sp)

    c1, c2 = st.columns(2)
    c1.download_button(
        "📄 Download PDF",
        pdf_bytes,
        file_name=f"{_safe_label(selected_sp)}_gap_history.pdf",
        mime="application/pdf",
        width="stretch",
    )

    all_pdfs = {sp: _build_pdf(df, tenant_name, sp) for sp in sp_list}
    c2.download_button(
        "📦 Download ALL PDFs",
        _zip_pdfs(all_pdfs, "gap_history"),
        file_name=f"gap_history_{datetime.now():%Y%m%d_%H%M}.zip",
        mime="application/zip",
        width="stretch",
    )

    # -------------------------------------------------------------------------
    # Send emails
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Send Emails")

    sender_email = (
        st.secrets.get("mail", {}).get("sender_email", DEFAULT_SENDER_EMAIL)
        if hasattr(st, "secrets")
        else DEFAULT_SENDER_EMAIL
    )

    s1, s2, s3 = st.columns([1, 1, 0.7])

    if s1.button(f"Send {selected_sp} Only", type="primary", width="stretch"):
        summary = send_gap_history_pdfs(
            con=conn,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            sender_email=sender_email,
            chains=res["chains"],
            suppliers=res["suppliers"],
            salespeople=None,
            min_streak=res["min_streak"],
            only_salespeople=[selected_sp],
        )
        st.success(f"Sent: {summary['salesperson_success']} | Failed: {summary['salesperson_fail']}")

    if s2.button("Send ALL", type="secondary", width="stretch"):
        summary = send_gap_history_pdfs(
            con=conn,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            sender_email=sender_email,
            chains=res["chains"],
            suppliers=res["suppliers"],
            salespeople=None,
            min_streak=res["min_streak"],
            only_salespeople=sp_list,
        )
        st.success(f"Sent: {summary['salesperson_success']} | Failed: {summary['salesperson_fail']}")

    if s3.button("Clear", width="stretch"):
        for k in SESSION_DEFAULTS:
            st.session_state[k] = None
        st.rerun()


def build_gap_history_email_body(
    salesperson: str,
    stats: dict,
) -> str:
    return f"""
    <p><strong>Gap History – Weekly Execution Focus</strong></p>
    <p>Hello {salesperson},</p>

    <p>
    Attached is your <strong>Gap History Report</strong>, showing current
    gaps and how long they have persisted.
    </p>

    <ul>
      <li><strong>Active Gaps:</strong> {stats["active"]}</li>
      <li><strong>New This Week:</strong> {stats["new"]}</li>
      <li><strong>2–3 Week Gaps:</strong> {stats["mid"]}</li>
      <li><strong>4+ Week Gaps:</strong> {stats["long"]}</li>
    </ul>

    <p>
    Please prioritize older gaps first and reach out to your manager if
    support is needed.
    </p>

    <p>
    Best regards,<br>
    <strong>Chainlink Analytics</strong>
    </p>

    <hr>
    <p style="font-size:12px;color:#666;">
    Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}
    </p>
    """



if __name__ == "__main__":
    render()
