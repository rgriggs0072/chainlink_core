# ------------------------- EMAIL GAP REPORT PAGE -------------------------

"""
Email Gap Report (per-salesperson, email-ready)

Page Overview
-------------
Generates “Gap Push” emails that summarize execution by salesperson and list
only true gaps (rows with numeric sr_upc IS NULL). Filters are wrapped in a
submit-only form to prevent full-page reruns while editing. Results are cached
in session_state and only recomputed when submitted filters change.

Key logic
---------
IN SCHEMATIC = count(In_Schematic = 1)
FULFILLED    = count(PURCHASED_YES_NO in {1,'1','YES','Y','TRUE'}) across ALL rows
GAPS         = IN SCHEMATIC - FULFILLED
% EXECUTION  = FULFILLED / IN SCHEMATIC
Detail table = only rows where sr_upc IS NULL

New manager summary logic
-------------------------
- SALES_CONTACTS is used to map SALESPERSON_NAME -> MANAGER_NAME / MANAGER_EMAIL.
- For each manager, we generate a summary table of all their salespeople with:
    SALESMAN, IN_SCHEMATIC, FULFILLED, GAPS, TARGET_AT_90, % EXECUTION, GAPS_AWAY_90
- Manager summaries are stored in session_state["egp_results"]["manager_html_by_manager"].

Phase 2 additions
-----------------
- Uses utils.email_gap_utils.send_all_gap_emails to:
    * Send one email per salesperson (their HTML report).
    * Send one combined email per manager (all of their salespeople).
- SALESPERSON column is removed from the detail table; name is only in the header.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from typing import Dict, List, Tuple, Iterable

import pandas as pd
import streamlit as st

from utils.email_gap_utils import send_all_gap_emails

PAGE_TITLE = "Email Gap Report"
TARGET_EXECUTION = 0.90  # 90% goal


# -------------------------------
# Session keys for this page
# -------------------------------
DEFAULT_KEYS = {
    "egp_filters": None,
    "egp_results": None,
    "egp_filters_hash": None,
    "egp_selected_sp": None,  # NEW: persist the selected preview salesperson
}
for k, v in DEFAULT_KEYS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def _build_in_clause(column: str, values: Iterable[str]) -> Tuple[str, List[str]]:
    """Return ("column IN (%s,...)", params) or ("", []) for optional filters."""
    vals = [v for v in values if v]
    if not vals:
        return "", []
    placeholders = ", ".join(["%s"] * len(vals))
    return f"{column} IN ({placeholders})", vals


def _filters_hash(chains: List[str], suppliers: List[str], salespeople: List[str]):
    """Stable, order-insensitive representation of current filter choices."""
    return (
        tuple(sorted(chains or [])),
        tuple(sorted(suppliers or [])),
        tuple(sorted(salespeople or [])),
    )


def _safe_label(name: str) -> str:
    """Filesystem-safe label for filenames."""
    return str(name).replace("/", "-").replace("\\", "-").strip()


def build_zip(html_map: Dict[str, str], suffix: str) -> bytes:
    """
    Bundle {key: html} as a ZIP.

    Parameters
    ----------
    html_map:
        Dictionary of {label: html_string}.
    suffix:
        String suffix for filenames, e.g. 'gap_email' or 'manager_summary'.
    """
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for label, html in html_map.items():
            safe = _safe_label(label)
            zf.writestr(f"{safe}_{suffix}.html", html)
    return bio.getvalue()


# -------------------------------------------------------
# Data access
# -------------------------------------------------------
@st.cache_data(ttl=300)
def load_dimensions(_con) -> Dict[str, List[str]]:
    """Pull distinct lists for dropdowns (cached 5 min)."""
    dims: Dict[str, List[str]] = {}
    with _con.cursor() as cur:
        cur.execute("SELECT DISTINCT CHAIN_NAME FROM GAP_REPORT ORDER BY 1;")
        dims["chains"] = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT SUPPLIER FROM GAP_REPORT ORDER BY 1;")
        dims["suppliers"] = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT SALESPERSON FROM GAP_REPORT ORDER BY 1;")
        dims["salespeople"] = [r[0] for r in cur.fetchall()]
    return dims


def fetch_gap_df(
    con,
    chains: List[str],
    suppliers: List[str],
    salespeople: List[str],
    only_gaps: bool = False,
) -> pd.DataFrame:
    """
    Fetch GAP_REPORT rows with optional filters.

    Parameters
    ----------
    con:
        Active Snowflake connection (tenant-scoped).
    chains, suppliers, salespeople:
        Optional filter lists; empty means "no filter" on that dimension.
    only_gaps:
        If True, append condition "sr_upc IS NULL" to only return true gaps.

    Returns
    -------
    pandas.DataFrame
        GAP_REPORT rows with the selected filter logic applied.
    """
    where_parts, params = [], []

    for col, vals in [
        ("CHAIN_NAME", chains),
        ("SUPPLIER", suppliers),
        ("SALESPERSON", salespeople),
    ]:
        clause, p = _build_in_clause(col, vals)
        if clause:
            where_parts.append(clause)
            params.extend(p)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    gap_cond = ""
    if only_gaps:
        gap_cond = f"""{"AND" if where_sql else "WHERE"} "sr_upc" IS NULL"""

    sql = f"""
        SELECT
          RESET_DATES,
          RESET_TIME,
          CHAIN_NAME,
          STORE_NAME,
          STORE_NUMBER,
          ADDRESS,
          CITY,
          COUNTY,
          SUPPLIER,
          PRODUCT_NAME,
          SALESPERSON,
          "dg_upc",
          "sr_upc",
          "In_Schematic",
          PURCHASED_YES_NO,
          "sc_STATUS"
        FROM GAP_REPORT
        {where_sql}
        {gap_cond}
    """

    with con.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    return pd.DataFrame(rows, columns=cols)


def load_sales_contacts(con, tenant_id: int) -> pd.DataFrame:
    """
    Load active sales contacts for the current tenant.

    Returns columns:
        SALESPERSON_NAME, SALESPERSON_EMAIL, MANAGER_NAME, MANAGER_EMAIL

    Notes:
    - Name matching is done case-insensitive (uppercased) against GAP_REPORT.SALESPERSON.
    - If there are multiple rows for the same salesperson, the first is used.
    """
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT
                SALESPERSON_NAME,
                SALESPERSON_EMAIL,
                MANAGER_NAME,
                MANAGER_EMAIL
            FROM SALES_CONTACTS
            WHERE TENANT_ID = %s
              AND IS_ACTIVE = TRUE
            """,
            (tenant_id,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows, columns=cols)
    df["SALESPERSON_NAME_UPPER"] = df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    return df


# -------------------------------------------------------
# Metrics
# -------------------------------------------------------
def compute_salesperson_metrics(df_sp: pd.DataFrame) -> dict:
    """
    Compute summary metrics for one salesperson.

    Rules:
    - IN_SCHEMATIC = count(In_Schematic == 1)
    - FULFILLED    = count(PURCHASED_YES_NO in {1, '1', 'YES', 'Y', 'TRUE'})
    """
    in_schem_col = pd.to_numeric(df_sp["In_Schematic"], errors="coerce").fillna(0)
    in_schematic = int((in_schem_col == 1).sum())

    py_col = df_sp["PURCHASED_YES_NO"].fillna("")

    s_str = py_col.astype(str).str.strip().str.upper()
    str_yes = s_str.isin({"YES", "Y", "TRUE", "1"})

    s_num = pd.to_numeric(py_col, errors="coerce")
    num_yes = (s_num == 1)

    fulfilled = int((str_yes | num_yes).sum())

    gaps = max(0, in_schematic - fulfilled)
    execution = (fulfilled / in_schematic) if in_schematic > 0 else 0.0
    target_at_90 = TARGET_EXECUTION * in_schematic
    gaps_away_90 = max(0.0, target_at_90 - fulfilled)

    return {
        "in_schematic": in_schematic,
        "fulfilled": fulfilled,
        "gaps": gaps,
        "execution_pct": execution,
        "target_at_90": target_at_90,
        "gaps_away_90": gaps_away_90,
    }


# -------------------------------------------------------
# HTML rendering: per-salesperson email
# -------------------------------------------------------
def render_email_html(salesperson: str, metrics: dict, gaps_df: pd.DataFrame) -> str:
    """
    Generate inline-CSS HTML email body listing only gap rows for a salesperson.
    """
    style = """
    <style>
      .wrap { font-family: Arial, Helvetica, sans-serif; color: #111; }
      .title { font-size: 18px; font-weight: 700; margin: 0 0 12px; }
      .summary { border-collapse: collapse; margin: 8px 0 14px; width: 100%; max-width: 720px; }
      .summary th, .summary td { border: 1px solid #ddd; padding: 6px 8px; text-align: center; font-size: 13px; }
      .summary th { background: #f4f6f8; font-weight: 700; }
      .para { font-size: 14px; margin: 10px 0 16px; }
      .gaps { border-collapse: collapse; width: 100%; max-width: 1000px; }
      .gaps th, .gaps td { border: 1px solid #ddd; padding: 6px 8px; font-size: 12px; }
      .gaps th { background: #f4f6f8; text-align: left; }
      .muted { color: #666; font-size: 12px; margin-top: 14px; }
    </style>
    """
    m = metrics

    exec_pct_val = float(m.get("execution_pct", 0.0)) * 100.0
    if exec_pct_val < 80.0:
        exec_style = ' style="background-color:#fdecea; color:#b00020; font-weight:700;"'
    elif 80.0 <= exec_pct_val < 85.0:
        exec_style = ' style="background-color:#fff3e0; color:#e65100; font-weight:700;"'
    elif 85.0 <= exec_pct_val < 90.0:
        exec_style = ' style="background-color:#fffde7; color:#8c6d1f; font-weight:700;"'
    else:
        exec_style = ' style="background-color:#e8f5e9; color:#1b5e20; font-weight:700;"'

    summary_html = f"""
    <table class="summary">
      <tr>
        <th>SALESMAN</th><th>IN SCHEMATIC</th><th>FULFILLED</th><th>GAPS</th>
        <th>PLACEMENT NEEDED FOR 90%</th><th>% EXECUTION</th><th>GAPS AWAY FROM 90%</th>
      </tr>
      <tr>
        <td>{salesperson}</td>
        <td>{m['in_schematic']}</td>
        <td>{m['fulfilled']}</td>
        <td>{m['gaps']}</td>
        <td>{m['target_at_90']:.1f}</td>
        <td{exec_style}>{int(round(exec_pct_val, 0))}%</td>
        <td>{m['gaps_away_90']:.1f}</td>
      </tr>
    </table>
    """

    ordered_cols = [
        "RESET_DATES",
        "RESET_TIME",
        "CHAIN_NAME",
        "STORE_NAME",
        "STORE_NUMBER",
        "ADDRESS",
        "CITY",
        "COUNTY",
        "SUPPLIER",
        "PRODUCT_NAME",
        "dg_upc",
        "sr_upc",
        "PURCHASED_YES_NO",
    ]

    display_names = {
        "RESET_DATES": "Reset Date",
        "RESET_TIME": "Reset Time",
        "CHAIN_NAME": "Chain",
        "STORE_NAME": "Store",
        "STORE_NUMBER": "Store #",
        "ADDRESS": "Address",
        "CITY": "City",
        "COUNTY": "County",
        "SUPPLIER": "Supplier",
        "PRODUCT_NAME": "Product",
        "dg_upc": "Distribution UPC",
        "sr_upc": "Sales Report UPC",
        "PURCHASED_YES_NO": "Purchased?",
    }

    show_cols = [c for c in ordered_cols if c in gaps_df.columns]
    remaining = gaps_df[show_cols].copy()

    head_html = "".join(f"<th>{display_names.get(col, col)}</th>" for col in remaining.columns)
    body_rows = []
    for _, row in remaining.iterrows():
        tds = "".join(f"<td>{'' if pd.isna(v) else v}</td>" for v in row.values)
        body_rows.append(f"<tr>{tds}</tr>")

    detail_html = f"""
    <table class="gaps">
      <thead><tr>{head_html}</tr></thead>
      <tbody>{"".join(body_rows)}</tbody>
    </table>
    """

    body = f"""
    <div class="wrap">
      <div class="title">Weekly Execution Focus</div>
      {summary_html}
      <div class="para">
        We need to get to 90% execution here. Please focus on filling the gaps with products in stock.
        Reach out to your manager if you need support. Thank you!
      </div>
      {detail_html}
      <div class="muted">Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
    </div>
    """
    return f"<!doctype html><html><head>{style}</head><body>{body}</body></html>"


# -------------------------------------------------------
# HTML rendering: manager summary
# -------------------------------------------------------
def build_manager_summaries(
    metrics_by_salesperson: Dict[str, dict],
    contacts_df: pd.DataFrame,
) -> tuple[Dict[str, str], List[dict]]:
    """Build manager-level summary HTML, grouped by MANAGER_NAME."""
    if contacts_df.empty or not metrics_by_salesperson:
        return {}, []

    contacts_df = contacts_df.copy()
    contacts_df["SALESPERSON_NAME_UPPER"] = contacts_df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()

    contact_lookup = (
        contacts_df.drop_duplicates(subset=["SALESPERSON_NAME_UPPER"])
        .set_index("SALESPERSON_NAME_UPPER")
    )

    manager_rows: Dict[str, List[tuple[str, dict]]] = {}
    for sp_name, metrics in metrics_by_salesperson.items():
        sp_key = str(sp_name).strip().upper()
        if sp_key not in contact_lookup.index:
            continue
        row = contact_lookup.loc[sp_key]
        manager_name = row.get("MANAGER_NAME") or "Unknown Manager"
        manager_rows.setdefault(manager_name, []).append((sp_name, metrics))

    manager_html_by_manager: Dict[str, str] = {}
    manager_contacts: List[dict] = []

    table_style = """
    <style>
      .wrap { font-family: Arial, Helvetica, sans-serif; color: #111; }
      .title { font-size: 18px; font-weight: 700; margin: 0 0 12px; }
      .summary { border-collapse: collapse; margin: 8px 0 14px; width: 100%; max-width: 720px; }
      .summary th, .summary td { border: 1px solid #ddd; padding: 6px 8px; text-align: center; font-size: 13px; }
      .summary th { background: #f4f6f8; font-weight: 700; }
      .muted { color: #666; font-size: 12px; margin-top: 14px; }
    </style>
    """

    for manager_name, rows in manager_rows.items():
        subset = contacts_df[
            contacts_df["MANAGER_NAME"].astype(str).str.strip() == str(manager_name).strip()
        ]
        manager_email = subset["MANAGER_EMAIL"].iloc[0] if not subset.empty else None

        manager_contacts.append({"manager_name": manager_name, "manager_email": manager_email})

        body_rows = []
        for sp_name, m in rows:
            exec_pct_val = float(m.get("execution_pct", 0.0)) * 100.0
            if exec_pct_val < 80.0:
                exec_style = ' style="background-color:#fdecea; color:#b00020; font-weight:700;"'
            elif 80.0 <= exec_pct_val < 85.0:
                exec_style = ' style="background-color:#fff3e0; color:#e65100; font-weight:700;"'
            elif 85.0 <= exec_pct_val < 90.0:
                exec_style = ' style="background-color:#fffde7; color:#8c6d1f; font-weight:700;"'
            else:
                exec_style = ' style="background-color:#e8f5e9; color:#1b5e20; font-weight:700;"'

            body_rows.append(
                f"""
                <tr>
                  <td>{sp_name}</td>
                  <td>{m['in_schematic']}</td>
                  <td>{m['fulfilled']}</td>
                  <td>{m['gaps']}</td>
                  <td>{m['target_at_90']:.1f}</td>
                  <td{exec_style}>{int(round(exec_pct_val, 0))}%</td>
                  <td>{m['gaps_away_90']:.1f}</td>
                </tr>
                """
            )

        summary_html = f"""
        <table class="summary">
          <tr>
            <th>SALESMAN</th>
            <th>IN SCHEMATIC</th>
            <th>FULFILLED</th>
            <th>GAPS</th>
            <th>PLACEMENT NEEDED FOR 90%</th>
            <th>% EXECUTION</th>
            <th>GAPS AWAY FROM 90%</th>
          </tr>
          {''.join(body_rows)}
        </table>
        """

        body = f"""
        <div class="wrap">
          <div class="title">Weekly Execution Summary – {manager_name}</div>
          {summary_html}
          <div class="muted">Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
        </div>
        """

        manager_html_by_manager[manager_name] = (
            f"<!doctype html><html><head>{table_style}</head><body>{body}</body></html>"
        )

    return manager_html_by_manager, manager_contacts


# -------------------------------------------------------
# Page
# -------------------------------------------------------
def render():
    """
    Main page entry.
    - Wrap filters in a form (no reruns while editing)
    - Only compute when submitted filters actually change
    - Persist results in session_state to avoid recompute on minor UI events
    - Adds:
        * preview selector
        * send selected-only button
        * send all + managers button
    """
    st.title(PAGE_TITLE)

    con = st.session_state.get("conn")
    if not con:
        st.error("No tenant Snowflake connection in session. Please log in via Chainlink Core.")
        return

    dims = load_dimensions(con)

    # -------- Filter form (no reruns while interacting) --------
    with st.form("egp_filters_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        chains = c1.multiselect("Chains", dims["chains"], placeholder="All", key="egp_chains")
        suppliers = c2.multiselect("Suppliers", dims["suppliers"], placeholder="All", key="egp_suppliers")
        salespeople = c3.multiselect("Salespeople", dims["salespeople"], placeholder="All", key="egp_salespeople")
        submitted = st.form_submit_button("Generate Emails")

    # -------- Execute once per submit when filters changed --------
    run_needed = False
    if submitted:
        new_hash = _filters_hash(chains, suppliers, salespeople)
        old_hash = st.session_state.get("egp_filters_hash")
        if old_hash is None or new_hash != old_hash:
            st.session_state["egp_filters_hash"] = new_hash
            st.session_state["egp_filters"] = {"chains": chains, "suppliers": suppliers, "salespeople": salespeople}
            run_needed = True

    if run_needed:
        with st.spinner("Building emails…"):
            df_all = fetch_gap_df(con, chains, suppliers, salespeople, only_gaps=False)
            df_gaps = fetch_gap_df(con, chains, suppliers, salespeople, only_gaps=True)

            if df_all.empty:
                st.session_state["egp_results"] = None
                st.warning("No rows matched your filters.")
            else:
                html_by_salesperson: Dict[str, str] = {}
                metrics_by_salesperson: Dict[str, dict] = {}

                for sp in sorted(df_all["SALESPERSON"].dropna().unique()):
                    sp_all = df_all[df_all["SALESPERSON"] == sp]
                    sp_gaps = df_gaps[df_gaps["SALESPERSON"] == sp]
                    metrics = compute_salesperson_metrics(sp_all)
                    metrics_by_salesperson[sp] = metrics
                    html_by_salesperson[sp] = render_email_html(sp, metrics, sp_gaps)

                # Default selection = first generated
                first_sp = next(iter(html_by_salesperson))
                st.session_state["egp_selected_sp"] = first_sp

                tenant_id = st.session_state.get("tenant_id")
                manager_html_by_manager = {}
                manager_contacts: List[dict] = []
                first_manager = None

                if tenant_id:
                    contacts_df = load_sales_contacts(con, tenant_id)
                    manager_html_by_manager, manager_contacts = build_manager_summaries(
                        metrics_by_salesperson, contacts_df
                    )
                    if manager_html_by_manager:
                        first_manager = next(iter(manager_html_by_manager.keys()))

                st.session_state["egp_results"] = {
                    "html_by_salesperson": html_by_salesperson,
                    "first_sp": first_sp,
                    "manager_html_by_manager": manager_html_by_manager,
                    "first_manager": first_manager,
                    "manager_contacts": manager_contacts,
                }

    # -------- Render results from state (no recompute) --------
    res = st.session_state.get("egp_results")
    if not res:
        return

    html_by_salesperson = res["html_by_salesperson"]
    manager_html_by_manager = res.get("manager_html_by_manager") or {}
    first_manager = res.get("first_manager")

    # ---- Preview selector ----
    st.markdown("### Salesperson Email Preview")

    sp_options = sorted(list(html_by_salesperson.keys()))
    default_sp = st.session_state.get("egp_selected_sp") or sp_options[0]
    if default_sp not in sp_options:
        default_sp = sp_options[0]

    selected_sp = st.selectbox(
        "Preview salesperson",
        options=sp_options,
        index=sp_options.index(default_sp),
        key="egp_preview_salesperson",
    )
    st.session_state["egp_selected_sp"] = selected_sp

    with st.expander(f"Preview: {selected_sp}", expanded=True):
        st.components.v1.html(html_by_salesperson[selected_sp], height=600, scrolling=True)

    left, right, clear_col = st.columns([1, 1, 0.5])

    safe_name = _safe_label(selected_sp)
    left.download_button(
        label=f"Download {selected_sp} HTML",
        file_name=f"{safe_name}_gap_email.html",
        data=html_by_salesperson[selected_sp],
        mime="text/html",
        width="stretch",
    )

    zip_bytes = build_zip(html_by_salesperson, suffix="gap_email")
    right.download_button(
        label=f"Download All ({len(html_by_salesperson)}) as ZIP",
        file_name=f"gap_push_emails_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
        data=zip_bytes,
        mime="application/zip",
        width="stretch",
    )

    # -------- Send emails --------
    st.markdown("---")
    st.markdown("### Send Gap Report Emails")

    tenant_id_for_send = (
        st.session_state.get("tenant_id")
        or st.session_state.get("tenant_config", {}).get("TENANT_ID")
    )

    if not tenant_id_for_send:
        st.info(
            "Tenant ID not found in session; email sending is disabled. "
            "Ensure tenant_id or tenant_config is set on login."
        )
    else:
        sender_email = "randy@chainlinkanalytics.com"  # or pull from secrets

        send_col1, send_col2 = st.columns(2)

        # ✅ Send ONLY selected salesperson
        if send_col1.button(f"Send Email for {selected_sp} Only", type="primary"):
            with st.spinner(f"Sending {selected_sp}…"):
                summary = send_all_gap_emails(
                    conn=con,
                    tenant_id=int(tenant_id_for_send),
                    html_by_salesperson={selected_sp: html_by_salesperson[selected_sp]},
                    sender_email=sender_email,
                    manager_html_by_manager={},  # don't send manager summaries for single-send
                )

            st.success(
                f"Salespeople: {summary.get('salesperson_success', 0)} sent, "
                f"{summary.get('salesperson_fail', 0)} failed."
            )
            skipped = summary.get("skipped_salespeople") or []
            if skipped:
                st.warning("No SALES_CONTACTS match for (skipped): " + ", ".join(skipped))

        # ✅ Send ALL generated salespeople + managers
        if send_col2.button("Send ALL Salespeople & Managers", type="secondary"):
            with st.spinner("Sending all emails…"):
                summary = send_all_gap_emails(
                    conn=con,
                    tenant_id=int(tenant_id_for_send),
                    html_by_salesperson=html_by_salesperson,
                    sender_email=sender_email,
                    manager_html_by_manager=manager_html_by_manager,
                )

            st.success(
                f"Salespeople: {summary.get('salesperson_success', 0)} sent, "
                f"{summary.get('salesperson_fail', 0)} failed. "
                f"Managers: {summary.get('manager_success', 0)} sent, "
                f"{summary.get('manager_fail', 0)} failed."
            )
            skipped = summary.get("skipped_salespeople") or []
            if skipped:
                st.warning("No SALES_CONTACTS match for (skipped): " + ", ".join(skipped))

    # -------- Manager summaries (optional preview/download) --------
    if manager_html_by_manager and first_manager:
        st.markdown("---")
        st.markdown("### Manager Summary Preview")

        with st.expander(f"Preview: {first_manager}", expanded=False):
            st.components.v1.html(manager_html_by_manager[first_manager], height=400, scrolling=True)

        m_left, m_right = st.columns(2)
        safe_mgr = _safe_label(first_manager)
        m_left.download_button(
            label=f"Download {first_manager} Summary HTML",
            file_name=f"{safe_mgr}_manager_summary.html",
            data=manager_html_by_manager[first_manager],
            mime="text/html",
            width="stretch",
        )

        mgr_zip = build_zip(manager_html_by_manager, suffix="manager_summary")
        m_right.download_button(
            label=f"Download All Manager Summaries ({len(manager_html_by_manager)})",
            file_name=f"manager_summaries_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            data=mgr_zip,
            mime="application/zip",
            width="stretch",
        )

    # -------- Clear cached results --------
    if clear_col.button("Clear", width="stretch"):
        st.session_state["egp_results"] = None
        st.session_state["egp_selected_sp"] = None
        st.rerun()


if __name__ == "__main__":
    render()
