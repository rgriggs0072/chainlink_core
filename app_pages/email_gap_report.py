"""
Email Gap Report (per-salesperson, email-ready)

Generates “Gap Push” emails that summarize execution by salesperson and list
only true gaps (rows with numeric sr_upc IS NULL).

Key logic:
-----------
IN SCHEMATIC = count(In_Schematic = 1)
FULFILLED    = count(PURCHASED_YES_NO = 'Yes') across ALL rows
GAPS         = IN SCHEMATIC - FULFILLED
% EXECUTION  = FULFILLED / IN SCHEMATIC
Detail table = only rows where sr_upc IS NULL
"""

from __future__ import annotations
import io
import zipfile
from datetime import datetime
from typing import Dict, List, Tuple, Iterable

import pandas as pd
import streamlit as st

PAGE_TITLE = "Email Gap Report"
TARGET_EXECUTION = 0.90  # 90% goal


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


def fetch_gap_df(con, chains: List[str], suppliers: List[str], salespeople: List[str], only_gaps=False) -> pd.DataFrame:
    """Fetch GAP_REPORT rows with optional filters; only_gaps=True filters sr_upc IS NULL."""
    where_parts, params = [], []

    for col, vals in [("CHAIN_NAME", chains), ("SUPPLIER", suppliers), ("SALESPERSON", salespeople)]:
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
    # Normalize types
    in_schem_col = pd.to_numeric(df_sp["In_Schematic"], errors="coerce").fillna(0)
    in_schematic = int((in_schem_col == 1).sum())

    py_col = df_sp["PURCHASED_YES_NO"].fillna("")

    # string view
    s_str = py_col.astype(str).str.strip().str.upper()
    str_yes = s_str.isin({"YES", "Y", "TRUE", "1"})

    # numeric view
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
# HTML rendering
# -------------------------------------------------------
def render_email_html(salesperson: str, metrics: dict, gaps_df: pd.DataFrame) -> str:
    """Generate inline-CSS HTML email body listing only gap rows."""
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
        <td>{int(round(m['execution_pct'] * 100, 0))}%</td>
        <td>{m['gaps_away_90']:.1f}</td>
      </tr>
    </table>
    """

    ordered_cols = [
        "RESET_DATES", "RESET_TIME", "CHAIN_NAME", "STORE_NAME", "STORE_NUMBER",
        "ADDRESS", "CITY", "COUNTY", "SUPPLIER", "PRODUCT_NAME", "SALESPERSON",
        "dg_upc", "sr_upc", "In_Schematic", "PURCHASED_YES_NO", "sc_STATUS"
    ]
    show_cols = [c for c in ordered_cols if c in gaps_df.columns]
    remaining = gaps_df[show_cols].copy()

    head_html = "".join(f"<th>{col}</th>" for col in remaining.columns)
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
        Reach out to Mike or Cristian if you need support. Thank you!
      </div>
      {detail_html}
      <div class="muted">Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
    </div>
    """
    return f"<!doctype html><html><head>{style}</head><body>{body}</body></html>"


# -------------------------------------------------------
# Utility
# -------------------------------------------------------
def build_zip(html_map: Dict[str, str]) -> bytes:
    """Bundle {salesperson: html} as a ZIP."""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
        for sp, html in html_map.items():
            safe = sp.replace("/", "-").replace("\\", "-")
            zf.writestr(f"{safe}_gap_email.html", html)
    return bio.getvalue()


# -------------------------------------------------------
# Page
# -------------------------------------------------------
def render():
    """Main page entry."""
    st.title(PAGE_TITLE)

    con = st.session_state.get("conn")
    if not con:
        st.error("No tenant Snowflake connection in session. Please log in via Chainlink Core.")
        return

    dims = load_dimensions(con)

    c1, c2, c3 = st.columns(3)
    chains = c1.multiselect("Chains", dims["chains"], placeholder="All")
    suppliers = c2.multiselect("Suppliers", dims["suppliers"], placeholder="All")
    salespeople = c3.multiselect("Salespeople", dims["salespeople"], placeholder="All")

    if st.button("Generate Emails", type="primary"):
        # full data for metrics
        df_all = fetch_gap_df(con, chains, suppliers, salespeople, only_gaps=False)
        # filtered data for gap detail table
        df_gaps = fetch_gap_df(con, chains, suppliers, salespeople, only_gaps=True)

        if df_all.empty:
            st.warning("No rows matched your filters.")
            return

        html_by_salesperson: Dict[str, str] = {}
        for sp in sorted(df_all["SALESPERSON"].dropna().unique()):
            sp_all = df_all[df_all["SALESPERSON"] == sp]
            sp_gaps = df_gaps[df_gaps["SALESPERSON"] == sp]
            metrics = compute_salesperson_metrics(sp_all)
            html_by_salesperson[sp] = render_email_html(sp, metrics, sp_gaps)

        first_sp = next(iter(html_by_salesperson))
        with st.expander(f"Preview: {first_sp}", expanded=True):
            st.components.v1.html(html_by_salesperson[first_sp], height=600, scrolling=True)

        left, right = st.columns(2)
        safe_name = first_sp.replace("/", "-").replace("\\", "-")
        file_name = f"{safe_name}_gap_email.html"
        left.download_button(
            label=f"Download {first_sp} HTML",
            file_name=file_name,
            data=html_by_salesperson[first_sp],
            mime="text/html",
        )

        zip_bytes = build_zip(html_by_salesperson)
        right.download_button(
            label=f"Download All ({len(html_by_salesperson)}) as ZIP",
            file_name=f"gap_push_emails_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            data=zip_bytes,
            mime="application/zip",
        )


if __name__ == "__main__":
    render()
