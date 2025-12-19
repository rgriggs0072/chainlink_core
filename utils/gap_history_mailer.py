# utils/gap_history_emailer.py
"""
Gap History Emailer (PDF + summary)

Page overview for future devs:
- Sends Gap History PDFs (from GAP_CURRENT_STREAKS) to:
    - salesperson (TO)
    - manager (CC)
- Email body includes a short execution-style summary (like legacy HTML).
- The attachment is the Gap History PDF (per salesperson).

Hard rules:
- This module contains NO Streamlit UI.
- It does NOT publish snapshots or mutate data.
- It assumes snapshots are already published for the current week.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.email_utils import send_email_with_attachment
from utils.pdf_reports import build_gap_streaks_pdf


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------
@dataclass
class Contact:
    salesperson_name: str
    salesperson_email: str
    manager_email: Optional[str] = None
    manager_name: Optional[str] = None


# -----------------------------------------------------------------------------
# Queries
# -----------------------------------------------------------------------------
def load_sales_contacts(con, tenant_id: int) -> pd.DataFrame:
    """
    Load active contacts.

    Returns columns:
      SALESPERSON_NAME, SALESPERSON_EMAIL, MANAGER_NAME, MANAGER_EMAIL
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
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    if not df.empty:
        df["SALESPERSON_NAME_UPPER"] = df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    return df


def fetch_current_streaks(
    con,
    tenant_id: int,
    chains: Optional[List[str]] = None,
    suppliers: Optional[List[str]] = None,
    salespeople: Optional[List[str]] = None,
    min_streak: int = 1,
) -> pd.DataFrame:
    """
    Fetch current streak rows from GAP_CURRENT_STREAKS.

    Note:
    - This view already represents "current streaks" (i.e., streaks that end on latest week).
    """
    where_parts = ["TENANT_ID = %s", "STREAK_WEEKS >= %s"]
    params: List[object] = [tenant_id, int(min_streak)]

    def add_in(col: str, vals: Optional[List[str]]):
        vals = [v for v in (vals or []) if v]
        if not vals:
            return
        placeholders = ", ".join(["%s"] * len(vals))
        where_parts.append(f"{col} IN ({placeholders})")
        params.extend(vals)

    add_in("CHAIN_NAME", chains)
    add_in("SUPPLIER_NAME", suppliers)
    add_in("SALESPERSON_NAME", salespeople)

    sql = f"""
        SELECT
          SNAPSHOT_WEEK_START,
          FIRST_GAP_WEEK,
          LAST_GAP_WEEK,
          SALESPERSON_NAME,
          CHAIN_NAME,
          STORE_NUMBER,
          STORE_NAME,
          UPC,
          PRODUCT_NAME,
          SUPPLIER_NAME,
          STREAK_WEEKS
        FROM GAP_CURRENT_STREAKS
        WHERE {" AND ".join(where_parts)}
        ORDER BY SALESPERSON_NAME, STREAK_WEEKS DESC, CHAIN_NAME, STORE_NUMBER, PRODUCT_NAME
    """

    with con.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


# -----------------------------------------------------------------------------
# Summary HTML (simple + actionable)
# -----------------------------------------------------------------------------
def build_summary_html(salesperson: str, sp_df: pd.DataFrame) -> str:
    """
    Build a compact HTML summary that mirrors the legacy “execution focus” feel.

    Uses:
    - total gaps (rows)
    - buckets by streak length (1, 2-3, 4+)
    - top chains / suppliers
    """
    total = int(len(sp_df))

    s = pd.to_numeric(sp_df.get("STREAK_WEEKS", 1), errors="coerce").fillna(1).astype(int)
    new_this_week = int((s == 1).sum())
    mid = int(((s >= 2) & (s <= 3)).sum())
    hot = int((s >= 4).sum())

    top_chains = (
        sp_df["CHAIN_NAME"].fillna("—").value_counts().head(3)
        if "CHAIN_NAME" in sp_df.columns else pd.Series(dtype=int)
    )
    top_suppliers = (
        sp_df["SUPPLIER_NAME"].fillna("—").value_counts().head(3)
        if "SUPPLIER_NAME" in sp_df.columns else pd.Series(dtype=int)
    )

    def as_list(vc: pd.Series) -> str:
        if vc.empty:
            return "<li>—</li>"
        return "".join([f"<li>{k}: {v}</li>" for k, v in vc.items()])

    style = """
    <style>
      .wrap { font-family: Arial, Helvetica, sans-serif; color: #111; }
      .title { font-size: 18px; font-weight: 700; margin: 0 0 10px; }
      .card { background:#f8f2eb; border:1px solid #e6e1dc; border-radius:12px; padding:12px; }
      .kpi { display:flex; gap:10px; flex-wrap:wrap; margin:10px 0; }
      .k { background:#fff; border:1px solid #eee; border-radius:10px; padding:8px 10px; min-width:140px; }
      .k .l { font-size:12px; color:#555; }
      .k .v { font-size:18px; font-weight:700; }
      ul { margin:6px 0 0 18px; }
      .muted { color:#666; font-size:12px; margin-top:10px; }
    </style>
    """

    body = f"""
    <div class="wrap">
      <div class="title">Gap History – Weekly Focus ({salesperson})</div>
      <div class="card">
        <div class="kpi">
          <div class="k"><div class="l">Active gaps</div><div class="v">{total}</div></div>
          <div class="k"><div class="l">New this week</div><div class="v">{new_this_week}</div></div>
          <div class="k"><div class="l">2–3 weeks</div><div class="v">{mid}</div></div>
          <div class="k"><div class="l">4+ weeks</div><div class="v">{hot}</div></div>
        </div>

        <div style="display:flex; gap:16px; flex-wrap:wrap;">
          <div style="flex:1; min-width:240px;">
            <div><b>Top chains</b></div>
            <ul>{as_list(top_chains)}</ul>
          </div>
          <div style="flex:1; min-width:240px;">
            <div><b>Top suppliers</b></div>
            <ul>{as_list(top_suppliers)}</ul>
          </div>
        </div>

        <div style="margin-top:12px;">
          Attached is your Gap History PDF. Please focus first on the 4+ week items.
        </div>

        <div class="muted">Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
      </div>
    </div>
    """
    return f"<!doctype html><html><head>{style}</head><body>{body}</body></html>"


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------
def send_gap_history_pdfs(
    con,
    tenant_id: int,
    tenant_name: str,
    sender_email: str,
    chains: Optional[List[str]] = None,
    suppliers: Optional[List[str]] = None,
    salespeople: Optional[List[str]] = None,
    min_streak: int = 1,
    only_salespeople: Optional[List[str]] = None,  # for “send selected only”
) -> Dict[str, object]:
    """
    Send per-salesperson Gap History emails with PDF attachments.

    Returns:
      summary dict like:
        {
          "salesperson_success": int,
          "salesperson_fail": int,
          "skipped_salespeople": [names...],
          "errors": [{"salesperson":..., "error":...}, ...]
        }
    """
    contacts_df = load_sales_contacts(con, tenant_id)
    if contacts_df.empty:
        return {
            "salesperson_success": 0,
            "salesperson_fail": 0,
            "skipped_salespeople": ["(no SALES_CONTACTS rows)"],
            "errors": [],
        }

    streaks_df = fetch_current_streaks(
        con,
        tenant_id=tenant_id,
        chains=chains,
        suppliers=suppliers,
        salespeople=salespeople,
        min_streak=min_streak,
    )
    if streaks_df.empty:
        return {
            "salesperson_success": 0,
            "salesperson_fail": 0,
            "skipped_salespeople": ["(no streak rows matched filters)"],
            "errors": [],
        }

    contacts_df = contacts_df.copy()
    lookup = (
        contacts_df.drop_duplicates(subset=["SALESPERSON_NAME_UPPER"])
        .set_index("SALESPERSON_NAME_UPPER")
    )

    if only_salespeople:
        wanted = {str(x).strip().upper() for x in only_salespeople if x}
        streaks_df = streaks_df[streaks_df["SALESPERSON_NAME"].astype(str).str.strip().str.upper().isin(wanted)]

    success = 0
    fail = 0
    skipped: List[str] = []
    errors: List[dict] = []

    for sp_name, sp_df in streaks_df.groupby("SALESPERSON_NAME", dropna=False):
        sp_key = str(sp_name).strip().upper()
        if sp_key not in lookup.index:
            skipped.append(str(sp_name))
            continue

        row = lookup.loc[sp_key]
        to_email = str(row.get("SALESPERSON_EMAIL") or "").strip()
        cc_email = str(row.get("MANAGER_EMAIL") or "").strip() or None

        if not to_email:
            skipped.append(str(sp_name))
            continue

        # Build PDF (attachment)
        pdf_cols = [
            "SALESPERSON_NAME", "CHAIN_NAME", "STORE_NUMBER", "STORE_NAME",
            "SUPPLIER_NAME", "PRODUCT_NAME", "UPC", "STREAK_WEEKS",
            "FIRST_GAP_WEEK", "LAST_GAP_WEEK",
        ]
        pdf_df = sp_df[[c for c in pdf_cols if c in sp_df.columns]].copy()
        pdf_bytes = build_gap_streaks_pdf(pdf_df, tenant_name=tenant_name)

        # Body summary
        html = build_summary_html(str(sp_name), sp_df)

        subject = f"Gap History Report – {tenant_name} – {str(sp_name)}"
        filename = f"gap_history_{tenant_name}_{str(sp_name)}.pdf".replace(" ", "_")

        result = send_email_with_attachment(
            to_email=to_email,
            cc_email=cc_email,
            subject=subject,
            html=html,
            from_email=sender_email,
            attachment_bytes=pdf_bytes,
            attachment_filename=filename,
        )

        if result.get("success"):
            success += 1
        else:
            fail += 1
            errors.append({"salesperson": str(sp_name), "error": result.get("error")})

    return {
        "salesperson_success": success,
        "salesperson_fail": fail,
        "skipped_salespeople": skipped,
        "errors": errors,
    }
