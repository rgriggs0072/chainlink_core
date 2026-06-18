# utils/gap_history_mailer.py
# -*- coding: utf-8 -*-
"""
Gap History Emailer (PDF + summary)
-----------------------------------

Page overview for future devs:
- Sends Gap History PDFs (from GAP_CURRENT_STREAKS) to:
    - salesperson (TO)
    - manager (CC)
- Email body includes:
    - streak distribution summary
    - optional Weekly Execution Focus table (latest GAP_REPORT_SNAPSHOT week)
- Attachment is the Gap History PDF (per salesperson).

Hard rules:
- This module contains NO Streamlit UI (no st.session_state, no st.*).
- It does NOT publish snapshots or mutate data.
- It assumes snapshots are already published for the current week.

Data contract:
- fetch_current_streaks() is the single source of truth for streak rows
  enriched with customer address fields (CUSTOMERS join).
- The PDF input contract is GAP_HISTORY_PDF_COLUMNS (from utils.pdf_reports).
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.email_utils import send_email_with_attachment
from utils.pdf_reports import GAP_HISTORY_PDF_COLUMNS, build_gap_streaks_pdf
from utils.ai_insights import generate_salesperson_coaching



def _clean_email(x: object) -> str:
    """Normalize an email field; return '' if blank/None."""
    return str(x or "").strip()

def _build_cc_list(contact_row: pd.Series) -> List[str]:
    """
    Build CC recipients from SALES_CONTACTS:
      - MANAGER_EMAIL
      - MANAGER_EMAIL_2
      - EXTRA_CC_EMAIL

    Returns a de-duplicated list (case-insensitive).
    """
    candidates = [
        _clean_email(contact_row.get("MANAGER_EMAIL")),
        _clean_email(contact_row.get("MANAGER_EMAIL_2")),
        _clean_email(contact_row.get("EXTRA_CC_EMAIL")),
    ]

    seen = set()
    out: List[str] = []
    for e in candidates:
        if not e:
            continue
        key = e.lower()
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out



# =============================================================================
# Small utilities (pure helpers)
# =============================================================================
def _upper(s: object) -> str:
    return str(s or "").strip().upper()


def _coerce_int_series(s: pd.Series, default: int = 1) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype(int)


def _safe_float(x: object, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: object, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_name_for_file(name: str) -> str:
    return str(name or "salesperson").strip().replace(" ", "_").replace("/", "-").replace("\\", "-")


# =============================================================================
# Contacts
# =============================================================================
def load_sales_contacts(con, tenant_id: int) -> pd.DataFrame:
    """
    Load active contacts from SALES_CONTACTS.

    Returns columns:
      SALESPERSON_NAME, SALESPERSON_EMAIL,
      MANAGER_NAME, MANAGER_EMAIL, MANAGER_EMAIL_2, EXTRA_CC_EMAIL,
      SALESPERSON_NAME_UPPER
    """
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT
              SALESPERSON_NAME,
              SALESPERSON_EMAIL,
              MANAGER_NAME,
              MANAGER_EMAIL,
              MANAGER_EMAIL_2,
              EXTRA_CC_EMAIL
            FROM SALES_CONTACTS
            WHERE TENANT_ID = %s
              AND IS_ACTIVE = TRUE
            """,
            (int(tenant_id),),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    if not df.empty:
        df["SALESPERSON_NAME_UPPER"] = df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    return df



# =============================================================================
# Streaks (enriched with address)
# =============================================================================
def fetch_current_streaks(
    con,
    tenant_id: int,
    chains: Optional[List[str]] = None,
    suppliers: Optional[List[str]] = None,
    salespeople: Optional[List[str]] = None,
    min_streak: int = 1,
) -> pd.DataFrame:
    """
    Fetch current streak rows from GAP_CURRENT_STREAKS, enriched with customer fields from CUSTOMERS.

    Join keys:
    - TENANT_ID + CHAIN_NAME + STORE_NUMBER

    Notes:
    - CUSTOMERS.TENANT_ID is VARCHAR in your schema, while streaks TENANT_ID is NUMBER.
      We join using TO_VARCHAR(s.TENANT_ID) to match CUSTOMERS.TENANT_ID.
    """
    where_parts = ["s.TENANT_ID = %s", "s.STREAK_WEEKS >= %s"]
    params: List[object] = [int(tenant_id), int(min_streak)]

    def add_in(col: str, vals: Optional[List[str]]) -> None:
        clean = [v for v in (vals or []) if str(v).strip()]
        if not clean:
            return
        placeholders = ", ".join(["%s"] * len(clean))
        where_parts.append(f"{col} IN ({placeholders})")
        params.extend(clean)

    add_in("s.CHAIN_NAME", chains)
    add_in("s.SUPPLIER_NAME", suppliers)
    add_in("s.SALESPERSON_NAME", salespeople)

    sql = f"""
        WITH c_dedup AS (
            SELECT
                TENANT_ID,
                CHAIN_NAME,
                STORE_NUMBER,
                ADDRESS,
                CITY,
                COUNTY,
                SALESPERSON,
                ROW_NUMBER() OVER (
                    PARTITION BY TENANT_ID, CHAIN_NAME, STORE_NUMBER
                    ORDER BY COALESCE(UPDATED_AT, CREATED_AT) DESC
                ) AS rn
            FROM CUSTOMERS
            WHERE TENANT_ID = %s
        )
        SELECT
          s.TENANT_ID,
          s.SNAPSHOT_WEEK_START,
          s.FIRST_GAP_WEEK,
          s.LAST_GAP_WEEK,
          s.SALESPERSON_NAME,
          c.SALESPERSON AS CURRENT_SALESPERSON,
          s.CHAIN_NAME,
          s.STORE_NUMBER,
          s.STORE_NAME,
          COALESCE(c.ADDRESS, '') AS ADDRESS,
          COALESCE(c.CITY, '')    AS CITY,
          COALESCE(c.COUNTY, '')  AS COUNTY,
          s.UPC,
          s.PRODUCT_NAME,
          s.SUPPLIER_NAME,
          s.STREAK_WEEKS,
          s.GAP_FLAG
        FROM GAP_CURRENT_STREAKS s
        LEFT JOIN c_dedup c
          ON c.TENANT_ID = TO_VARCHAR(s.TENANT_ID)
         AND c.CHAIN_NAME = s.CHAIN_NAME
         AND c.STORE_NUMBER = s.STORE_NUMBER
         AND c.rn = 1
        WHERE {" AND ".join(where_parts)}
        ORDER BY s.SALESPERSON_NAME, s.STREAK_WEEKS DESC, s.CHAIN_NAME, s.STORE_NUMBER, s.PRODUCT_NAME
    """.strip()

    # 1st param belongs to the CUSTOMERS CTE, then the rest are main WHERE params
    params_with_cte = [str(tenant_id)] + params

    with con.cursor() as cur:
        cur.execute(sql, params_with_cte)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df

    # Normalize streaks to int for downstream filters/summaries
    if "STREAK_WEEKS" in df.columns:
        df["STREAK_WEEKS"] = _coerce_int_series(df["STREAK_WEEKS"], default=1)

    return df


# =============================================================================
# Weekly Execution Focus (latest snapshot week)
# =============================================================================
def fetch_execution_summary_df(con, tenant_id: int, salesperson_name: str) -> pd.DataFrame:
    """
    Public function:
    Fetch Weekly Execution Focus for one salesperson for the tenant's latest snapshot week.

    This function is deliberately public so BOTH:
    - email sender path
    - download/preview page path
    can use the exact same computation (keeps PDFs identical).
    """
    sql = """
    WITH latest AS (
      SELECT TENANT_ID, MAX(SNAPSHOT_WEEK_START) AS SNAPSHOT_WEEK_START
      FROM GAP_REPORT_SNAPSHOT
      WHERE TENANT_ID = %s
      GROUP BY TENANT_ID
    ),
    base AS (
      SELECT s.*
      FROM GAP_REPORT_SNAPSHOT s
      JOIN latest l
        ON l.TENANT_ID = s.TENANT_ID
       AND l.SNAPSHOT_WEEK_START = s.SNAPSHOT_WEEK_START
      WHERE s.TENANT_ID = %s
        AND s.SALESPERSON_NAME = %s
        AND COALESCE(s.IN_SCHEMATIC, FALSE) = TRUE
    )
    SELECT
      SALESPERSON_NAME AS SALESMAN,
      COUNT(*) AS IN_SCHEMATIC,
      SUM(IFF(COALESCE(IS_GAP, FALSE) = FALSE, 1, 0)) AS FULFILLED,
      SUM(IFF(COALESCE(IS_GAP, FALSE) = TRUE,  1, 0)) AS GAPS,
      (0.9 * COUNT(*)) AS PLACEMENT_NEEDED_FOR_90,
      ROUND(
        (SUM(IFF(COALESCE(IS_GAP, FALSE) = FALSE, 1, 0)) / NULLIF(COUNT(*), 0)) * 100,
        0
       ) AS PCT_EXECUTION,
      GREATEST(
        0,
        (0.9 * COUNT(*)) - SUM(IFF(COALESCE(IS_GAP, FALSE) = FALSE, 1, 0))
      ) AS GAPS_AWAY_FROM_90
    FROM base
    GROUP BY SALESPERSON_NAME
    """.strip()

    params = (int(tenant_id), int(tenant_id), str(salesperson_name or "").strip())

    with con.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return pd.DataFrame(rows, columns=cols)


# Backwards-compatible alias (keep older imports working)
def fetch_weekly_execution_focus(con, tenant_id: int, salesperson_name: str) -> pd.DataFrame:
    """Backward-compatible name -> calls the canonical execution fetcher."""
    return fetch_execution_summary_df(con, tenant_id, salesperson_name)


# =============================================================================
# Summary HTML (email-safe)
# =============================================================================
def build_summary_html(
    salesperson_name: str,
    sp_df: pd.DataFrame,
    tenant_name: str = "",
    execution_df: Optional[pd.DataFrame] = None,
    ai_coaching: str = "",
) -> str:
    """
    Build a professional HTML email body for Gap History (PDF attachment).

    Notes:
    - Keep styling email-client safe (tables + simple CSS).
    - execution_df is optional; when provided it renders the Weekly Execution Focus table.
    """
    df = sp_df.copy()

    if "STREAK_WEEKS" in df.columns:
        df["STREAK_WEEKS"] = _coerce_int_series(df["STREAK_WEEKS"], default=1)
    else:
        df["STREAK_WEEKS"] = 1

    active_gaps = int(len(df))
    new_this_week = int((df["STREAK_WEEKS"] == 1).sum())
    two_three = int(df["STREAK_WEEKS"].isin([2, 3]).sum())
    four_plus = int((df["STREAK_WEEKS"] >= 4).sum())

    top_chains = df.get("CHAIN_NAME", pd.Series(dtype=str)).fillna("Unknown").value_counts().head(3).to_dict()
    top_suppliers = df.get("SUPPLIER_NAME", pd.Series(dtype=str)).fillna("Unknown").value_counts().head(3).to_dict()

    def _bullet_lines(d: dict) -> str:
        if not d:
            return "<div style='color:#94A3B8; font-size:13px;'>None</div>"
        return "".join(
            f"<div style='font-size:13px; margin:5px 0; color:#1E293B;'>"
            f"<span style='color:#6497D6; font-weight:700;'>&#x25CF;</span> "
            f"<b>{k}</b> &nbsp;<span style='color:#64748B;'>{v} gaps</span></div>"
            for k, v in d.items()
        )

    def _execution_block(exe: Optional[pd.DataFrame]) -> str:
        if exe is None or exe.empty:
            return ""

        r = exe.iloc[0].to_dict()

        salesman = str(r.get("SALESMAN") or salesperson_name)
        in_sch = _safe_int(r.get("IN_SCHEMATIC"), 0)
        fulfilled = _safe_int(r.get("FULFILLED"), 0)
        gaps = _safe_int(r.get("GAPS"), 0)
        placement_90 = _safe_float(r.get("PLACEMENT_NEEDED_FOR_90"), 0.0)
        pct_exec = _safe_int(r.get("PCT_EXECUTION"), 0)
        gaps_away = _safe_float(r.get("GAPS_AWAY_FROM_90"), 0.0)

        return f"""
        <div style="background:#fff; border-radius:10px; padding:14px 16px; margin-bottom:12px;">
          <div class="section-title">Weekly Execution Focus</div>
          <table role="presentation" style="width:100%; border-collapse:collapse; font-size:13px; margin-top:8px;">
            <tr style="background:#F1F5F9;">
              <th style="text-align:left; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Salesperson</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">In Schematic</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Fulfilled</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Gaps</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Needed for 90%</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Execution</th>
              <th style="text-align:right; padding:9px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; color:#64748B;">Gaps to 90%</th>
            </tr>
            <tr>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; font-weight:600;">{salesman}</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right;">{in_sch}</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right;">{fulfilled}</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right; color:#DC2626; font-weight:700;">{gaps}</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right;">{placement_90:,.0f}</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right; font-size:16px; font-weight:800; color:#6497D6;">{pct_exec}%</td>
              <td style="padding:9px 10px; border-bottom:1px solid #F1F5F9; text-align:right; color:#DC2626;">{gaps_away:,.0f}</td>
            </tr>
          </table>
        </div>
        """

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    tenant_line = f" &nbsp;|&nbsp; {tenant_name.upper()}" if tenant_name else ""

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ margin:0; padding:0; background:#EEF2F7; font-family: Arial, Helvetica, sans-serif; color:#1E293B; }}
    .wrap {{ max-width:680px; margin:0 auto; padding:24px 16px; }}

    /* Header rendered via table+bgcolor for Outlook — no CSS class needed */

    /* Coaching */
    .coaching {{ background:#fff; border-left:4px solid #991B1B; border-radius:10px; padding:16px 20px; margin:12px 0 0 0; }}
    .coaching-label {{ font-size:13px; font-weight:700; letter-spacing:1px; color:#991B1B; text-transform:uppercase; margin:0 0 8px; }}
    .coaching-text {{ font-size:14px; line-height:1.65; color:#1E293B; margin:0; }}

    /* Metrics row */
    .metrics-row {{ display:table; width:100%; border-collapse:separate; border-spacing:0; }}
    .metric-cell {{ display:table-cell; width:25%; padding:14px 10px; text-align:center; vertical-align:top; }}
    .metric-inner {{ border-radius:10px; padding:12px 8px; }}
    .m-new {{ background:#FEF3C7; }}
    .m-mid {{ background:#FEE2E2; }}
    .m-old {{ background:#7F1D1D; color:#fff; }}
    .m-total {{ background:#EFF6FF; }}
    .m_label {{ font-size:11px; font-weight:600; opacity:0.75; margin:0 0 5px; text-transform:uppercase; letter-spacing:0.5px; }}
    .m_val {{ font-size:24px; font-weight:800; margin:0; line-height:1; }}

    /* Two-col section */
    .two-col {{ display:table; width:100%; border-collapse:separate; border-spacing:12px 0; margin-top:4px; }}
    .col {{ display:table-cell; width:50%; vertical-align:top; }}
    .section-box {{ background:#fff; border-radius:10px; padding:14px 16px; height:100%; box-sizing:border-box; }}
    .section-title {{ font-size:11px; font-weight:700; letter-spacing:1px; color:#64748B; text-transform:uppercase; margin:0 0 10px; }}
    .bullet {{ font-size:13px; margin:5px 0; color:#1E293B; }}
    .bullet b {{ color:#6497D6; }}

    /* Execution table */
    .exec-wrap {{ background:#fff; border-radius:10px; padding:14px 16px; margin-top:0; }}
    .exec-table {{ width:100%; border-collapse:collapse; font-size:12px; margin-top:8px; }}
    .exec-table th {{ background:#F1F5F9; color:#64748B; font-weight:700; text-align:left; padding:8px 10px; border-bottom:2px solid #E2E8F0; font-size:11px; letter-spacing:0.5px; text-transform:uppercase; }}
    .exec-table td {{ padding:8px 10px; border-bottom:1px solid #F1F5F9; font-size:13px; }}
    .exec-pct {{ font-size:18px; font-weight:800; color:#6497D6; }}

    /* Attachment note */
    .attach-note {{ background:#F8FAFC; border:1px dashed #B3D7ED; border-radius:10px; padding:12px 16px; font-size:13px; color:#475569; }}

    /* Footer */
    .footer {{ text-align:center; color:#94A3B8; font-size:11px; margin-top:16px; }}

    .card-body {{ background:#F1F5F9; padding:20px; border-radius:0 0 16px 16px; }}
    .gap-row {{ margin-bottom:12px; }}
  </style>
</head>
<body>
  <div class="wrap">

    <!-- Header (table+bgcolor for Outlook compatibility) -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-radius:16px 16px 0 0; overflow:hidden;">
      <tr>
        <td bgcolor="#6497D6" style="background:#6497D6; padding:22px 24px 18px; border-radius:16px 16px 0 0;">
          <p style="font-size:11px; font-weight:700; letter-spacing:2px; color:#B3D7ED; margin:0 0 6px; text-transform:uppercase;">Gap History &mdash; Weekly Focus</p>
          <p style="font-size:22px; font-weight:700; color:#ffffff; margin:0;">{salesperson_name}</p>
        </td>
      </tr>
    </table>

    <!-- Coaching note (top, before everything) -->
    {f'''
    <div class="coaching" style="padding-top:20px; padding-bottom:16px;">
      <div class="coaching-label">&#x1F4A1; Your Weekly Coaching Note</div>
      <p class="coaching-text">{ai_coaching}</p>
    </div>
    ''' if ai_coaching else ''}

    <div class="card-body">

      <!-- Gap metrics -->
      <div style="margin-bottom:12px;">
        <table role="presentation" style="width:100%; border-collapse:separate; border-spacing:8px;">
          <tr>
            <td style="width:25%; background:#EFF6FF; border-radius:10px; padding:14px 10px; text-align:center;">
              <div class="m_label" style="color:#3B82F6;">Total Gaps</div>
              <div class="m_val" style="color:#1E40AF;">{active_gaps}</div>
            </td>
            <td style="width:25%; background:#FEF3C7; border-radius:10px; padding:14px 10px; text-align:center;">
              <div class="m_label" style="color:#D97706;">New This Week</div>
              <div class="m_val" style="color:#92400E;">{new_this_week}</div>
            </td>
            <td style="width:25%; background:#FEE2E2; border-radius:10px; padding:14px 10px; text-align:center;">
              <div class="m_label" style="color:#EF4444;">2&ndash;3 Weeks</div>
              <div class="m_val" style="color:#991B1B;">{two_three}</div>
            </td>
            <td style="width:25%; background:#7F1D1D; border-radius:10px; padding:14px 10px; text-align:center;">
              <div class="m_label" style="color:#FCA5A5;">4+ Weeks</div>
              <div class="m_val" style="color:#fff;">{four_plus}</div>
            </td>
          </tr>
        </table>
      </div>

      <!-- Chains + Suppliers side by side -->
      <table role="presentation" style="width:100%; border-collapse:separate; border-spacing:8px; margin-bottom:12px;">
        <tr>
          <td style="width:50%; vertical-align:top; background:#fff; border-radius:10px; padding:14px 16px;">
            <div class="section-title">Top Chains</div>
            {_bullet_lines(top_chains)}
          </td>
          <td style="width:50%; vertical-align:top; background:#fff; border-radius:10px; padding:14px 16px;">
            <div class="section-title">Top Suppliers</div>
            {_bullet_lines(top_suppliers)}
          </td>
        </tr>
      </table>

      <!-- Execution Focus -->
      {_execution_block(execution_df)}

      <!-- Attachment note -->
      <div class="attach-note">
        <b>&#x1F4CE; Attached:</b> your full Gap History PDF &mdash;
        work oldest gaps first: <b>4+ weeks &rarr; 2&ndash;3 weeks &rarr; new this week</b>.
      </div>

    </div><!-- end card-body -->

    <div class="footer">Chainlink Analytics &bull; {tenant_name.upper() + " &bull; " if tenant_name else ""}Generated {generated}</div>
  </div>
</body>
</html>
""".strip()


# =============================================================================
# Pre-send validation
# =============================================================================
def validate_contacts_before_send(
    con,
    tenant_id: int,
    streaks_df: pd.DataFrame,
) -> List[str]:
    """
    Validate that every salesperson in the current gap data has an active
    SALES_CONTACTS entry. Uses CURRENT_SALESPERSON_UPPER (live CUSTOMERS value).

    Returns a list of rep names missing from SALES_CONTACTS.
    Empty list means all reps are covered — safe to send.
    """
    reps_in_report = (
        streaks_df["CURRENT_SALESPERSON_UPPER"]
        .dropna()
        .unique()
        .tolist()
    )

    if not reps_in_report:
        return []

    with con.cursor() as cur:
        cur.execute(
            """
            SELECT UPPER(TRIM(SALESPERSON_NAME)) AS SALESPERSON_NAME_NORM
            FROM SALES_CONTACTS
            WHERE TENANT_ID = %s
              AND IS_ACTIVE = TRUE
            """,
            (int(tenant_id),),
        )
        rows = cur.fetchall()

    active_reps = {str(r[0]) for r in rows} if rows else set()
    return [rep for rep in reps_in_report if rep not in active_reps]


# =============================================================================
# Orchestrator
# =============================================================================
def send_gap_history_pdfs(
    con,
    tenant_id: int,
    tenant_name: str,
    sender_email: str,
    chains: Optional[List[str]] = None,
    suppliers: Optional[List[str]] = None,
    salespeople: Optional[List[str]] = None,
    min_streak: int = 1,
    only_salespeople: Optional[List[str]] = None,
    ai_api_key: str = "",
) -> Dict[str, object]:
    """
    Send per-salesperson Gap History emails with PDF attachments.

    What "success" means:
    - success increments when the email send returns success=True (salesperson TO send succeeded).
    - CC recipients are best-effort; we track them separately using the SMTP recipient list returned
      by send_email_with_attachment() (expects result["recipients"]).

    Returns:
      {
        "salesperson_success": int,     # count of successful sends (TO)
        "salesperson_fail": int,        # count of failed sends (TO)
        "skipped_salespeople": [str],   # missing contact rows or missing TO email
        "errors": [{"salesperson": str, "error": str}],
        "sent_without_cc": [str],       # successful TO send but no CC recipients
        "salesperson_emailed": int,     # successful TO deliveries
        "manager_emailed": int,         # successful CC deliveries (count of CC recipients)
        "total_emails_sent": int,       # total recipients across all successful sends (TO + CC)
      }
    """

    # -----------------------------
    # Delivery metrics (truthful)
    # -----------------------------
    salesperson_emailed = 0
    manager_emailed = 0
    total_emails_sent = 0

    # -----------------------------
    # Contacts
    # -----------------------------
    contacts_df = load_sales_contacts(con, tenant_id)
    if contacts_df.empty:
        return {
            "salesperson_success": 0,
            "salesperson_fail": 0,
            "skipped_salespeople": ["(no SALES_CONTACTS rows)"],
            "errors": [],
            "sent_without_cc": [],
            "salesperson_emailed": 0,
            "manager_emailed": 0,
            "total_emails_sent": 0,
        }

    contact_lookup = (
        contacts_df.drop_duplicates(subset=["SALESPERSON_NAME_UPPER"])
        .set_index("SALESPERSON_NAME_UPPER")
    )

    # -----------------------------
    # Streak rows (address-enriched)
    # -----------------------------
    streaks_df = fetch_current_streaks(
        con=con,
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
            "sent_without_cc": [],
            "salesperson_emailed": 0,
            "manager_emailed": 0,
            "total_emails_sent": 0,
        }

    streaks_df["SALESPERSON_NAME_UPPER"] = (
        streaks_df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    )

    # CURRENT_SALESPERSON: live assignment from CUSTOMERS; fall back to snapshot
    # name for stores that exist in the snapshot but have been removed from CUSTOMERS
    # (e.g., closed stores). This prevents those rows from silently dropping out.
    streaks_df["CURRENT_SALESPERSON_UPPER"] = (
        streaks_df["CURRENT_SALESPERSON"]
        .fillna(streaks_df["SALESPERSON_NAME"])
        .astype(str)
        .str.strip()
        .str.upper()
    )

    if only_salespeople:
        wanted = {_upper(x) for x in only_salespeople if str(x).strip()}
        streaks_df = streaks_df[streaks_df["CURRENT_SALESPERSON_UPPER"].isin(wanted)]

    # -----------------------------
    # Pre-send validation gate
    # -----------------------------
    missing_contacts = validate_contacts_before_send(con, tenant_id, streaks_df)
    if missing_contacts:
        missing_list = ", ".join(missing_contacts)
        return {
            "salesperson_success": 0,
            "salesperson_fail": 0,
            "skipped_salespeople": [],
            "errors": [],
            "sent_without_cc": [],
            "salesperson_emailed": 0,
            "manager_emailed": 0,
            "total_emails_sent": 0,
            "missing_contacts": missing_contacts,
            "error": (
                f"Cannot send gap report emails. The following salespeople have "
                f"stores in this report but no email address in Sales Contacts: "
                f"{missing_list}. "
                f"Please add them to Sales Contacts and try again."
            ),
        }

    # -----------------------------
    # Orchestrate sends
    # -----------------------------
    success = 0
    fail = 0
    skipped: List[str] = []
    errors: List[dict] = []
    sent_without_cc: List[str] = []

    for sp_key, sp_df in streaks_df.groupby("CURRENT_SALESPERSON_UPPER"):
        if not sp_key or sp_key not in contact_lookup.index:
            skipped.append(sp_key or "(missing salesperson)")
            continue

        contact = contact_lookup.loc[sp_key]
        salesperson_name = str(contact.get("SALESPERSON_NAME") or "").strip() or sp_key
        to_email = str(contact.get("SALESPERSON_EMAIL") or "").strip()

        # CC: keep as list (send_email_with_attachment will normalize anyway)
        cc_list = _build_cc_list(contact)  # List[str]
        cc_email = cc_list                 # ✅ pass list through (not comma string)

        if not to_email:
            skipped.append(salesperson_name)
            continue

        try:
            # 1) Weekly Execution Focus (header summary)
            execution_df = fetch_execution_summary_df(con, tenant_id, salesperson_name)

            # 2) PDF (use contract columns)
            pdf_df = sp_df[[c for c in GAP_HISTORY_PDF_COLUMNS if c in sp_df.columns]].copy()
            pdf_bytes = build_gap_streaks_pdf(
                pdf_df,
                tenant_name=tenant_name,
                salesperson_name=salesperson_name,
                execution_df=execution_df,
            )

            # 3) AI coaching message (Haiku, per-salesperson)
            coaching = generate_salesperson_coaching(
                salesperson_name=salesperson_name,
                sp_df=sp_df,
                execution_df=execution_df,
                api_key=ai_api_key,
            )

            # 4) HTML body
            html_body = build_summary_html(
                salesperson_name=salesperson_name,
                sp_df=sp_df,
                tenant_name=tenant_name,
                execution_df=execution_df,
                ai_coaching=coaching,
            )

            # 5) Send
            subject = f"Gap History Report – {tenant_name}"
            filename = f"gap_history_{_safe_name_for_file(salesperson_name)}.pdf"

            result = send_email_with_attachment(
                to_email=to_email,
                cc_email=cc_email,  # ✅ list
                subject=subject,
                html=html_body,
                from_email=sender_email,
                attachment_bytes=pdf_bytes,
                attachment_filename=filename,
            )

            if result.get("success"):
                success += 1

                # truthful metrics based on actual SMTP recipient list
                recipients = result.get("recipients") or []
                salesperson_emailed += 1  # TO was delivered
                total_emails_sent += len(recipients) if recipients else 1
                manager_emailed += max(0, (len(recipients) - 1)) if recipients else 0

                # if no CC recipients at all
                if not cc_list:
                    sent_without_cc.append(salesperson_name)

            else:
                fail += 1
                errors.append({
                    "salesperson": salesperson_name,
                    "error": result.get("error"),
                    "refused": result.get("refused", []),
                })

        except Exception as e:
            fail += 1
            errors.append({"salesperson": salesperson_name, "error": str(e)})

    return {
        "salesperson_success": success,
        "salesperson_fail": fail,
        "skipped_salespeople": skipped,
        "errors": errors,
        "sent_without_cc": sent_without_cc,
        "salesperson_emailed": salesperson_emailed,
        "manager_emailed": manager_emailed,
        "total_emails_sent": total_emails_sent,
    }














