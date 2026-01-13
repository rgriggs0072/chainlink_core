# utils/gap_history_mailer.py
"""
Gap History Emailer (PDF + summary)
-----------------------------------

Page overview for future devs:
- Sends Gap History PDFs (from GAP_CURRENT_STREAKS) to:
    - salesperson (TO)
    - manager (CC)
- Email body includes a short execution-style summary AND the Weekly Execution Focus table
  computed from the latest GAP_REPORT_SNAPSHOT week.
- Attachment is the Gap History PDF (per salesperson).

Hard rules:
- This module contains NO Streamlit UI (no st.session_state, no st.*).
- It does NOT publish snapshots or mutate data.
- It assumes snapshots are already published for the current week.

Data contract:
- DO NOT enrich ADDRESS outside this module.
- fetch_current_streaks() is the single source of truth for enriched streak rows
  (preview/download/email should all call it).
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from utils.email_utils import send_email_with_attachment
from utils.pdf_reports import GAP_HISTORY_PDF_COLUMNS, build_gap_streaks_pdf


# -----------------------------------------------------------------------------
# Contacts
# -----------------------------------------------------------------------------
def load_sales_contacts(con, tenant_id: int) -> pd.DataFrame:
    """
    Load active contacts from SALES_CONTACTS.

    Returns columns:
      SALESPERSON_NAME, SALESPERSON_EMAIL, MANAGER_NAME, MANAGER_EMAIL, SALESPERSON_NAME_UPPER
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
            (int(tenant_id),),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    if not df.empty:
        df["SALESPERSON_NAME_UPPER"] = df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    return df


# -----------------------------------------------------------------------------
# Streaks (enriched with address)
# -----------------------------------------------------------------------------
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

    Why this exists:
    - GAP_CURRENT_STREAKS does NOT contain ADDRESS.
    - CUSTOMERS does.
    - This function is the ONE shared data path for preview/download/email.

    Join keys:
    - TENANT_ID + CHAIN_NAME + STORE_NUMBER

    Notes:
    - CUSTOMERS.TENANT_ID is VARCHAR in your schema, while streaks TENANT_ID is NUMBER.
      We join using TO_VARCHAR(s.TENANT_ID) to match CUSTOMERS.TENANT_ID.
    """
    where_parts = ["s.TENANT_ID = %s", "s.STREAK_WEEKS >= %s"]
    params: List[object] = [int(tenant_id), int(min_streak)]

    def add_in(col: str, vals: Optional[List[str]]) -> None:
        clean = [v for v in (vals or []) if v]
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
          s.CHAIN_NAME,
          s.STORE_NUMBER,
          s.STORE_NAME,
          COALESCE(c.ADDRESS, '') AS ADDRESS,
          COALESCE(c.CITY, '')    AS CITY,
          COALESCE(c.COUNTY, '')  AS COUNTY,
          s.UPC,
          s.PRODUCT_NAME,
          s.SUPPLIER_NAME,
          s.STREAK_WEEKS
        FROM GAP_CURRENT_STREAKS s
        LEFT JOIN c_dedup c
          ON c.TENANT_ID = TO_VARCHAR(s.TENANT_ID)
         AND c.CHAIN_NAME = s.CHAIN_NAME
         AND c.STORE_NUMBER = s.STORE_NUMBER
         AND c.rn = 1
        WHERE {" AND ".join(where_parts)}
        ORDER BY s.SALESPERSON_NAME, s.STREAK_WEEKS DESC, s.CHAIN_NAME, s.STORE_NUMBER, s.PRODUCT_NAME
    """

    # params are for the main query; we need tenant_id first for the CUSTOMERS CTE
    params_with_cte = [str(tenant_id)] + params

    with con.cursor() as cur:
        cur.execute(sql, params_with_cte)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return pd.DataFrame(rows, columns=cols)


# -----------------------------------------------------------------------------
# Weekly Execution Focus (latest snapshot week)
# -----------------------------------------------------------------------------
def fetch_weekly_execution_focus(con, tenant_id: int, salesperson_name: str) -> pd.DataFrame:
    """
    Fetch Weekly Execution Focus for one salesperson for the tenant's latest snapshot week.

    Rules:
    - Use bind params (%s). No f-strings.
    - No SQL interpolation.
    - No "sql % params" logging (breaks %s + tuples).
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

    params = (
        int(tenant_id),
        int(tenant_id),
        str(salesperson_name or "").strip(),
    )

    #print("EXECUTION FOCUS SQL:\n", sql)


    with con.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return pd.DataFrame(rows, columns=cols)








# -----------------------------------------------------------------------------
# Summary HTML (email-safe)
# -----------------------------------------------------------------------------
def build_summary_html(
    salesperson_name: str,
    sp_df: pd.DataFrame,
    tenant_name: str = "",
    execution_df: Optional[pd.DataFrame] = None,
) -> str:
    """
    Build a professional HTML email body for Gap History (PDF attachment).

    Notes:
    - Keep styling email-client safe (tables + simple CSS).
    - execution_df is optional; when provided it renders the Weekly Execution Focus table.
    """
    df = sp_df.copy()
    df["STREAK_WEEKS"] = pd.to_numeric(df.get("STREAK_WEEKS"), errors="coerce").fillna(1).astype(int)

    active_gaps = int(len(df))
    new_this_week = int((df["STREAK_WEEKS"] == 1).sum())
    two_three = int(df["STREAK_WEEKS"].isin([2, 3]).sum())
    four_plus = int((df["STREAK_WEEKS"] >= 4).sum())

    top_chains = df["CHAIN_NAME"].fillna("Unknown").value_counts().head(3).to_dict()
    top_suppliers = df["SUPPLIER_NAME"].fillna("Unknown").value_counts().head(3).to_dict()

    def _bullet_lines(d: dict) -> str:
        if not d:
            return "<div style='color:#666;'>None</div>"
        return "".join(f"<div style='margin:2px 0;'>• <b>{k}</b>: {v}</div>" for k, v in d.items())

    def _execution_block(exe: Optional[pd.DataFrame]) -> str:
        if exe is None or exe.empty:
            return ""

        r = exe.iloc[0].to_dict()
        salesman = str(r.get("SALESMAN", salesperson_name) or salesperson_name)
        in_sch = int(r.get("IN_SCHEMATIC", 0) or 0)
        fulfilled = int(r.get("FULFILLED", 0) or 0)
        gaps = int(r.get("GAPS", 0) or 0)
        placement_90 = float(r.get("PLACEMENT_NEEDED_FOR_90", 0) or 0)
        pct_exec = int(r.get("PCT_EXECUTION", 0) or 0)
        gaps_away = float(r.get("GAPS_AWAY_FROM_90", 0) or 0)

        return f"""
        <div class="section">
          <h3>Weekly Execution Focus</h3>

          <table role="presentation" style="width:100%; border-collapse:collapse; font-size:13px; border:1px solid #e6e9ef;">
            <tr style="background:#F8F2EB;">
              <th style="text-align:left; padding:8px; border:1px solid #e6e9ef;">Salesman</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">In Schematic</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">Fulfilled</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">Gaps</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">Placement Needed (90%)</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">% Execution</th>
              <th style="text-align:right; padding:8px; border:1px solid #e6e9ef;">Gaps Away (90%)</th>
            </tr>
            <tr>
              <td style="padding:8px; border:1px solid #e6e9ef;">{salesman}</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{in_sch}</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{fulfilled}</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{gaps}</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{placement_90:,.1f}</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{pct_exec}%</td>
              <td style="padding:8px; border:1px solid #e6e9ef; text-align:right;">{gaps_away:,.1f}</td>
            </tr>
          </table>

          <div class="note" style="margin-top:10px;">
            <b>We need to get to 90% execution here.</b> Please focus on filling the gaps with products in stock.
            Reach out to your manager if you need support. Thank you!
          </div>
        </div>
        """

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ margin:0; padding:0; background:#f6f8fb; }}
    .wrap {{ max-width:720px; margin:0 auto; padding:18px; font-family: Arial, Helvetica, sans-serif; color:#111; }}
    .card {{ background:#ffffff; border:1px solid #e6e9ef; border-radius:14px; overflow:hidden; }}
    .header {{ background:#6497D6; color:#fff; padding:16px 18px; }}
    .title {{ font-size:18px; font-weight:700; margin:0; }}
    .subtitle {{ font-size:13px; opacity:0.95; margin:6px 0 0; }}
    .content {{ padding:16px 18px 18px; }}
    .metrics {{ width:100%; border-collapse:separate; border-spacing:10px; }}
    .metric {{ background:#F8F2EB; border:1px solid rgba(0,0,0,0.06); border-radius:12px; padding:12px; }}
    .m_label {{ font-size:12px; color:#444; margin:0 0 6px; }}
    .m_val {{ font-size:20px; font-weight:700; margin:0; }}
    .section {{ margin-top:14px; }}
    .section h3 {{ font-size:14px; margin:0 0 8px; }}
    .note {{ background:#fff7e6; border:1px solid #ffe2a8; padding:10px 12px; border-radius:12px; font-size:13px; }}
    .footer {{ color:#777; font-size:12px; margin-top:14px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="header">
        <p class="title">Gap History – Weekly Focus</p>
        <p class="subtitle">{salesperson_name}{f" • {tenant_name}" if tenant_name else ""}</p>
      </div>

      <div class="content">
        <table class="metrics" role="presentation">
          <tr>
            <td class="metric" width="25%"><div class="m_label">Active gaps</div><div class="m_val">{active_gaps}</div></td>
            <td class="metric" width="25%"><div class="m_label">New this week</div><div class="m_val">{new_this_week}</div></td>
            <td class="metric" width="25%"><div class="m_label">2–3 weeks</div><div class="m_val">{two_three}</div></td>
            <td class="metric" width="25%"><div class="m_label">4+ weeks</div><div class="m_val">{four_plus}</div></td>
          </tr>
        </table>

        <div class="section">
          <h3>Top chains</h3>
          {_bullet_lines(top_chains)}
        </div>

        <div class="section">
          <h3>Top suppliers</h3>
          {_bullet_lines(top_suppliers)}
        </div>

        {_execution_block(execution_df)}

        <div class="section note">
          <b>Attached:</b> your Gap History PDF.<br/>
          Priority order: <b>4+ weeks</b> → <b>2–3 weeks</b> → <b>new this week</b>.
        </div>

        <div class="footer">Generated {generated}</div>
      </div>
    </div>
  </div>
</body>
</html>
""".strip()


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
    only_salespeople: Optional[List[str]] = None,
) -> Dict[str, object]:
    """
    Send per-salesperson Gap History emails with PDF attachments.

    Returns:
      {
        "salesperson_success": int,
        "salesperson_fail": int,
        "skipped_salespeople": [str],
        "errors": [{"salesperson": str, "error": str}]
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

    contact_lookup = (
        contacts_df.drop_duplicates(subset=["SALESPERSON_NAME_UPPER"])
        .set_index("SALESPERSON_NAME_UPPER")
    )

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
        }

    streaks_df["SALESPERSON_NAME_UPPER"] = (
        streaks_df["SALESPERSON_NAME"].astype(str).str.strip().str.upper()
    )

    if only_salespeople:
        wanted = {str(x).strip().upper() for x in only_salespeople if x}
        streaks_df = streaks_df[streaks_df["SALESPERSON_NAME_UPPER"].isin(wanted)]

    success = 0
    fail = 0
    skipped: List[str] = []
    errors: List[dict] = []

    for sp_key, sp_df in streaks_df.groupby("SALESPERSON_NAME_UPPER"):
        if not sp_key or sp_key not in contact_lookup.index:
            skipped.append(sp_key)
            continue

        contact = contact_lookup.loc[sp_key]
        salesperson_name = contact["SALESPERSON_NAME"]
        to_email = str(contact.get("SALESPERSON_EMAIL") or "").strip()
        cc_email = str(contact.get("MANAGER_EMAIL") or "").strip() or None

        if not to_email:
            skipped.append(salesperson_name)
            continue
            


        try:
            # 1) Weekly Execution Focus
            import inspect
            print("fetch_weekly_execution_focus file:", inspect.getsourcefile(fetch_weekly_execution_focus))
            print("fetch_weekly_execution_focus first line:", inspect.getsourcelines(fetch_weekly_execution_focus)[1])

            execution_df = fetch_weekly_execution_focus(con, tenant_id, salesperson_name)

            # 2) PDF
            pdf_df = sp_df[[c for c in GAP_HISTORY_PDF_COLUMNS if c in sp_df.columns]].copy()
            pdf_bytes = build_gap_streaks_pdf(
                pdf_df,
                tenant_name=tenant_name,
                salesperson_name=salesperson_name,
                execution_df=execution_df,
            )

            # 3) HTML
            html_body = build_summary_html(
                salesperson_name,
                sp_df,
                tenant_name=tenant_name,
                execution_df=execution_df,
            )

            # 4) Send
            safe_name = salesperson_name.replace(" ", "_")
            subject = f"Gap History Report – {tenant_name}"
            filename = f"gap_history_{safe_name}.pdf"

            result = send_email_with_attachment(
                to_email=to_email,
                cc_email=cc_email,
                subject=subject,
                html=html_body,
                from_email=sender_email,
                attachment_bytes=pdf_bytes,
                attachment_filename=filename,
            )

            if result.get("success"):
                success += 1
            else:
                fail += 1
                errors.append({"salesperson": salesperson_name, "error": result.get("error")})

        except Exception as e:
            fail += 1
            errors.append({"salesperson": salesperson_name, "error": str(e)})



    return {
        "salesperson_success": success,
        "salesperson_fail": fail,
        "skipped_salespeople": skipped,
        "errors": errors,
    }
