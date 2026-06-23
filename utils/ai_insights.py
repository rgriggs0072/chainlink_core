# utils/ai_insights.py
"""
Home dashboard proactive anomaly detection + per-salesperson email coaching.
"""
import json
import anthropic
import streamlit as st
import pandas as pd
from typing import Any

_MAX_INSIGHTS = 6
_MODEL = "claude-sonnet-4-6"
_COACHING_MODEL = "claude-haiku-4-5-20251001"

_SEVERITY_ICON = {
    "warning":  "⚠️",
    "info":     "💡",
    "positive": "✅",
}


def fetch_duplicate_stores(conn: Any, tenant_id: int) -> list[dict]:
    """
    Query CUSTOMERS for CHAIN_NAME + STORE_NUMBER combinations that appear
    more than once for the same tenant. Returns rows as dicts, or [] on error.
    Only examines ACCOUNT_STATUS = 'ACTIVE' records.
    """
    sql = """
        SELECT
            CHAIN_NAME,
            STORE_NUMBER,
            COUNT(*) AS DUPLICATE_COUNT,
            LISTAGG(CITY, ' | ') WITHIN GROUP (ORDER BY CITY) AS CITIES,
            LISTAGG(ADDRESS, ' | ') WITHIN GROUP (ORDER BY CITY) AS ADDRESSES,
            LISTAGG(SALESPERSON, ' | ') WITHIN GROUP (ORDER BY CITY) AS SALESPERSONS,
            LISTAGG(CUSTOMER_ID::VARCHAR, ' | ') WITHIN GROUP (ORDER BY CITY) AS CUSTOMER_IDS
        FROM CUSTOMERS
        WHERE TENANT_ID = %(tenant_id)s
          AND ACCOUNT_STATUS = 'ACTIVE'
        GROUP BY CHAIN_NAME, STORE_NUMBER
        HAVING COUNT(*) > 1
        ORDER BY CHAIN_NAME, STORE_NUMBER
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, {"tenant_id": tenant_id})
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows
    except Exception:
        return []


def _build_prompt(
    execution_summary: tuple,
    salesperson_df: pd.DataFrame,
    gap_history_df: pd.DataFrame,
    chain_df: pd.DataFrame,
    duplicate_stores: list[dict] | None = None,
) -> str:
    total_in_schematic, purchased, total_gaps, purchased_pct = execution_summary

    sp_text = "No data"
    if not salesperson_df.empty:
        sp_text = salesperson_df.head(15).to_string(index=False)

    hist_text = "No data"
    if not gap_history_df.empty:
        df = gap_history_df.copy()
        df["LOG_DATE"] = pd.to_datetime(df["LOG_DATE"], errors="coerce")
        recent_dates = df["LOG_DATE"].drop_duplicates().nlargest(4)
        hist_text = df[df["LOG_DATE"].isin(recent_dates)].to_string(index=False)

    chain_text = "No data"
    if not chain_df.empty:
        chain_text = chain_df.to_string(index=False)

    dup_section = ""
    if duplicate_stores:
        dup_lines = "\n".join(
            f"- {row['CHAIN_NAME']} Store #{row['STORE_NUMBER']}: "
            f"found in {row['CITIES']} (Customer IDs: {row['CUSTOMER_IDS']})"
            for row in duplicate_stores
        )
        dup_section = f"""

DATA INTEGRITY ISSUES DETECTED:
The following store numbers exist more than once for the same chain at different locations.
These are source application data issues and should be flagged to the admin immediately:
{dup_lines}

Flag these as severity "warning" in your response.
Use this exact language for each: "[CHAIN] has [N] locations with store number [STORE_NUMBER] — resolve in source application before the next data upload."
"""

    return f"""You are analyzing sales execution data for a beverage distribution company.
Identify the most important anomalies, risks, and opportunities a sales manager should act on TODAY.

OVERALL EXECUTION:
  In Schematic: {total_in_schematic:,} | Purchased: {purchased:,} | Gaps: {total_gaps:,} | Execution: {purchased_pct:.1f}%

SALESPERSON EXECUTION (sorted by most gaps):
{sp_text}

GAP HISTORY — last 4 periods (use to detect trends):
{hist_text}

CHAIN SUMMARY:
{chain_text}{dup_section}

Return ONLY a valid JSON array — no markdown, no explanation, just the JSON:
[
  {{"severity": "warning",  "text": "specific insight with real name and numbers"}},
  {{"severity": "info",     "text": "neutral observation with real name and numbers"}},
  {{"severity": "positive", "text": "positive trend with real name and numbers"}}
]

Rules:
- Maximum {_MAX_INSIGHTS} insights, ordered by importance
- Every insight MUST name a specific salesperson, chain, or metric — no generic observations
- Detect: execution drops vs prior period, multi-period worsening trends, chains with high gap rates vs volume, standout positive performers
- DATA INTEGRITY warnings (if any) must appear first in the array
- severity must be exactly: "warning", "info", or "positive"
- If data is insufficient for meaningful insights, return []
"""


def get_home_insights(
    execution_summary: tuple,
    salesperson_df: pd.DataFrame,
    gap_history_df: pd.DataFrame,
    chain_df: pd.DataFrame,
    duplicate_stores: list[dict] | None = None,
) -> list[dict]:
    """
    Call Claude to surface anomalies from home page data.
    Returns list of {severity, text} dicts.
    """
    client = anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])
    prompt = _build_prompt(
        execution_summary, salesperson_df, gap_history_df, chain_df,
        duplicate_stores=duplicate_stores or [],
    )

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        insights = json.loads(raw.strip())
        if not isinstance(insights, list):
            return []

        return [
            {"severity": i.get("severity", "info"), "text": i["text"]}
            for i in insights
            if isinstance(i, dict) and "text" in i
        ]

    except Exception as e:
        return [{"severity": "info", "text": f"Analysis unavailable — {e}"}]


def generate_salesperson_coaching(
    salesperson_name: str,
    sp_df: pd.DataFrame,
    execution_df: pd.DataFrame | None = None,
    api_key: str = "",
) -> str:
    """
    Generate a 2-3 sentence personalized coaching message for one salesperson's gap email.
    Returns plain text, or "" on failure.
    Uses Haiku for speed and cost efficiency (called once per salesperson per send).
    """
    if not api_key or sp_df.empty:
        return ""

    try:
        streaks = pd.to_numeric(sp_df.get("STREAK_WEEKS", pd.Series(dtype=float)), errors="coerce").fillna(1)
        active_gaps = len(sp_df)
        new_this_week = int((streaks == 1).sum())
        two_three = int(streaks.isin([2, 3]).sum())
        four_plus = int((streaks >= 4).sum())

        top_chains = (
            sp_df["CHAIN_NAME"].dropna().value_counts().head(3).to_dict()
            if "CHAIN_NAME" in sp_df.columns else {}
        )
        chains_str = ", ".join(f"{k} ({v})" for k, v in top_chains.items()) or "unknown"

        exec_line = ""
        if execution_df is not None and not execution_df.empty:
            r = execution_df.iloc[0]
            pct = r.get("PCT_EXECUTION", "")
            gaps_away = r.get("GAPS_AWAY_FROM_90", "")
            exec_line = f"\nExecution: {pct}% | Gaps away from 90%: {gaps_away}"

        prompt = f"""You are writing a brief coaching note inside a weekly gap report email sent to a salesperson.

Salesperson: {salesperson_name}
Active gaps: {active_gaps} | New this week: {new_this_week} | 2-3 weeks: {two_three} | 4+ weeks: {four_plus}
Top chains with gaps: {chains_str}{exec_line}

Write exactly 2-3 sentences of direct, specific, motivating coaching.
Rules:
- Reference their actual numbers (gaps, chains, weeks)
- Tell them what to prioritize first (oldest gaps)
- Be encouraging but direct — no fluff
- Do NOT use a greeting or sign-off
- Plain text only, no bullet points, no markdown
"""

        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=_COACHING_MODEL,
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()

    except Exception:
        return ""
