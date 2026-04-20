# -------------- ai_placement_helpers.py --------------
"""
AI Placement Intelligence helpers

Overview for future devs:
- Backend support for the Placement Intelligence workflow:
  * get_current_and_archived_distro(): fetches current DISTRO_GRID and
    archived DISTRO_GRID_MATCHED_ARCHIVE rows for a given chain/season,
    applying the three-way Delta Pacific filter to both sides.
  * compare_current_vs_archived(): computes new and removed placements
    using set operations on (STORE_NUMBER, UPC_KEY11) tuples.
  * summarize_placement_diffs(): returns net change counts.
  * generate_ai_summary_text(): sends placement diff data to GPT-4 for
    a human-readable executive summary.
  * fetch_distinct_values(): generic helper for populating dropdowns.

v1.2.0 changes:
  - Archive source changed from DISTRO_GRID_ARCHIVE to DISTRO_GRID_MATCHED_ARCHIVE.
  - Three-way filter (PRODUCT_ID, COUNTY, SUPPLIER_COUNTY) now applied to
    both the current grid query and the archive query for apples-to-apples comparison.
  - Archive date lookup updated to use FULL_ARCHIVED_AT instead of ARCHIVED_AT
    following DG_ARCHIVE_TRACKING column rename.

Notes:
- All Streamlit UI (forms, selectboxes, layout) must live in
  app_pages/ai_placement_intelligence.py.
- This module may emit Streamlit messages (st.error, st.warning, etc.)
  but should not define pages or layout components.
"""

import streamlit as st
import pandas as pd
from openai import OpenAI
import os

client = OpenAI(api_key=st.secrets["openai"]["api_key"])


# ====================================================================================================================
# UPC normalization helper
# ====================================================================================================================

def _upc_key11(series: pd.Series) -> pd.Series:
    """
    Normalize UPC to 11-digit canonical key — same logic as PROCESS_GAP_REPORT:
    - 12 digits -> LEFT(UPC, 11)  (drop check digit)
    - 11 digits -> UPC as-is
    - anything else -> NaN (excluded from matching)
    """
    s = series.astype(str).str.strip()
    result = pd.Series(index=series.index, dtype="object")
    result[s.str.len() == 12] = s[s.str.len() == 12].str[:11]
    result[s.str.len() == 11] = s[s.str.len() == 11]
    return result


# ====================================================================================================================
# Core placement fetch
# ====================================================================================================================

def get_current_and_archived_distro(conn, chain, season):
    """
    Fetch current DISTRO_GRID and archived DISTRO_GRID_MATCHED_ARCHIVE rows
    for a given chain and season, applying the three-way Delta Pacific filter
    to both sides so the comparison is apples-to-apples.

    Three-way filter applied to both queries:
        1. PRODUCT_ID <> 0    — product exists in Delta Pacific catalog
        2. COUNTY is valid    — store is in a served territory
        3. SUPPLIER_COUNTY    — manufacturer is authorized for that county

    UPPER(TRIM()) wrapping on the manufacturer/county join is intentional —
    inconsistent casing between tables has caused missed matches before.

    Parameters:
        conn:   Active Snowflake connection.
        chain:  Chain name (e.g., 'SAFEWAY').
        season: Season label (e.g., 'Spring 2026').

    Returns:
        df_current: Current DISTRO_GRID rows passing the three-way filter.
        df_archive: Archived DISTRO_GRID_MATCHED_ARCHIVE rows for the season.
    """
    chain_upper = chain.strip().upper()

    # -----------------------------------------------------------------------
    # Step 1: Look up archive date from tracking table.
    # v1.2.0: Use FULL_ARCHIVED_AT instead of ARCHIVED_AT — the column was
    # renamed as part of the two-table archive restructure. FULL_ARCHIVED_AT
    # and MATCHED_ARCHIVED_AT are stamped together at upload time so either
    # could be used here; FULL_ARCHIVED_AT is used for consistency with the
    # original ARCHIVED_AT field it replaced.
    # -----------------------------------------------------------------------
    archive_query = f"""
        SELECT FULL_ARCHIVED_AT::DATE AS ARCHIVE_DATE
        FROM DG_ARCHIVE_TRACKING
        WHERE UPPER(TRIM(CHAIN_NAME)) = '{chain_upper}'
          AND SEASON = '{season}'
        LIMIT 1
    """
    archive_df = pd.read_sql(archive_query, conn)
    if archive_df.empty:
        raise ValueError(f"No archive found for chain '{chain}' and season '{season}'")

    archive_date = pd.to_datetime(archive_df["ARCHIVE_DATE"].iloc[0]).date()

    # -----------------------------------------------------------------------
    # Step 2: Fetch current DISTRO_GRID filtered to Delta Pacific placements.
    # The three-way filter is applied here via INNER JOIN on SUPPLIER_COUNTY
    # plus PRODUCT_ID and COUNTY checks. This replaces the old EXISTS subquery
    # against PRODUCTS which only checked product catalog membership — it did
    # not filter by territory or manufacturer authorization.
    # UPC normalization uses the same 11-digit key logic as PROCESS_GAP_REPORT.
    # -----------------------------------------------------------------------
    current_query = f"""
        SELECT
            dg.STORE_NUMBER,
            dg.UPC,
            dg.PRODUCT_NAME,
            dg.SEGMENT,
            dg.MANUFACTURER,
            dg.COUNTY,
            CASE
                WHEN LENGTH(TRIM(dg.UPC)) = 12 THEN LEFT(TRIM(dg.UPC), 11)
                WHEN LENGTH(TRIM(dg.UPC)) = 11 THEN TRIM(dg.UPC)
                ELSE NULL
            END AS UPC_KEY11
        FROM DISTRO_GRID dg
        INNER JOIN SUPPLIER_COUNTY sc
            ON UPPER(TRIM(sc.SUPPLIER)) = UPPER(TRIM(dg.MANUFACTURER))
            AND UPPER(TRIM(sc.COUNTY)) = UPPER(TRIM(dg.COUNTY))
            AND sc.STATUS = 'Yes'
            AND sc.TENANT_ID = dg.TENANT_ID
        WHERE UPPER(TRIM(dg.CHAIN_NAME)) = '{chain_upper}'
          AND dg.YES_NO = 1
          AND dg.PRODUCT_ID <> 0
          AND dg.COUNTY IS NOT NULL
          AND dg.COUNTY <> 'None'
    """
    df_current = pd.read_sql(current_query, conn)

    # -----------------------------------------------------------------------
    # Step 3: Fetch archived placements from DISTRO_GRID_MATCHED_ARCHIVE.
    # v1.2.0: Changed source from DISTRO_GRID_ARCHIVE to
    # DISTRO_GRID_MATCHED_ARCHIVE. The matched archive already contains only
    # filtered Delta Pacific placements (three-way filter was applied at
    # archive time), but we apply the SUPPLIER_COUNTY join again here for
    # safety — in case any territory authorizations changed since the archive
    # was written. PRODUCT_ID and COUNTY filters are also re-applied for the
    # same reason.
    # -----------------------------------------------------------------------
    archived_query = f"""
        SELECT
            dga.STORE_NUMBER,
            dga.UPC,
            dga.PRODUCT_NAME,
            dga.SEGMENT,
            dga.MANUFACTURER,
            dga.COUNTY,
            CASE
                WHEN LENGTH(TRIM(dga.UPC)) = 12 THEN LEFT(TRIM(dga.UPC), 11)
                WHEN LENGTH(TRIM(dga.UPC)) = 11 THEN TRIM(dga.UPC)
                ELSE NULL
            END AS UPC_KEY11
        FROM DISTRO_GRID_MATCHED_ARCHIVE dga
        INNER JOIN SUPPLIER_COUNTY sc
            ON UPPER(TRIM(sc.SUPPLIER)) = UPPER(TRIM(dga.MANUFACTURER))
            AND UPPER(TRIM(sc.COUNTY)) = UPPER(TRIM(dga.COUNTY))
            AND sc.STATUS = 'Yes'
            AND sc.TENANT_ID = dga.TENANT_ID
        WHERE UPPER(TRIM(dga.CHAIN_NAME)) = '{chain_upper}'
          AND dga.ARCHIVE_DATE = '{archive_date}'
          AND dga.PRODUCT_ID <> 0
          AND dga.COUNTY IS NOT NULL
          AND dga.COUNTY <> 'None'
    """
    df_archive = pd.read_sql(archived_query, conn)

    # Normalize STORE_NUMBER to string for key matching
    df_current["STORE_NUMBER"] = df_current["STORE_NUMBER"].astype(str).str.strip()
    df_archive["STORE_NUMBER"] = df_archive["STORE_NUMBER"].astype(str).str.strip()

    return df_current, df_archive


# ====================================================================================================================
# Placement comparison
# ====================================================================================================================

def compare_current_vs_archived(conn, chain, season):
    """
    Compare current DISTRO_GRID against the archived season snapshot and
    return DataFrames of new and removed placements.

    Uses (STORE_NUMBER, UPC_KEY11) tuple set operations — same approach as
    PROCESS_GAP_REPORT — to identify placements that appear in one snapshot
    but not the other.

    Parameters:
        conn:   Active Snowflake connection.
        chain:  Chain name (e.g., 'SAFEWAY').
        season: Season label to compare against (e.g., 'Fall 2025').

    Returns:
        new_df:     Placements in current grid but not in archive (gained).
        removed_df: Placements in archive but not in current grid (lost).
    """
    df_current, df_archive = get_current_and_archived_distro(conn, chain, season)

    current_upc_col = "UPC_KEY11" if "UPC_KEY11" in df_current.columns else "UPC"
    archive_upc_col = "UPC_KEY11" if "UPC_KEY11" in df_archive.columns else "UPC"

    df_current = df_current[df_current[current_upc_col].notnull()].copy()
    df_archive = df_archive[df_archive[archive_upc_col].notnull()].copy()

    # Build tuple key column for fast set operations — same approach as PROCESS_GAP_REPORT
    df_current["_KEY"] = list(zip(df_current["STORE_NUMBER"], df_current[current_upc_col]))
    df_archive["_KEY"] = list(zip(df_archive["STORE_NUMBER"], df_archive[archive_upc_col]))

    current_keys = set(df_current["_KEY"])
    archive_keys = set(df_archive["_KEY"])

    # New placements = in current but not in archive
    new_df = df_current[df_current["_KEY"].isin(current_keys - archive_keys)].drop(columns=["_KEY"])
    # Removed placements = in archive but not in current
    removed_df = df_archive[df_archive["_KEY"].isin(archive_keys - current_keys)].drop(columns=["_KEY"])

    # Store in session state so UI and AI summary can both access without re-querying
    st.session_state["new_df"] = new_df
    st.session_state["removed_df"] = removed_df

    return new_df, removed_df


# ====================================================================================================================
# Placement summary helpers
# ====================================================================================================================

def summarize_placement_diffs(df_new, df_removed):
    """
    Summarize net changes in placements.

    Returns:
        dict with new_count, removed_count, and net_change.
    """
    return {
        "new_count": len(df_new),
        "removed_count": len(df_removed),
        "net_change": len(df_new) - len(df_removed),
    }


def generate_ai_summary_text(new_df, removed_df, chain, season):
    """
    Send placement diff data to GPT-4 and return a human-readable executive
    summary for the sales team.

    Includes top 10 manufacturers by new and removed placements, plus sample
    product names, so the AI can answer follow-up questions accurately.

    Parameters:
        new_df:      DataFrame of new placements.
        removed_df:  DataFrame of removed placements.
        chain:       Chain name for context.
        season:      Season label for context.

    Returns:
        AI-generated summary string, or an error message if the call fails.
    """
    new_count = len(new_df)
    removed_count = len(removed_df)

    # Full manufacturer breakdown so AI can answer follow-up questions accurately
    new_by_mfg = (
        new_df.groupby("MANUFACTURER").size()
        .reset_index(name="New Placements")
        .sort_values("New Placements", ascending=False)
        .head(10)
        .to_string(index=False)
    )
    removed_by_mfg = (
        removed_df.groupby("MANUFACTURER").size()
        .reset_index(name="Removed Placements")
        .sort_values("Removed Placements", ascending=False)
        .head(10)
        .to_string(index=False)
    )
    sample_new = new_df["PRODUCT_NAME"].dropna().unique().tolist()[:5]
    sample_removed = removed_df["PRODUCT_NAME"].dropna().unique().tolist()[:5]

    prompt = f"""
You are an expert retail data analyst. A placement change analysis has been run for chain '{chain}' comparing the current distro grid to the archived season '{season}'.

Total new placements: {new_count}
Total removed placements: {removed_count}
Net change: {new_count - removed_count:+d}

New placements by manufacturer (top 10):
{new_by_mfg}

Removed placements by manufacturer (top 10):
{removed_by_mfg}

Sample new products: {sample_new}
Sample removed products: {sample_removed}

Please summarize the most meaningful insights from this comparison, including which manufacturers gained or lost the most placements and what actions the sales team should prioritize.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a retail analytics assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=400,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"⚠️ AI summary failed:\n\n{e}"


# ====================================================================================================================
# Generic dropdown helper
# ====================================================================================================================

def fetch_distinct_values(conn, table, column, filters=None):
    """
    Returns a list of distinct values from a column, optionally filtered.

    Used to populate UI dropdowns (e.g., chain selector, season selector).
    v1.2.0: Season dropdown should now point to DISTRO_GRID_MATCHED_ARCHIVE
    instead of DISTRO_GRID_ARCHIVE — callers should pass the correct table name.

    Parameters:
        conn:    Active Snowflake connection.
        table:   Table name to query.
        column:  Column to return distinct values from.
        filters: Optional WHERE clause string (e.g., "CHAIN_NAME = 'SAFEWAY'").

    Returns:
        List of distinct non-null values sorted ascending.
    """
    query = f"SELECT DISTINCT {column} FROM {table}"
    if filters:
        query += f" WHERE {filters}"
    query += f" ORDER BY {column}"
    return pd.read_sql(query, conn)[column].dropna().tolist()