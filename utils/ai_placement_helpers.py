# -------------- ai_placement_helpers.py --------------
import streamlit as st
import pandas as pd
from openai import OpenAI
import os

client = OpenAI(api_key=st.secrets["openai"]["api_key"])


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


def get_current_and_archived_distro(conn, chain, season):
    chain_upper = chain.strip().upper()

    # Step 1: Look up archive date from tracking table
    archive_query = f"""
        SELECT ARCHIVED_AT::DATE AS ARCHIVE_DATE
        FROM DG_ARCHIVE_TRACKING
        WHERE UPPER(TRIM(CHAIN_NAME)) = '{chain_upper}' AND SEASON = '{season}'
        LIMIT 1
    """
    archive_df = pd.read_sql(archive_query, conn)
    if archive_df.empty:
        raise ValueError(f"No archive found for chain '{chain}' and season '{season}'")

    archive_date = pd.to_datetime(archive_df["ARCHIVE_DATE"].iloc[0]).date()

    # Step 2: Fetch current DISTRO_GRID filtered to products Delta carries.
    # Uses same 11-digit UPC key normalization as PROCESS_GAP_REPORT stored proc.
    current_query = f"""
        SELECT
            dg.STORE_NUMBER,
            dg.UPC,
            dg.PRODUCT_NAME,
            dg.SEGMENT,
            dg.MANUFACTURER,
            CASE
                WHEN LENGTH(TRIM(dg.UPC)) = 12 THEN LEFT(TRIM(dg.UPC), 11)
                WHEN LENGTH(TRIM(dg.UPC)) = 11 THEN TRIM(dg.UPC)
                ELSE NULL
            END AS UPC_KEY11
        FROM DISTRO_GRID dg
        WHERE UPPER(TRIM(dg.CHAIN_NAME)) = '{chain_upper}'
          AND dg.YES_NO = 1
          AND EXISTS (
            SELECT 1 FROM PRODUCTS p
            WHERE
                CASE
                    WHEN LENGTH(TRIM(p.CARRIER_UPC)) = 12 THEN LEFT(TRIM(p.CARRIER_UPC), 11)
                    WHEN LENGTH(TRIM(p.CARRIER_UPC)) = 11 THEN TRIM(p.CARRIER_UPC)
                    ELSE NULL
                END =
                CASE
                    WHEN LENGTH(TRIM(dg.UPC)) = 12 THEN LEFT(TRIM(dg.UPC), 11)
                    WHEN LENGTH(TRIM(dg.UPC)) = 11 THEN TRIM(dg.UPC)
                    ELSE NULL
                END
          )
    """
    df_current = pd.read_sql(current_query, conn)

    # Step 3: Fetch archived DISTRO_GRID with same product filter
    archived_query = f"""
        SELECT
            dga.STORE_NUMBER,
            dga.UPC,
            dga.PRODUCT_NAME,
            dga.SEGMENT,
            dga.MANUFACTURER,
            CASE
                WHEN LENGTH(TRIM(dga.UPC)) = 12 THEN LEFT(TRIM(dga.UPC), 11)
                WHEN LENGTH(TRIM(dga.UPC)) = 11 THEN TRIM(dga.UPC)
                ELSE NULL
            END AS UPC_KEY11
        FROM DISTRO_GRID_ARCHIVE dga
        WHERE UPPER(TRIM(dga.CHAIN_NAME)) = '{chain_upper}'
          AND dga.ARCHIVE_DATE = '{archive_date}'
          AND EXISTS (
            SELECT 1 FROM PRODUCTS p
            WHERE
                CASE
                    WHEN LENGTH(TRIM(p.CARRIER_UPC)) = 12 THEN LEFT(TRIM(p.CARRIER_UPC), 11)
                    WHEN LENGTH(TRIM(p.CARRIER_UPC)) = 11 THEN TRIM(p.CARRIER_UPC)
                    ELSE NULL
                END =
                CASE
                    WHEN LENGTH(TRIM(dga.UPC)) = 12 THEN LEFT(TRIM(dga.UPC), 11)
                    WHEN LENGTH(TRIM(dga.UPC)) = 11 THEN TRIM(dga.UPC)
                    ELSE NULL
                END
          )
    """
    df_archive = pd.read_sql(archived_query, conn)

    # Normalize STORE_NUMBER to string for key matching
    df_current["STORE_NUMBER"] = df_current["STORE_NUMBER"].astype(str).str.strip()
    df_archive["STORE_NUMBER"] = df_archive["STORE_NUMBER"].astype(str).str.strip()

    return df_current, df_archive


def compare_current_vs_archived(conn, chain, season):
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

    new_df = df_current[df_current["_KEY"].isin(current_keys - archive_keys)].drop(columns=["_KEY"])
    removed_df = df_archive[df_archive["_KEY"].isin(archive_keys - current_keys)].drop(columns=["_KEY"])

    st.session_state["new_df"] = new_df
    st.session_state["removed_df"] = removed_df

    return new_df, removed_df


def summarize_placement_diffs(df_new, df_removed):
    """Summarize net changes in placements."""
    return {
        "new_count": len(df_new),
        "removed_count": len(df_removed),
        "net_change": len(df_new) - len(df_removed),
    }


def generate_ai_summary_text(new_df, removed_df, chain, season):
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
        return f"⚠️ AI failed:\n\n{e}"


def fetch_distinct_values(conn, table, column, filters=None):
    """Returns a list of distinct values from a column, optionally with a WHERE clause."""
    query = f"SELECT DISTINCT {column} FROM {table}"
    if filters:
        query += f" WHERE {filters}"
    query += f" ORDER BY {column}"
    return pd.read_sql(query, conn)[column].dropna().tolist()
