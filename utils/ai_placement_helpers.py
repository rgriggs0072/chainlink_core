# -------------- ai_placement_helpers.py --------------
import streamlit as st
import pandas as pd
from openai import OpenAI
import os


client = OpenAI(api_key=st.secrets["openai"]["api_key"])


def get_current_and_archived_distro(conn, chain, season):
    # Step 1: Look up the archived timestamp
    archive_query = f"""
        SELECT ARCHIVED_AT
        FROM DG_ARCHIVE_TRACKING
        WHERE CHAIN_NAME = '{chain}' AND SEASON = '{season}'
        LIMIT 1
    """
    archive_df = pd.read_sql(archive_query, conn)
    if archive_df.empty:
        raise ValueError(f"No archive found for chain '{chain}' and season '{season}'")

    archive_ts = archive_df["ARCHIVED_AT"].iloc[0]

    # Step 2: Fetch current and archived distro data
    current_query = f"""
        SELECT STORE_NUMBER, UPC, PRODUCT_NAME, SEGMENT, MANUFACTURER, CUSTOMER_ID, PRODUCT_ID
        FROM DISTRO_GRID
        WHERE CHAIN_NAME = '{chain}'
    """
    df_current = pd.read_sql(current_query, conn)

    archived_query = f"""
        SELECT STORE_NUMBER, UPC, PRODUCT_NAME, SEGMENT, MANUFACTURER, CUSTOMER_ID, PRODUCT_ID
        FROM DISTRO_GRID_ARCHIVE
        WHERE CHAIN_NAME = '{chain}' AND ARCHIVE_DATE = '{archive_ts}'
    """
    df_archive = pd.read_sql(archived_query, conn)

    # ✅ Normalize keys for join compatibility
    df_current["UPC"] = df_current["UPC"].astype(str).str.zfill(11)
    df_archive["UPC"] = df_archive["UPC"].astype(str).str.zfill(11)

    df_current["STORE_NUMBER"] = df_current["STORE_NUMBER"].astype(str)
    df_archive["STORE_NUMBER"] = df_archive["STORE_NUMBER"].astype(str)

    # ✅ Filter to valid tracked products
    df_current = df_current[(df_current["CUSTOMER_ID"].notnull()) & (df_current["PRODUCT_ID"] != 0)]
    df_archive = df_archive[(df_archive["CUSTOMER_ID"].notnull()) & (df_archive["PRODUCT_ID"] != 0)]

    return df_current, df_archive



def compare_current_vs_archived(conn, chain, season):
    df_current, df_archive = get_current_and_archived_distro(conn, chain, season)

    # 🧼 Normalize types and strip whitespace
    for df in [df_current, df_archive]:
        df["STORE_NUMBER"] = df["STORE_NUMBER"].astype(str).str.strip()
        df["UPC"] = df["UPC"].astype(str).str.strip()

    # 🧪 Debug intersection
    current_keys = set(zip(df_current["STORE_NUMBER"], df_current["UPC"]))
    archive_keys = set(zip(df_archive["STORE_NUMBER"], df_archive["UPC"]))
    common_keys = current_keys & archive_keys

    # st.write(f"🔍 Common keys found: {len(common_keys)}")
    # st.write("🔍 current_keys sample:", list(current_keys)[:5])
    # st.write("🔍 archive_keys sample:", list(archive_keys)[:5])

    # 🔍 Determine new and removed
    new_keys = current_keys - archive_keys
    removed_keys = archive_keys - current_keys

    new_df = df_current[df_current.set_index(["STORE_NUMBER", "UPC"]).index.isin(new_keys)]
    removed_df = df_archive[df_archive.set_index(["STORE_NUMBER", "UPC"]).index.isin(removed_keys)]
    st.session_state["new_df"] = new_df
    st.session_state["removed_df"] = removed_df

    # ✅ Display removed placements if available
    if removed_df.empty:
        st.warning("No removed placements detected.")
    else:
        st.success(f"❌ Removed Placements: {len(removed_df)}")
        st.dataframe(removed_df.head(25))  # Display first 25 for clarity


    return new_df, removed_df


def summarize_placement_diffs(df_new, df_removed):
    """
    Summarize net changes in placements.
    """
    summary = {
        "new_count": len(df_new),
        "removed_count": len(df_removed),
        "net_change": len(df_new) - len(df_removed)
    }
    return summary


def generate_ai_summary_text(new_df, removed_df, chain, season):
    new_count = len(new_df)
    removed_count = len(removed_df)
    sample_new = new_df["PRODUCT_NAME"].dropna().unique().tolist()[:5]
    sample_removed = removed_df["PRODUCT_NAME"].dropna().unique().tolist()[:5]

    prompt = f"""
You are an expert retail data analyst. A placement change analysis has been run for chain '{chain}' comparing the current distro grid to the archived season '{season}'.

There are {new_count} new product placements and {removed_count} removed placements.

Top examples of new products: {sample_new}
Top examples of removed products: {sample_removed}

Please summarize the most meaningful insights from this comparison.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a retail analytics assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=400
        )

        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"⚠️ AI failed:\n\n{e}"


def fetch_distinct_values(conn, table, column, filters=None):
    """
    Returns a list of distinct values from a column, optionally with a WHERE clause.
    """
    query = f"SELECT DISTINCT {column} FROM {table}"
    if filters:
        query += f" WHERE {filters}"
    query += f" ORDER BY {column}"

    return pd.read_sql(query, conn)[column].dropna().tolist()
