# -------------- ai_placement_intelegence.py --------------

import streamlit as st
import pandas as pd
#from utils.snowflake_utils import fetch_distinct_values
from utils.ai_placement_helpers import get_current_and_archived_distro, compare_current_vs_archived, summarize_placement_diffs, generate_ai_summary_text, fetch_distinct_values
from sf_connector.service_connector import connect_to_tenant_snowflake
from openai import OpenAI

# Load OpenAI key
OPENAI_API_KEY = st.secrets["openai"]["api_key"]



# ----------------------------------------
# 🧵 Placement Intelligence: AI-Powered Analytics
# ----------------------------------------
def render():
    st.title("🧠 Placement Intelligence")
    st.markdown("Gain AI insights on item placements by comparing your current grid vs. archived seasons.")
    
    # ⚖️ Load context
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("Tenant configuration missing.")
        return

    conn = connect_to_tenant_snowflake(toml_info)

    # 🔢 Chain selector
    chains = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
    chain = st.selectbox("Select Chain", chains, index=0 if chains else None)

    # 🌾 Season selector (from archive tracking table)
    seasons = fetch_distinct_values(conn, "DG_ARCHIVE_TRACKING", "SEASON", filters=f"CHAIN_NAME = '{chain}'") if chain else []
    season = st.selectbox("Compare Against Archived Season", seasons, index=0 if seasons else None)

    # ⏰ Action buttons
    col1, col2 = st.columns([1, 1])
    with col1:
        run_compare = st.button("🔍 Run Placement Comparison")
    with col2:
        run_ai = st.button("🤖 Generate AI Summary")

    # 📊 Comparison output
    if run_compare and chain and season:
        with st.spinner("Comparing DISTRO_GRID vs ARCHIVE ..."):
            st.subheader("🔁 Comparison Results")

            # Run the actual comparison
            new_df, removed_df = compare_current_vs_archived(conn, chain, season)

            # st.write("🔍 new_df sample:", new_df.head())
            # st.write("🔍 removed_df sample:", removed_df.head())

            # Save for AI use
            st.session_state["last_comparison"] = {
                "new": new_df,
                "removed": removed_df,
                "summary": summarize_placement_diffs(new_df, removed_df)
            }

            # Group by Manufacturer
            new_by_mfg = new_df.groupby("MANUFACTURER").size().reset_index(name="New Placements")
            removed_by_mfg = removed_df.groupby("MANUFACTURER").size().reset_index(name="Removed Placements")

            st.markdown("### 🆕 New Placements by Manufacturer")
            st.dataframe(new_by_mfg)

            st.markdown("### ❌ Removed Placements by Manufacturer")
            st.dataframe(removed_by_mfg)



       # 🧐 AI Output
    if run_ai and chain and season:
        with st.spinner("Talking to the AI..."):
            st.subheader("📜 AI Narrative Summary")

            # 🔁 Pull previous comparison data
            new_df = st.session_state.get("new_df")
            removed_df = st.session_state.get("removed_df")

            if new_df is not None and removed_df is not None:
                ai_summary = generate_ai_summary_text(new_df, removed_df, chain, season)
                st.markdown("### 📜 AI Summary")
                st.markdown(ai_summary)
            else:
                st.warning("Please run the comparison first before generating an AI summary.")

            st.text_input("Ask a follow-up question:", key="followup_question")


    conn.close()
