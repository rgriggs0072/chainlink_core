# -------------- ai_placement_intelligence.py --------------
"""
Placement Intelligence Page

Overview:
- Compares current DISTRO_GRID vs an archived season for a selected chain.
- Shows new and removed placements grouped by manufacturer.
- Generates an AI narrative summary via OpenAI GPT-4.
- Follow-up Q&A wired to the same AI context.

Fix history:
- 2026-03-25: Rewrote to use session_state for comparison results so
  "Run Comparison" and "Generate AI Summary" work as independent actions
  without wiping each other's state on rerun. Added follow-up Q&A wiring.
  Fixed connection leak (use st.session_state["conn"] instead of opening
  a new connection on every render).
"""

import streamlit as st
import pandas as pd
from openai import OpenAI

from utils.ai_placement_helpers import (
    compare_current_vs_archived,
    summarize_placement_diffs,
    generate_ai_summary_text,
    fetch_distinct_values,
)
from sf_connector.service_connector import connect_to_tenant_snowflake

# OpenAI client
OPENAI_API_KEY = st.secrets["openai"]["api_key"]
client = OpenAI(api_key=OPENAI_API_KEY)


def render():
    st.title("Placement Intelligence")
    st.markdown(
        "Compare your current distribution grid against an archived season "
        "to identify new and removed placements — with an AI-powered summary."
    )

    # ── Tenant / connection guard ──────────────────────────────────────────
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("Tenant configuration missing. Please log in again.")
        return

    # Open a fresh connection for pd.read_sql
    try:
        conn = connect_to_tenant_snowflake(toml_info)
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return
    # REMOVED: shared conn silently fails with pd.read_sql
    if not conn:
        st.error("Database connection not available. Please log in again.")
        return

    # ── Selectors ─────────────────────────────────────────────────────────
    try:
        chains = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
    except Exception as e:
        st.error(f"Could not load chain list: {e}")
        return

    if not chains:
        st.warning("No chains found in CUSTOMERS table.")
        return

    chain = st.selectbox("Select Chain", chains)

    try:
        seasons = fetch_distinct_values(
            conn,
            "DG_ARCHIVE_TRACKING",
            "SEASON",
            filters=f"CHAIN_NAME = '{chain}'"
        ) if chain else []
    except Exception as e:
        st.error(f"Could not load seasons: {e}")
        return

    if not seasons:
        st.warning(f"No archived seasons found for chain '{chain}'. Upload and archive a distro grid first.")
        return

    season = st.selectbox("Compare Against Archived Season", seasons)

    st.markdown("---")

    # ── Step 1: Run Comparison ─────────────────────────────────────────────
    if st.button("▶ Run Placement Comparison", type="primary"):
        with st.spinner("Comparing current DISTRO_GRID vs archive..."):
            try:
                new_df, removed_df = compare_current_vs_archived(conn, chain, season)
                summary = summarize_placement_diffs(new_df, removed_df)

                # Store results in session state so AI step can access them
                st.session_state["placement_comparison"] = {
                    "new": new_df,
                    "removed": removed_df,
                    "summary": summary,
                    "chain": chain,
                    "season": season,
                }
                # Clear any prior AI output when a new comparison runs
                st.session_state.pop("placement_ai_summary", None)
                st.session_state.pop("placement_followup_history", None)

            except Exception as e:
                st.error(f"Comparison failed: {e}")
                return

    # ── Show comparison results (persists across reruns) ───────────────────
    comparison = st.session_state.get("placement_comparison")

    if comparison and comparison.get("chain") == chain and comparison.get("season") == season:
        new_df = comparison["new"]
        removed_df = comparison["removed"]
        summary = comparison["summary"]

        col1, col2, col3 = st.columns(3)
        col1.metric("New Placements", summary["new_count"])
        col2.metric("Removed Placements", summary["removed_count"])
        col3.metric("Net Change", f"{summary['net_change']:+d}")

        st.markdown("---")

        tab1, tab2 = st.tabs(["🟢 New Placements", "🔴 Removed Placements"])

        with tab1:
            if new_df.empty:
                st.info("No new placements detected.")
            else:
                new_by_mfg = (
                    new_df.groupby("MANUFACTURER")
                    .size()
                    .reset_index(name="New Placements")
                    .sort_values("New Placements", ascending=False)
                )
                st.dataframe(new_by_mfg, use_container_width=True)
                with st.expander("View full new placements detail"):
                    st.dataframe(new_df, use_container_width=True)

        with tab2:
            if removed_df.empty:
                st.info("No removed placements detected.")
            else:
                removed_by_mfg = (
                    removed_df.groupby("MANUFACTURER")
                    .size()
                    .reset_index(name="Removed Placements")
                    .sort_values("Removed Placements", ascending=False)
                )
                st.dataframe(removed_by_mfg, use_container_width=True)
                with st.expander("View full removed placements detail"):
                    st.dataframe(removed_df, use_container_width=True)

        st.markdown("---")

        # ── Step 2: Generate AI Summary ────────────────────────────────────
        if st.button("🤖 Generate AI Summary", type="primary"):
            with st.spinner("Generating AI narrative summary..."):
                try:
                    ai_text = generate_ai_summary_text(new_df, removed_df, chain, season)
                    st.session_state["placement_ai_summary"] = ai_text
                    # Build manufacturer breakdown for follow-up context
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
                    # Seed follow-up conversation history with full context
                    st.session_state["placement_followup_history"] = [
                        {"role": "system", "content": "You are a retail analytics assistant helping analyze placement changes in a distribution grid. You have access to the full manufacturer breakdown data."},
                        {"role": "user", "content": f"Analyze placement changes for chain '{chain}' vs season '{season}': {summary['new_count']} new, {summary['removed_count']} removed.\n\nNew by manufacturer:\n{new_by_mfg}\n\nRemoved by manufacturer:\n{removed_by_mfg}"},
                        {"role": "assistant", "content": ai_text},
                    ]
                except Exception as e:
                    st.error(f"AI summary failed: {e}")

        # ── Show AI summary (persists across reruns) ───────────────────────
        ai_summary = st.session_state.get("placement_ai_summary")
        if ai_summary:
            st.subheader("🤖 AI Narrative Summary")
            st.markdown(ai_summary)

            st.markdown("---")

            # ── Step 3: Follow-up Q&A ──────────────────────────────────────
            st.subheader("💬 Ask a Follow-up Question")

            with st.form("placement_followup_form"):
                followup = st.text_input(
                    "Ask the AI a follow-up question about these placements:",
                    placeholder="e.g. Which manufacturer lost the most placements?"
                )
                ask = st.form_submit_button("Ask")

            if ask and followup:
                history = st.session_state.get("placement_followup_history", [])
                history.append({"role": "user", "content": followup})

                with st.spinner("Thinking..."):
                    try:
                        response = client.chat.completions.create(
                            model="gpt-4",
                            messages=history,
                            temperature=0.7,
                            max_tokens=400,
                        )
                        answer = response.choices[0].message.content.strip()
                        history.append({"role": "assistant", "content": answer})
                        st.session_state["placement_followup_history"] = history
                    except Exception as e:
                        st.error(f"Follow-up failed: {e}")

            # Display follow-up conversation (skip system + first 2 seeding messages)
            history = st.session_state.get("placement_followup_history", [])
            followup_turns = history[3:]  # skip system prompt + initial Q&A seed
            for msg in followup_turns:
                if msg["role"] == "user":
                    st.markdown(f"**You:** {msg['content']}")
                elif msg["role"] == "assistant":
                    st.markdown(f"**AI:** {msg['content']}")

    elif not comparison:
        st.info("Select a chain and season above, then click **Run Placement Comparison** to get started.")
