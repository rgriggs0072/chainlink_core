# app_pages/ai_chat.py
"""
Persistent AI Chat Page — Claude with direct Snowflake tool access.
Available to all authenticated users. Conversation persists for the session.
"""
import streamlit as st
from utils.ai_chat import run_chat_turn
from utils.snowflake_utils import fetch_distinct_values


def render() -> None:
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if not conn or not tenant_id:
        st.error("Missing tenant connection. Please log in again.")
        return

    # ── Session state init (once per session, not every rerun) ────────────────
    if "ai_chat_api_messages" not in st.session_state:
        st.session_state["ai_chat_api_messages"] = []
    if "ai_chat_display" not in st.session_state:
        st.session_state["ai_chat_display"] = []
    if "ai_chat_chain_names" not in st.session_state:
        try:
            st.session_state["ai_chat_chain_names"] = fetch_distinct_values(
                conn, "CUSTOMERS", "CHAIN_NAME"
            )
        except Exception:
            st.session_state["ai_chat_chain_names"] = []

    chain_names: list[str] = st.session_state["ai_chat_chain_names"]
    display_msgs: list[dict] = st.session_state["ai_chat_display"]
    api_msgs: list[dict] = st.session_state["ai_chat_api_messages"]

    # ── Header ─────────────────────────────────────────────────────────────────
    col_title, col_clear = st.columns([5, 1])
    with col_title:
        st.markdown("## Claude")
        st.markdown(
            "<p style='color:#64748B;margin-top:-0.5rem;font-size:0.9rem;'>"
            "Ask anything about your data. Claude queries Snowflake directly."
            "</p>",
            unsafe_allow_html=True,
        )
    with col_clear:
        st.markdown("<div style='padding-top:1.75rem;'>", unsafe_allow_html=True)
        if st.button("Clear", type="secondary", width='stretch'):
            st.session_state["ai_chat_api_messages"] = []
            st.session_state["ai_chat_display"] = []
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Render existing messages ───────────────────────────────────────────────
    for msg in display_msgs:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            for df in msg.get("frames", []):
                st.dataframe(df, width='stretch')

    # ── Handle new input ───────────────────────────────────────────────────────
    if prompt := st.chat_input("Ask about your data..."):
        display_msgs.append({"role": "user", "content": prompt, "frames": []})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Claude is thinking..."):
                try:
                    text, frames = run_chat_turn(
                        user_message=prompt,
                        api_messages=api_msgs,
                        conn=conn,
                        tenant_id=tenant_id,
                        chain_names=chain_names,
                    )
                except Exception as e:
                    text = f"Something went wrong: {e}"
                    frames = []

            st.markdown(text)
            for df in frames:
                st.dataframe(df, width='stretch')

        display_msgs.append({"role": "assistant", "content": text, "frames": frames})
