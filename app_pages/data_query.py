# app_pages/data_query.py
"""
AI-Powered Data Query Page (Admin Only)

Overview:
- Allows admins to query Snowflake tables using plain English.
- Claude API generates a safe SELECT query from the natural language input.
- Chain names are loaded dynamically from CUSTOMERS at page load so the AI
  always knows the exact values — no hardcoding needed.
- Query is validated (SELECT only, allowed tables only) before execution.
- Results shown in st.dataframe with SQL visible in an expander.
- Hard LIMIT 500 injected automatically to prevent runaway queries.

Allowed tables: CUSTOMERS, DISTRO_GRID, RESET_SCHEDULE,
                SALES_REPORT, PRODUCTS, SUPPLIER_COUNTY
"""

import re
import time
import streamlit as st
import pandas as pd
import anthropic
from sf_connector.service_connector import connect_to_tenant_snowflake
from utils.snowflake_utils import fetch_distinct_values

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

ALLOWED_TABLES = {
    "CUSTOMERS",
    "DISTRO_GRID",
    "RESET_SCHEDULE",
    "SALES_REPORT",
    "PRODUCTS",
    "SUPPLIER_COUNTY",
}

ROW_LIMIT = 500

EXAMPLE_QUESTIONS = [
    "Show me all stores in FOODMAXX with their salesperson and county",
    "How many stores does each chain have?",
    "Show me all products from 2 TOWNS CIDERHOUSE",
    "Which salesperson covers the most stores?",
    "Show me all reset schedules for LUCKY in April 2026",
    "Which suppliers are active in San Joaquin county?",
    "Show me all distro grid items for RALEYS where YES_NO is 1",
]


# ─────────────────────────────────────────────────────────────────────────────
# Schema builder — dynamic, uses live chain names from Snowflake
# ─────────────────────────────────────────────────────────────────────────────

def _build_schema_context(chain_names: list[str]) -> str:
    """
    Build the schema context string injected into the AI system prompt.
    Chain names are loaded live from CUSTOMERS so the AI always knows
    the exact values stored in Snowflake — no hardcoding required.
    """
    chains_str = ", ".join(f"'{c}'" for c in sorted(chain_names)) if chain_names else "unknown"

    return f"""
You have access to the following Snowflake tables. Use EXACT column names as shown.

CUSTOMERS:
  CUSTOMER_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, ADDRESS, CITY, STATE,
  COUNTY, SALESPERSON, PHONE_NUMBER
  IMPORTANT — the exact CHAIN_NAME values in this database are: {chains_str}
  Always match chain names exactly as listed above.

DISTRO_GRID:
  DISTRO_GRID_ID, CHAIN_NAME, STORE_NAME, STORE_NUMBER, COUNTY, UPC,
  SKU, PRODUCT_NAME, MANUFACTURER, SEGMENT, YES_NO, ACTIVATION_STATUS,
  LAST_LOAD_DATE

RESET_SCHEDULE:
  RESET_SCHEDULE_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, PHONE_NUMBER,
  CITY, ADDRESS, STATE, COUNTY, TEAM_LEAD, RESET_DATE, RESET_TIME,
  STATUS, NOTES

SALES_REPORT:
  STORE_NUMBER, STORE_NAME, CHAIN_NAME, UPC, PRODUCT_NAME, SALESPERSON,
  PURCHASED_YES_NO, COUNTY

PRODUCTS:
  PRODUCT_ID, SUPPLIER, PRODUCT_NAME, PACKAGE, CARRIER_UPC, PRODUCT_MANAGER

SUPPLIER_COUNTY:
  SUPPLIER, COUNTY
"""


# ─────────────────────────────────────────────────────────────────────────────
# SQL generation
# ─────────────────────────────────────────────────────────────────────────────

def _generate_sql(question: str, schema_context: str) -> str:
    """
    Call Claude API to generate a Snowflake SQL SELECT query.
    Retries up to 3 times on API overload (529) with exponential backoff.
    """
    client = anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])

    system_prompt = f"""You are a Snowflake SQL expert. Generate a single valid Snowflake SQL SELECT query based on the user's question.

STRICT RULES:
1. Only generate SELECT statements — never INSERT, UPDATE, DELETE, DROP, CREATE, or any DDL/DML.
2. Only query these tables: {', '.join(sorted(ALLOWED_TABLES))}
3. Do NOT include a LIMIT clause — it will be added automatically.
4. Always use UPPER(TRIM()) on both sides of ALL string comparisons, e.g.: WHERE UPPER(TRIM(CHAIN_NAME)) = UPPER(TRIM('foodmaxx')). All string data is stored uppercase in Snowflake.
5. Return ONLY the raw SQL query with no explanation, no markdown, no backticks, no preamble.
6. If the question cannot be answered with the available tables, respond with exactly: CANNOT_ANSWER

{schema_context}"""

    for attempt in range(3):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": question}],
                system=system_prompt,
            )
            return message.content[0].text.strip()
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < 2:
                time.sleep(2 ** attempt)  # 1s then 2s backoff
                continue
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Validation + safety
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate that the generated SQL is safe to run.
    Returns (is_valid, error_message).
    """
    sql_upper = sql.upper().strip()

    if not sql_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed."

    dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
                 "TRUNCATE", "EXECUTE", "EXEC", "CALL", "GRANT", "REVOKE"]
    for keyword in dangerous:
        if re.search(rf'\b{keyword}\b', sql_upper):
            return False, f"Query contains disallowed keyword: {keyword}"

    table_refs = re.findall(r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)', sql_upper)
    for table in table_refs:
        if table not in ALLOWED_TABLES:
            return False, f"Table '{table}' is not in the allowed list."

    return True, ""


def _inject_limit(sql: str, limit: int = ROW_LIMIT) -> str:
    """Inject LIMIT clause if not already present."""
    if re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
        return sql
    return f"{sql.rstrip(';').rstrip()}\nLIMIT {limit}"


def _run_query(sql: str) -> pd.DataFrame:
    """Execute SQL against tenant Snowflake and return results as DataFrame."""
    toml_info = st.session_state.get("toml_info")
    conn = connect_to_tenant_snowflake(toml_info)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Page
# ─────────────────────────────────────────────────────────────────────────────

def render():
    if not st.session_state.get("is_admin"):
        st.warning("You don't have access to this page.")
        return

    st.title("🔍 Data Query")
    st.markdown(
        "Ask a question about your data in plain English and get instant results. "
        "Queries are read-only and limited to 500 rows."
    )

    # ── Load chain names dynamically so AI always knows exact values ──────────
    conn = st.session_state.get("conn")
    try:
        chain_names = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
    except Exception:
        chain_names = []

    schema_context = _build_schema_context(chain_names)

    # ── Example questions ─────────────────────────────────────────────────────
    with st.expander("💡 Example questions", expanded=False):
        for q in EXAMPLE_QUESTIONS:
            if st.button(q, key=f"example_{q[:30]}"):
                st.session_state["dq_question"] = q
                # Clear previous results when a new example is selected
                st.session_state.pop("dq_result", None)
                st.session_state.pop("dq_sql", None)
                st.rerun()

    # ── Question input ────────────────────────────────────────────────────────
    question = st.text_input(
        "Ask a question about your data:",
        value=st.session_state.get("dq_question", ""),
        placeholder="e.g. How many stores does each chain have?",
        key="dq_input",
    )

    col1, col2 = st.columns([1, 6])
    with col1:
        run = st.button("Run Query", type="primary")
    with col2:
        if st.button("Clear", type="secondary"):
            for key in ["dq_question", "dq_result", "dq_sql"]:
                st.session_state.pop(key, None)
            st.rerun()

    # ── Generate + run ────────────────────────────────────────────────────────
    if run and question.strip():
        st.session_state["dq_question"] = question

        # Always clear previous results before generating a new query
        # so stale data never shows if the new query fails or returns different results
        st.session_state.pop("dq_result", None)
        st.session_state.pop("dq_sql", None)

        with st.spinner("Generating query..."):
            try:
                sql = _generate_sql(question, schema_context)
            except anthropic.APIStatusError as e:
                if e.status_code == 529:
                    st.warning(
                        "⏳ The AI service is currently busy. Please wait a moment and try again."
                    )
                else:
                    st.error(f"❌ AI service error: {e.message}")
                return
            except Exception as e:
                st.error(f"❌ Failed to generate query: {e}")
                return

        if sql == "CANNOT_ANSWER":
            st.warning(
                "I couldn't find a way to answer that with the available data. "
                "Try rephrasing or pick one of the example questions for ideas."
            )
            return

        is_valid, error = _validate_sql(sql)
        if not is_valid:
            st.error(f"❌ Query validation failed: {error}")
            return

        sql_limited = _inject_limit(sql)
        st.session_state["dq_sql"] = sql_limited

        with st.spinner("Running query..."):
            try:
                df = _run_query(sql_limited)
                st.session_state["dq_result"] = df
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
                return

    # ── Results ───────────────────────────────────────────────────────────────
    df = st.session_state.get("dq_result")
    sql_used = st.session_state.get("dq_sql")

    if df is not None:
        if df.empty:
            st.info("Query ran successfully but returned no results.")
        else:
            limited = len(df) == ROW_LIMIT
            st.success(
                f"**{len(df):,} row(s)** returned"
                f"{' — results limited to 500 rows' if limited else ''}."
            )
            st.dataframe(df, use_container_width=True)

            st.download_button(
                "⬇️ Download Results as CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="query_results.csv",
                mime="text/csv",
            )

        if sql_used:
            with st.expander("🔎 View generated SQL", expanded=False):
                st.code(sql_used, language="sql")
