# app_pages/data_query.py
"""
AI-Powered Data Query Page (Admin Only)

Overview:
- Allows admins to query Snowflake tables using plain English.
- Claude API generates a safe SELECT query from the natural language input.
- Chain names are loaded dynamically from CUSTOMERS at page load so the AI
  always knows the exact values — no hardcoding needed.
- Query is validated (SELECT only, allowed tables only, TENANT_ID required)
  before execution.
- Row count is checked before fetching — warns if results exceed safety cap.
- Results shown in st.dataframe with SQL visible in an expander.
- Safety cap of 200K rows prevents runaway queries on large tables.
- Always uses cached st.session_state["conn"] — never opens a new connection.
- Always injects TENANT_ID from st.session_state as a bound parameter.

Allowed tables: CUSTOMERS, DISTRO_GRID, RESET_SCHEDULE,
                SALES_REPORT, PRODUCTS, SUPPLIER_COUNTY
"""

import re
import time
import streamlit as st
import pandas as pd
import anthropic
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

# Safety cap — prevents runaway queries on large tables like DISTRO_GRID (100K+)
MAX_ROW_SAFETY_CAP = 200000

EXAMPLE_QUESTIONS = [
    # Simple single-table
    "Show me all stores in FOODMAXX with their salesperson and county",
    "How many stores does each chain have?",
    "Show me all reset schedules for LUCKY in April 2026",
    "Show me the first 5 rows for SAFEWAY in the distro grid",
    # Cross-table
    "Which salesperson has the most active distro grid placements?",
    "Show me all products from 2 TOWNS CIDERHOUSE approved in Alameda county",
    "Which stores in SAFEWAY are missing from the distro grid?",
    "Show me all active placements for RALEYS with product details",
    # Supplier/county
    "Which suppliers are approved in San Joaquin county?",
    "Show me all distro grid items for SPROUTS where YES_NO is 1",
    "Which chains have stores in Fresno county?",
]


# ─────────────────────────────────────────────────────────────────────────────
# Schema builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_schema_context(chain_names: list[str]) -> str:
    chains_str = ", ".join(f"'{c}'" for c in sorted(chain_names)) if chain_names else "unknown"

    return f"""
You have access to the following Snowflake tables. Use EXACT column names as shown.
Every table has a TENANT_ID column — you MUST always filter on TENANT_ID = :tenant_id.

TABLE ALIASES — always use these short aliases:
  C  = CUSTOMERS
  P  = PRODUCTS
  DG = DISTRO_GRID
  SC = SUPPLIER_COUNTY
  RS = RESET_SCHEDULE
  SR = SALES_REPORT

CUSTOMERS (alias: C):
  CUSTOMER_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, ADDRESS, CITY, STATE,
  COUNTY, SALESPERSON, PHONE_NUMBER, ACCOUNT_STATUS ('ACTIVE'/'INACTIVE'), TENANT_ID
  IMPORTANT — the exact CHAIN_NAME values in this database are: {chains_str}
  Always match chain names exactly as listed above.

DISTRO_GRID (alias: DG):
  DISTRO_GRID_ID, TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME,
  STORE_NUMBER, COUNTY, PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
  SEGMENT, YES_NO (1=active placement, 0=inactive), ACTIVATION_STATUS,
  LAST_LOAD_DATE
  Join to CUSTOMERS on CUSTOMER_ID. Join to PRODUCTS on PRODUCT_ID.
  IMPORTANT: COUNTY stores the string 'None' (not SQL NULL) when no county
  is assigned. Always filter empty counties with:
  AND UPPER(TRIM(DG.COUNTY)) != 'NONE'
  Do NOT use IS NOT NULL for county filtering on this table.

PRODUCTS (alias: P):
  PRODUCT_ID, SUPPLIER, PRODUCT_NAME, PACKAGE, CARRIER_UPC,
  PRODUCT_MANAGER, TENANT_ID
  Join to DISTRO_GRID on PRODUCT_ID. Join to SUPPLIER_COUNTY on SUPPLIER.

SUPPLIER_COUNTY (alias: SC):
  SUPPLIER, COUNTY, STATUS ('Yes'=approved, 'No'=not approved), TENANT_ID
  Join to PRODUCTS on SUPPLIER. Join to CUSTOMERS on COUNTY.

RESET_SCHEDULE (alias: RS):
  RESET_SCHEDULE_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, PHONE_NUMBER,
  CITY, ADDRESS, STATE, COUNTY, TEAM_LEAD, RESET_DATE, RESET_TIME,
  STATUS, NOTES, TENANT_ID
  Join to CUSTOMERS on STORE_NUMBER and CHAIN_NAME.

SALES_REPORT (alias: SR):
  STORE_NUMBER, STORE_NAME, CHAIN_NAME, UPC, PRODUCT_NAME, SALESPERSON,
  PURCHASED_YES_NO, COUNTY, TENANT_ID

KEY RELATIONSHIPS:
  CUSTOMERS → DISTRO_GRID:     JOIN C ON DG.CUSTOMER_ID = C.CUSTOMER_ID
  PRODUCTS  → DISTRO_GRID:     JOIN P ON DG.PRODUCT_ID  = P.PRODUCT_ID
  PRODUCTS  → SUPPLIER_COUNTY: JOIN SC ON P.SUPPLIER    = SC.SUPPLIER
  CUSTOMERS → SUPPLIER_COUNTY: JOIN SC ON C.COUNTY      = SC.COUNTY
  CUSTOMERS → RESET_SCHEDULE:  JOIN RS ON C.STORE_NUMBER = RS.STORE_NUMBER
                                        AND C.CHAIN_NAME  = RS.CHAIN_NAME
"""


# ─────────────────────────────────────────────────────────────────────────────
# SQL generation
# ─────────────────────────────────────────────────────────────────────────────

def _generate_sql(question: str, schema_context: str) -> str:
    client = anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])

    system_prompt = f"""You are a Snowflake SQL expert. Generate a single valid Snowflake SQL SELECT query based on the user's question.

STRICT RULES:
1. Only generate SELECT statements — never INSERT, UPDATE, DELETE, DROP, CREATE, or any DDL/DML.
2. Only query these tables: {', '.join(sorted(ALLOWED_TABLES))}
3. Do NOT include a LIMIT clause unless the user explicitly requests a specific
   number of rows (e.g. 'first 5', 'top 10', 'limit to 3'). In those cases
   include the LIMIT in the query.
4. ALWAYS filter every table on TENANT_ID = :tenant_id — no exceptions.
5. ALWAYS use UPPER(TRIM()) on both sides of ALL string comparisons.
6. ALWAYS use the short table aliases: C=CUSTOMERS, P=PRODUCTS, DG=DISTRO_GRID, SC=SUPPLIER_COUNTY, RS=RESET_SCHEDULE, SR=SALES_REPORT.
7. Use CTEs (WITH clause) for any multi-step or multi-table logic.
8. Never use SELECT * — always name columns explicitly.
9. Always use UPPER_CASE for all column and table names.
10. Always terminate the query with a semicolon (;).
11. Never use f-strings or string formatting — only bound parameters (:tenant_id).
12. Return ONLY the raw SQL query with no explanation, no markdown, no backticks, no preamble.
13. If the question cannot be answered with the available tables, respond with exactly: CANNOT_ANSWER

{schema_context}"""

    for attempt in range(3):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": question}],
                system=system_prompt,
            )
            return message.content[0].text.strip()
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Validation + safety
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sql(sql: str) -> tuple[bool, str]:
    sql_upper = sql.upper().strip()

    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
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

    if ":tenant_id" not in sql.lower():
        return False, "Query must filter on TENANT_ID = :tenant_id for data security."

    return True, ""


def _inject_safety_cap(sql: str, cap: int = MAX_ROW_SAFETY_CAP) -> str:
    """
    Inject safety cap LIMIT only if the query has no LIMIT already.
    Preserves user-requested LIMIT (e.g. 'first 5 rows').
    """
    if re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
        return sql
    return f"{sql.rstrip(';').rstrip()}\nLIMIT {cap};"


def _get_row_count(sql: str) -> int | None:
    """
    Run a COUNT(*) wrapper around the generated SQL to get the total row count
    before fetching all results. Returns None if count fails.
    """
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    try:
        clean_sql = sql.replace(":tenant_id", str(tenant_id)).rstrip(';').rstrip()
        count_sql = f"SELECT COUNT(*) FROM ({clean_sql}) AS _count_query"
        with conn.cursor() as cur:
            cur.execute(count_sql)
            result = cur.fetchone()
            return result[0] if result else None
    except Exception:
        return None


def _run_query(sql: str) -> pd.DataFrame:
    """
    Execute SQL against tenant Snowflake using the cached session connection.
    Always injects TENANT_ID from session state as a string replacement.
    Never opens a new connection.
    """
    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if not conn:
        st.error("❌ No active Snowflake connection. Please log in again.")
        st.stop()
    if not tenant_id:
        st.error("❌ No tenant ID in session. Please log in again.")
        st.stop()

    sql = sql.replace(":tenant_id", str(tenant_id))

    with conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    return pd.DataFrame(results, columns=columns)


# ─────────────────────────────────────────────────────────────────────────────
# Page
# ─────────────────────────────────────────────────────────────────────────────

def render():
    if not st.session_state.get("is_admin"):
        st.warning("You don't have access to this page.")
        return

    conn = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if not conn or not tenant_id:
        st.error("❌ Missing tenant connection. Please log in again.")
        st.stop()

    st.title("🔍 Data Query")
    st.markdown(
        "Ask a question about your data in plain English and get instant results. "
        "Queries are read-only and tenant-scoped."
    )

    # ── Load chain names dynamically ──────────────────────────────────────────
    try:
        chain_names = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
    except Exception:
        chain_names = []

    schema_context = _build_schema_context(chain_names)

    # ── Example questions ─────────────────────────────────────────────────────
    with st.expander("💡 Example questions", expanded=not st.session_state.get("dq_question")):
        for q in EXAMPLE_QUESTIONS:
            if st.button(q, key=f"example_{q[:30]}"):
                st.session_state["dq_input"] = q  # ← update the text input key directly
                st.session_state["dq_question"] = q
                st.session_state.pop("dq_result", None)
                st.session_state.pop("dq_sql", None)
                st.rerun()

    # ── Question input ────────────────────────────────────────────────────────
    question = st.text_input(
        "Ask a question about your data:",
        value=st.session_state.get("dq_question", ""),
        placeholder="e.g. Which salesperson has the most active distro grid placements?",
        key="dq_input",
    )

    col_run, col_clear = st.columns([1, 1])
    with col_run:
        run = st.button("▶ Run Query", type="primary", use_container_width=True)
    with col_clear:
        if st.button("✕ Clear", type="secondary", use_container_width=True):
            for key in ["dq_question", "dq_result", "dq_sql",
                        "dq_row_count", "dq_capped"]:
                st.session_state.pop(key, None)
            st.rerun()

    # ── Generate + run ────────────────────────────────────────────────────────
    if run and question.strip():
        st.session_state["dq_question"] = question
        for key in ["dq_result", "dq_sql", "dq_row_count", "dq_capped"]:
            st.session_state.pop(key, None)

        # Step 1 — Generate SQL
        with st.spinner("Generating query..."):
            try:
                sql = _generate_sql(question, schema_context)
            except anthropic.APIStatusError as e:
                if e.status_code == 529:
                    st.warning("⏳ The AI service is currently busy. Please wait a moment and try again.")
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

        # Step 2 — Validate SQL
        is_valid, error = _validate_sql(sql)
        if not is_valid:
            st.error(f"❌ Query validation failed: {error}")
            with st.expander("🔎 View raw AI response"):
                st.code(sql, language="sql")
            return

        # Step 3 — Count rows (skip if query already has a user-requested LIMIT)
        has_user_limit = bool(re.search(r'\bLIMIT\b', sql, re.IGNORECASE))
        total_rows = None
        capped = False

        if not has_user_limit:
            with st.spinner("Counting rows..."):
                total_rows = _get_row_count(sql)

            if total_rows is not None:
                if total_rows > MAX_ROW_SAFETY_CAP:
                    st.warning(
                        f"⚠️ This query returns **{total_rows:,} rows** which exceeds the "
                        f"**{MAX_ROW_SAFETY_CAP:,} row** safety cap. Results will be truncated."
                    )
                    capped = True
                else:
                    st.info(f"ℹ️ Query will return **{total_rows:,} rows** — fetching all.")

        # Step 4 — Apply safety cap if needed and run
        sql_final = _inject_safety_cap(sql) if not has_user_limit else sql
        st.session_state["dq_sql"] = sql_final
        st.session_state["dq_row_count"] = total_rows
        st.session_state["dq_capped"] = capped

        with st.spinner("Running query..."):
            try:
                df = _run_query(sql_final)
                st.session_state["dq_result"] = df
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
                return

    # ── Results ───────────────────────────────────────────────────────────────
    df = st.session_state.get("dq_result")
    sql_used = st.session_state.get("dq_sql")
    total_rows = st.session_state.get("dq_row_count")
    capped = st.session_state.get("dq_capped", False)

    if df is not None:
        if df.empty:
            st.info("Query ran successfully but returned no results.")
        else:
            if capped:
                st.success(
                    f"**{len(df):,} row(s)** returned "
                    f"(capped at {MAX_ROW_SAFETY_CAP:,} of {total_rows:,} total rows)."
                )
            else:
                st.success(f"**{len(df):,} row(s)** returned.")

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
