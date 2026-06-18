# app_pages/data_query.py
"""
AI-Powered Data Query Page (Admin Only)

Overview:
- Allows admins to query Snowflake tables using plain English.
- Claude API classifies intent (LOOKUP/ANALYSIS/DIAGNOSTIC), extracts supplier names,
  and generates a safe SELECT query (DIAGNOSTIC skips SQL entirely).
- Query is validated (SELECT only, allowed tables only, TENANT_ID required)
  before execution.
- Row count is checked before fetching — warns if results exceed safety cap.
- When a query returns 0 rows for an ANALYSIS question, utils.diagnostics
  runs a SQL check sequence and reports the root cause in plain English.
- LOOKUP zero-row results are reported cleanly without triggering diagnostics.
- Safety cap of 200K rows prevents runaway queries on large tables.
- Always uses cached st.session_state["conn"] — never opens a new connection.

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
    "GAP_REPORT_TMP",
    "GAP_REPORT_TMP2",
}

# Gap tables are per-tenant databases — no TENANT_ID column exists in these tables
GAP_TABLES = {"GAP_REPORT_TMP", "GAP_REPORT_TMP2"}

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
  CUSTOMER_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, ADDRESS, CITY,
  COUNTY, SALESPERSON, ACCOUNT_STATUS ('ACTIVE'/'INACTIVE'),
  TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
  NOTE: There is NO STATE column in CUSTOMERS. Do not reference C.STATE.
  IMPORTANT — the exact CHAIN_NAME values in this database are: {chains_str}
  Always match chain names exactly as listed above.

DISTRO_GRID (alias: DG):
  DISTRO_GRID_ID, TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME,
  STORE_NUMBER, COUNTY, PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
  SEGMENT, YES_NO (1=active placement, 0=inactive), ACTIVATION_STATUS,
  CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
  DENORMALIZATION NOTE: DISTRO_GRID already contains CHAIN_NAME, STORE_NAME,
  STORE_NUMBER, COUNTY, and PRODUCT_NAME — joins back to CUSTOMERS or PRODUCTS
  are only needed for columns NOT present in DISTRO_GRID (e.g. SALESPERSON,
  ACCOUNT_STATUS, SUPPLIER, PACKAGE).
  COUNTY NOTE: COUNTY stores the string 'None' (not SQL NULL) when no county
  is assigned. Always filter empty counties with:
  AND UPPER(TRIM(DG.COUNTY)) != 'NONE'
  Do NOT use IS NOT NULL for county filtering on this table.

PRODUCTS (alias: P):
  PRODUCT_ID, SUPPLIER, PRODUCT_NAME, PACKAGE, CARRIER_UPC,
  PRODUCT_MANAGER, TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
  Join to DISTRO_GRID on PRODUCT_ID. Join to SUPPLIER_COUNTY on SUPPLIER.

SUPPLIER_COUNTY (alias: SC):
  SUPPLIER, COUNTY, STATUS ('Yes'=approved to sell, 'No'=not approved),
  TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
  NOTE: STATUS is mixed case ('Yes'/'No') — not all caps.
  Join to PRODUCTS on SUPPLIER. Join to CUSTOMERS on COUNTY.

RESET_SCHEDULE (alias: RS):
  RESET_SCHEDULE_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME, PHONE_NUMBER,
  CITY, ADDRESS, STATE, COUNTY, TEAM_LEAD, RESET_DATE, RESET_TIME,
  STATUS, NOTES, TENANT_ID, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
  NOTE: STATE exists in RESET_SCHEDULE but NOT in CUSTOMERS.
  Join to CUSTOMERS on STORE_NUMBER and CHAIN_NAME.

SALES_REPORT (alias: SR):
  STORE_NUMBER, STORE_NAME, ADDRESS, CHAIN_NAME, UPC, PRODUCT_NAME,
  SALESPERSON, PURCHASED_YES_NO, SALE_DATE, TENANT_ID,
  CREATED_AT, LAST_LOAD_DATE
  LAST_LOAD_DATE: the date this data was uploaded/loaded into Snowflake.
    Use this for questions like "when was the last upload?" or "when was data last loaded?"
  SALE_DATE: the transaction date from the source file — may be NULL if not captured in the upload.
  NOTE: There is NO COUNTY column in SALES_REPORT. Do not reference SR.COUNTY.

KEY RELATIONSHIPS:
  CUSTOMERS → DISTRO_GRID:     JOIN C ON DG.CUSTOMER_ID = C.CUSTOMER_ID
                                (only needed for SALESPERSON, ACCOUNT_STATUS,
                                ADDRESS, CITY — DG already has CHAIN_NAME,
                                STORE_NAME, STORE_NUMBER, COUNTY)
  PRODUCTS  → DISTRO_GRID:     JOIN P ON DG.PRODUCT_ID  = P.PRODUCT_ID
                                (only needed for SUPPLIER, PACKAGE,
                                CARRIER_UPC, PRODUCT_MANAGER)
  PRODUCTS  → SUPPLIER_COUNTY: JOIN SC ON P.SUPPLIER    = SC.SUPPLIER
  CUSTOMERS → SUPPLIER_COUNTY: JOIN SC ON C.COUNTY      = SC.COUNTY
  CUSTOMERS → RESET_SCHEDULE:  JOIN RS ON C.STORE_NUMBER = RS.STORE_NUMBER
                                        AND C.CHAIN_NAME  = RS.CHAIN_NAME

## GAP REPORT TABLES — ALWAYS USE THESE FOR GAP ANALYSIS

Two pre-built tables exist for gap reporting. NEVER rebuild gap logic from
the raw tables (DISTRO_GRID, SALES_REPORT, SUPPLIER_COUNTY, PRODUCTS).
All business rules are already applied by a stored procedure.

TABLE: GAP_REPORT_TMP
Use for: gap analysis, distribution gaps, missing placements, purchased vs not purchased
Columns: CHAIN_NAME, STORE_NAME, STORE_NUMBER, ADDRESS, CITY, COUNTY, SUPPLIER,
         PRODUCT_NAME, SALESPERSON, "dg_upc", "sr_upc", "In_Schematic", PURCHASED_YES_NO

TABLE: GAP_REPORT_TMP2
Use for: gap analysis that also involves reset schedules or upcoming store visits
Adds to GAP_REPORT_TMP: RESET_DATE, RESET_TIME, sc_STATUS

CRITICAL RULES FOR THESE TABLES:
- Do NOT add WHERE TENANT_ID = :tenant_id — these tables have NO TENANT_ID column; adding one returns zero rows
- Always quote mixed-case columns exactly: "dg_upc", "sr_upc", "In_Schematic"
- PURCHASED_YES_NO = 1 means the store HAS purchased the product (NOT a gap)
- PURCHASED_YES_NO = 0 or NULL means the store has NOT purchased it (IS a gap)
- In_Schematic is always 1 in these tables — do not filter on it
- When to use which: gap/placement/purchase question → GAP_REPORT_TMP; mentions resets/upcoming dates → GAP_REPORT_TMP2
- Never use raw DISTRO_GRID + SALES_REPORT join for gap analysis

GAP QUERY PATTERNS:

-- All gaps for a supplier (not purchased):
SELECT CHAIN_NAME, STORE_NAME, STORE_NUMBER, COUNTY, SALESPERSON,
       PRODUCT_NAME, "dg_upc", PURCHASED_YES_NO
FROM GAP_REPORT_TMP
WHERE UPPER(TRIM(SUPPLIER)) = UPPER(TRIM('<supplier_name>'))
  AND (PURCHASED_YES_NO = 0 OR PURCHASED_YES_NO IS NULL)
ORDER BY CHAIN_NAME, COUNTY, STORE_NUMBER;

-- Full gap report with gap status label:
SELECT CHAIN_NAME, STORE_NAME, STORE_NUMBER, COUNTY, SALESPERSON,
       PRODUCT_NAME, "dg_upc", PURCHASED_YES_NO,
       CASE WHEN PURCHASED_YES_NO = 1 THEN 'NOT A GAP' ELSE 'GAP' END AS GAP_STATUS
FROM GAP_REPORT_TMP
WHERE UPPER(TRIM(SUPPLIER)) = UPPER(TRIM('<supplier_name>'))
ORDER BY GAP_STATUS DESC, CHAIN_NAME, COUNTY, STORE_NUMBER;

-- Gaps with upcoming reset dates:
SELECT CHAIN_NAME, STORE_NAME, STORE_NUMBER, COUNTY, SALESPERSON,
       SUPPLIER, PRODUCT_NAME, RESET_DATE, RESET_TIME,
       CASE WHEN PURCHASED_YES_NO = 1 THEN 'NOT A GAP' ELSE 'GAP' END AS GAP_STATUS
FROM GAP_REPORT_TMP2
WHERE UPPER(TRIM(SUPPLIER)) = UPPER(TRIM('<supplier_name>'))
  AND (PURCHASED_YES_NO = 0 OR PURCHASED_YES_NO IS NULL)
ORDER BY RESET_DATE, CHAIN_NAME, STORE_NUMBER;

-- Gap summary by salesperson:
SELECT SALESPERSON,
       COUNT(*) AS TOTAL_ELIGIBLE,
       SUM(CASE WHEN PURCHASED_YES_NO = 1 THEN 1 ELSE 0 END) AS SOLD,
       SUM(CASE WHEN PURCHASED_YES_NO != 1 OR PURCHASED_YES_NO IS NULL THEN 1 ELSE 0 END) AS GAPS
FROM GAP_REPORT_TMP
WHERE UPPER(TRIM(SUPPLIER)) = UPPER(TRIM('<supplier_name>'))
GROUP BY SALESPERSON
ORDER BY GAPS DESC;

-- Gap summary by county:
SELECT COUNTY,
       COUNT(*) AS TOTAL_ELIGIBLE,
       SUM(CASE WHEN PURCHASED_YES_NO = 1 THEN 1 ELSE 0 END) AS SOLD,
       SUM(CASE WHEN PURCHASED_YES_NO != 1 OR PURCHASED_YES_NO IS NULL THEN 1 ELSE 0 END) AS GAPS
FROM GAP_REPORT_TMP
WHERE UPPER(TRIM(SUPPLIER)) = UPPER(TRIM('<supplier_name>'))
GROUP BY COUNTY
ORDER BY GAPS DESC;
"""


# ─────────────────────────────────────────────────────────────────────────────
# AI call + response parsing
# ─────────────────────────────────────────────────────────────────────────────

def _call_ai(question: str, schema_context: str) -> str:
    """Call the Claude API. Returns the full response text including INTENT metadata."""
    client = anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])

    system_prompt = f"""You are a Snowflake SQL expert. Generate a valid Snowflake SQL SELECT query based on the user's question.

First, classify the user's question:

INTENT: LOOKUP      — user is checking whether something exists in a specific table
INTENT: ANALYSIS    — user expects data to exist and wants insights, gaps, or reports
INTENT: DIAGNOSTIC  — user is asking WHY something is missing or not showing (see below)

Examples:
  "Is Sunboy in the Supplier County table?"         → INTENT: LOOKUP
  "Show me Sunboy entries in the Distro Grid"       → INTENT: LOOKUP
  "Is Pacific Coast Seltzers an approved supplier?" → INTENT: LOOKUP
  "Which stores are missing Sunboy from the grid?"  → INTENT: ANALYSIS
  "Show me the gap report for Alameda county"       → INTENT: ANALYSIS

## INTENT: DIAGNOSTIC — DO NOT WRITE SQL

A third intent class exists for questions asking WHY something is missing,
not showing, or not appearing in a report or analysis.

INTENT: DIAGNOSTIC applies when the user asks:
  "Why is X not showing in the gap report?"
  "Why aren't X products appearing?"
  "Why is X missing from the report?"
  "Why can't I see X?"
  "Why is X not in the distro grid?"
  "Why doesn't X show up?"
  "X is not showing — why?"

CRITICAL RULES FOR DIAGNOSTIC INTENT:
- Do NOT generate any SQL whatsoever
- Do NOT attempt to investigate the data yourself
- Do NOT write a UNION ALL or any other diagnostic query
- Return ONLY the following two lines and nothing else:

  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: <supplier name extracted from the question>

If no supplier name can be extracted, return:
  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: UNKNOWN

The application handles all investigation automatically.
Any SQL you generate for a DIAGNOSTIC question will be ignored and
will produce incorrect results for the user.

DIAGNOSTIC EXAMPLES:

User: "Why is Sunboy not showing in the gap report?"
Correct response:
  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: Sunboy

User: "Why aren't 2 Towns Ciderhouse products appearing?"
Correct response:
  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: 2 Towns Ciderhouse

User: "Cascade Brewing is missing from my report, why?"
Correct response:
  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: Cascade Brewing

User: "Why is my gap report empty?"
Correct response:
  INTENT: DIAGNOSTIC
  EXTRACTED_SUPPLIER: UNKNOWN

If the question names a specific supplier, brand, or manufacturer, include:
EXTRACTED_SUPPLIER: <name>
(Omit this line entirely if no supplier is mentioned.)

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
12. Never invent column names — only use columns listed in the schema below.
13. Format your response as:
    INTENT: <LOOKUP|ANALYSIS>
    [EXTRACTED_SUPPLIER: <name>]
    <raw SQL query starting with SELECT or WITH>
    For DIAGNOSTIC intent, return ONLY:
    INTENT: DIAGNOSTIC
    EXTRACTED_SUPPLIER: <name or UNKNOWN>
    If the question cannot be answered with SQL, respond with:
    INTENT: UNKNOWN
    CANNOT_ANSWER

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


def _extract_intent(ai_response: str) -> str:
    """Returns 'LOOKUP', 'ANALYSIS', 'DIAGNOSTIC', or 'UNKNOWN'."""
    match = re.search(r'INTENT:\s*(LOOKUP|ANALYSIS|DIAGNOSTIC)', ai_response, re.IGNORECASE)
    return match.group(1).upper() if match else "UNKNOWN"


def _extract_supplier(ai_response: str) -> str | None:
    """Returns the extracted supplier name, or None if not present."""
    match = re.search(r'EXTRACTED_SUPPLIER:\s*(.+)', ai_response)
    return match.group(1).strip() if match else None


def _extract_sql(ai_response: str) -> str:
    """Extract the SQL query from a full AI response (strips INTENT/EXTRACTED_SUPPLIER lines)."""
    match = re.search(r'((?:WITH|SELECT)\b.+)', ai_response, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return ai_response.strip()


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

    # Extract CTE names so we don't flag them as unknown tables
    cte_names = set(re.findall(r'\b(\w+)\s+AS\s*\(', sql_upper))

    table_refs = re.findall(r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)', sql_upper)
    for table in table_refs:
        if table in cte_names:
            continue  # CTE alias — not a real table
        if table not in ALLOWED_TABLES:
            return False, f"Table '{table}' is not in the allowed list."

    # Gap tables are per-tenant databases with no TENANT_ID column — skip the
    # filter check only when the query touches exclusively gap tables.
    actual_refs = {t for t in table_refs if t not in cte_names}
    non_gap_refs = actual_refs - GAP_TABLES
    if non_gap_refs and ":tenant_id" not in sql.lower():
        return False, "Query must filter on TENANT_ID = :tenant_id for data security."

    return True, ""


def _inject_safety_cap(sql: str, cap: int = MAX_ROW_SAFETY_CAP) -> str:
    """Inject safety cap LIMIT only if the query has no LIMIT already."""
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

    # ── Load chain names (cached in session_state to avoid repeated DB hits) ──
    if "dq_chain_names" not in st.session_state:
        try:
            st.session_state["dq_chain_names"] = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        except Exception:
            st.session_state["dq_chain_names"] = []
    chain_names = st.session_state["dq_chain_names"]

    schema_context = _build_schema_context(chain_names)

    # ── Example questions ─────────────────────────────────────────────────────
    with st.expander("💡 Example questions", expanded=not st.session_state.get("dq_question")):
        for q in EXAMPLE_QUESTIONS:
            if st.button(q, key=f"example_{q[:30]}"):
                st.session_state["dq_input"] = q
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
            for key in ["dq_question", "dq_result", "dq_sql", "dq_row_count",
                        "dq_capped", "dq_input", "dq_intent", "dq_supplier",
                        "dq_diagnosis", "dq_diagnosis_counts"]:
                st.session_state.pop(key, None)
            st.rerun()

    # ── Generate + run ────────────────────────────────────────────────────────
    if run and question.strip():
        st.session_state["dq_question"] = question
        for key in ["dq_result", "dq_sql", "dq_row_count", "dq_capped",
                    "dq_intent", "dq_supplier", "dq_diagnosis", "dq_diagnosis_counts"]:
            st.session_state.pop(key, None)

        # Step 1 — Call AI (returns full response with INTENT + SQL)
        with st.spinner("Generating query..."):
            try:
                ai_response = _call_ai(question, schema_context)
            except anthropic.APIStatusError as e:
                if e.status_code == 529:
                    st.warning("⏳ The AI service is currently busy. Please wait a moment and try again.")
                else:
                    st.error(f"❌ AI service error: {e.message}")
                return
            except Exception as e:
                st.error(f"❌ Failed to generate query: {e}")
                return

        intent = _extract_intent(ai_response)
        supplier = _extract_supplier(ai_response)
        st.session_state["dq_intent"] = intent
        st.session_state["dq_supplier"] = supplier

        if intent == "DIAGNOSTIC":
            # Skip SQL entirely — hand off to diagnostic function
            from utils.diagnostics import run_diagnostic
            supplier_key = supplier if supplier and supplier.upper() != "UNKNOWN" else None
            with st.spinner("Investigating..."):
                diagnosis, counts = run_diagnostic(
                    original_question=question,
                    generated_sql=None,
                    conn=conn,
                    tenant_id=tenant_id,
                    extracted_supplier=supplier_key,
                )
            st.session_state["dq_diagnosis"] = diagnosis
            st.session_state["dq_diagnosis_counts"] = counts

        else:
            # LOOKUP or ANALYSIS — extract and run SQL
            if "CANNOT_ANSWER" in ai_response.upper():
                st.warning(
                    "I couldn't find a way to answer that with the available data. "
                    "Try rephrasing or pick one of the example questions for ideas."
                )
                return

            sql = _extract_sql(ai_response)

            # Step 2 — Validate SQL
            is_valid, error = _validate_sql(sql)
            if not is_valid:
                st.error(f"❌ Query validation failed: {error}")
                with st.expander("🔎 View raw AI response"):
                    st.code(ai_response, language="sql")
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

            # Step 5 — Diagnostic on 0 rows (ANALYSIS intent only)
            if df.empty and intent != "LOOKUP":
                from utils.diagnostics import run_diagnostic
                with st.spinner("Investigating why no results were found..."):
                    diagnosis, counts = run_diagnostic(
                        original_question=question,
                        generated_sql=sql_final,
                        conn=conn,
                        tenant_id=tenant_id,
                        extracted_supplier=supplier,
                    )
                st.session_state["dq_diagnosis"] = diagnosis
                st.session_state["dq_diagnosis_counts"] = counts

    # ── Results ───────────────────────────────────────────────────────────────
    df = st.session_state.get("dq_result")
    sql_used = st.session_state.get("dq_sql")
    total_rows = st.session_state.get("dq_row_count")
    capped = st.session_state.get("dq_capped", False)
    intent = st.session_state.get("dq_intent", "UNKNOWN")
    diagnosis = st.session_state.get("dq_diagnosis")

    counts = st.session_state.get("dq_diagnosis_counts", {})
    supplier_name = st.session_state.get("dq_supplier") or "this supplier"

    if intent == "DIAGNOSTIC" and diagnosis is not None:
        st.warning("**Diagnostic Results**")
        if counts:
            from utils.diagnostics import build_narrative
            st.markdown(build_narrative(supplier_name, counts))
        else:
            st.markdown(diagnosis)

    elif df is not None:
        if df.empty:
            if intent == "LOOKUP":
                st.info("✅ Query ran successfully. **No records found** matching your criteria.")
            elif diagnosis:
                st.info("ℹ️ Query returned no results.")
                st.warning("**Why did this return no results?**")
                if counts:
                    from utils.diagnostics import build_narrative
                    st.markdown(build_narrative(supplier_name, counts))
                else:
                    st.markdown(diagnosis)
            else:
                st.info("ℹ️ Query returned no results.")
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