# utils/ai_chat.py
"""
Claude AI chat with tool use for Chainlink Analytics.
Provides an agentic loop where Claude can call run_sql or describe_table
to query Snowflake directly, then interpret results in plain English.
"""
import re
import pandas as pd
import anthropic
import streamlit as st

# Tables Claude may run SELECT queries against
ALLOWED_TABLES = {
    "CUSTOMERS", "DISTRO_GRID", "RESET_SCHEDULE",
    "SALES_REPORT", "PRODUCTS", "SUPPLIER_COUNTY",
    "GAP_REPORT_TMP", "GAP_REPORT_TMP2",
    "GAP_REPORT",
    "SALESPERSON_EXECUTION_SUMMARY",
    "SALESPERSON_EXECUTION_SUMMARY_TBL",
}

# Tables that live in the tenant's own DB — no TENANT_ID column, no filter needed
NO_TENANT_TABLES = {
    "GAP_REPORT_TMP", "GAP_REPORT_TMP2",
    "GAP_REPORT",
    "SALESPERSON_EXECUTION_SUMMARY",
    "SALESPERSON_EXECUTION_SUMMARY_TBL",
}

# Tables Claude may DESCRIBE (superset of ALLOWED_TABLES — metadata only, always safe)
DESCRIBABLE_TABLES = ALLOWED_TABLES

CHAT_ROW_CAP = 500

TOOLS = [
    {
        "name": "describe_table",
        "description": (
            "Look up the exact column names and data types for any table. "
            "Use this before writing a query whenever you are unsure which columns exist, "
            "what they are named, or what date/timestamp columns track (e.g. upload date vs sale date). "
            "Always prefer this over guessing column names."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": (
                        "The table to describe. One of: "
                        + ", ".join(sorted(DESCRIBABLE_TABLES))
                    ),
                }
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Execute a read-only SELECT query against Snowflake. "
            "Use this whenever you need actual data to answer the question. "
            "For tables that are NOT in the no-tenant list, always filter "
            "WHERE TENANT_ID = '<tenant_id>'. "
            "The following tables have NO TENANT_ID column — do NOT add a filter for them: "
            "GAP_REPORT_TMP, GAP_REPORT_TMP2, GAP_REPORT, "
            "SALESPERSON_EXECUTION_SUMMARY, SALESPERSON_EXECUTION_SUMMARY_TBL. "
            "Results are capped at 500 rows. "
            "If unsure about column names, call describe_table first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A valid Snowflake SELECT (or WITH ... SELECT) query.",
                }
            },
            "required": ["query"],
        },
    },
]


def _build_system_prompt(chain_names: list[str], tenant_id: str) -> str:
    chains_str = ", ".join(f"'{c}'" for c in sorted(chain_names)) if chain_names else "unknown"
    return f"""You are Claude, the AI analyst inside Chainlink Analytics — a sales execution \
and gap reporting platform for beverage distributors and field sales teams.

Your job: answer questions about the tenant's data clearly and concisely. \
When you need data, call run_sql. When you have results, interpret them — \
give the insight, not just the rows. Sales managers want answers, not raw SQL output.

TENANT_ID for this session: '{tenant_id}'
Always inject this value literally into queries for tables that require it.

━━━ TOOLS ━━━

describe_table — use this FIRST whenever you are unsure about column names or \
which date columns exist. Tables have columns like LAST_LOAD_DATE (upload date), \
CREATED_AT, UPDATED_AT that you may not expect. Always verify before querying.

run_sql — execute a SELECT query. See rules below.

━━━ TABLE OVERVIEW ━━━

Core tables (require TENANT_ID filter):
  CUSTOMERS, DISTRO_GRID, PRODUCTS, SUPPLIER_COUNTY, RESET_SCHEDULE, SALES_REPORT

No-TENANT_ID tables (connection provides tenant isolation — never add TENANT_ID filter):
  GAP_REPORT_TMP       — current gap analysis (use for gaps/placements/purchases)
  GAP_REPORT_TMP2      — same + reset schedule info (use when resets/visits mentioned)
  GAP_REPORT           — summary gap view
  SALESPERSON_EXECUTION_SUMMARY      — current period execution by salesperson
  SALESPERSON_EXECUTION_SUMMARY_TBL  — historical execution (has LOG_DATE for trend analysis)

Table aliases for queries:
  C=CUSTOMERS, P=PRODUCTS, DG=DISTRO_GRID, SC=SUPPLIER_COUNTY,
  RS=RESET_SCHEDULE, SR=SALES_REPORT

Key facts about CUSTOMERS:
  Exact chain names in this database: {chains_str}
  No STATE column. ACCOUNT_STATUS is 'ACTIVE' or 'INACTIVE'.

Key facts about DISTRO_GRID:
  YES_NO = 1 means active placement. Already contains CHAIN_NAME, STORE_NAME, STORE_NUMBER, COUNTY.
  COUNTY stores the string 'None' (not SQL NULL) when empty.

Key facts about GAP tables:
  PURCHASED_YES_NO = 1 → purchased (NOT a gap). 0 or NULL → IS a gap.
  Always quote mixed-case columns: "dg_upc", "sr_upc", "In_Schematic", "sc_STATUS"

━━━ SQL RULES ━━━
1. SELECT only — never INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE.
2. Filter TENANT_ID = '{tenant_id}' for core tables. Never add it for no-TENANT_ID tables.
3. Use UPPER(TRIM()) on both sides of string comparisons.
4. Never SELECT * — name columns explicitly.
5. Use CTEs (WITH clause) for multi-step logic.
6. When unsure about column names → call describe_table first, then run_sql.

━━━ RESPONSE STYLE ━━━
- Lead with the key insight after seeing data. Use specific names and numbers.
- If a query returns 0 rows for an analysis question, investigate before saying "no results".
- For conversational questions that need no data, answer directly without tools.
- NEVER mention "Snowflake" to the user. Refer to the data store as "your Chainlink database" \
— users don't need to know the underlying technology.
"""


def _validate_sql(sql: str, tenant_id: str) -> tuple[bool, str]:
    sql_upper = sql.upper().strip()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False, "Only SELECT queries are allowed."
    dangerous = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TRUNCATE", "EXECUTE", "EXEC", "CALL", "GRANT", "REVOKE",
    ]
    for kw in dangerous:
        if re.search(rf'\b{kw}\b', sql_upper):
            return False, f"Disallowed keyword: {kw}"
    cte_names = set(re.findall(r'\b(\w+)\s+AS\s*\(', sql_upper))
    table_refs = re.findall(r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)', sql_upper)
    for table in table_refs:
        if table in cte_names:
            continue
        if table not in ALLOWED_TABLES:
            return False, f"Table '{table}' is not in the allowed list."
    actual_refs = {t for t in table_refs if t not in cte_names}
    tenant_required = actual_refs - NO_TENANT_TABLES
    if tenant_required and str(tenant_id) not in sql:
        return False, "Query must include TENANT_ID filter for data security."
    return True, ""


def _execute_sql(query: str, conn, tenant_id: str) -> tuple[str, pd.DataFrame | None]:
    valid, err = _validate_sql(query, tenant_id)
    if not valid:
        return f"SQL blocked: {err}", None

    clean = query.replace(":tenant_id", str(tenant_id))
    if not re.search(r'\bLIMIT\b', clean, re.IGNORECASE):
        clean = f"{clean.rstrip(';').rstrip()}\nLIMIT {CHAT_ROW_CAP};"

    try:
        with conn.cursor() as cur:
            cur.execute(clean)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            return "Query returned 0 rows.", None
        return f"{len(df)} row(s) returned.", df
    except Exception as e:
        return f"Query error: {e}", None


def _describe_table(table_name: str, conn) -> str:
    table_upper = table_name.upper().strip()
    if table_upper not in DESCRIBABLE_TABLES:
        return (
            f"'{table_name}' is not in the allowed table list. "
            f"Available: {', '.join(sorted(DESCRIBABLE_TABLES))}"
        )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {table_upper}")
            rows = cur.fetchall()
        # Snowflake DESCRIBE TABLE returns: name, type, kind, null?, default, pk, uk, ...
        lines = [f"  {r[0]}  {r[1]}  (null={r[3]})" for r in rows]
        return f"Table {table_upper} — {len(rows)} columns:\n" + "\n".join(lines)
    except Exception as e:
        return f"Could not describe table '{table_name}': {e}"


def _content_to_dicts(content) -> list[dict]:
    result = []
    for block in content:
        if hasattr(block, "model_dump"):
            result.append(block.model_dump())
        elif isinstance(block, dict):
            result.append(block)
        else:
            result.append({"type": "text", "text": str(block)})
    return result


def run_chat_turn(
    user_message: str,
    api_messages: list[dict],
    conn,
    tenant_id: str,
    chain_names: list[str],
) -> tuple[str, list[pd.DataFrame]]:
    """
    Run one user turn through the Claude agentic loop.

    Mutates api_messages in place with the full assistant turn.
    Returns (final_text_response, list_of_dataframes_produced_by_tool_calls).
    On error, api_messages is rolled back to its pre-call length.
    """
    client = anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])
    system = _build_system_prompt(chain_names, tenant_id)

    checkpoint = len(api_messages)
    api_messages.append({"role": "user", "content": user_message})

    data_frames: list[pd.DataFrame] = []

    try:
        for _ in range(10):
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                system=system,
                messages=api_messages,
                tools=TOOLS,
            )

            if response.stop_reason == "end_turn":
                text = next(
                    (b.text for b in response.content if hasattr(b, "text")),
                    "I wasn't able to generate a response.",
                )
                api_messages.append({
                    "role": "assistant",
                    "content": _content_to_dicts(response.content),
                })
                return text, data_frames

            if response.stop_reason == "tool_use":
                api_messages.append({
                    "role": "assistant",
                    "content": _content_to_dicts(response.content),
                })
                tool_results = []
                for block in response.content:
                    if not hasattr(block, "type") or block.type != "tool_use":
                        continue

                    if block.name == "describe_table":
                        content = _describe_table(block.input["table_name"], conn)

                    elif block.name == "run_sql":
                        summary, df = _execute_sql(block.input["query"], conn, tenant_id)
                        if df is not None:
                            data_frames.append(df)
                            cols_str = ", ".join(df.columns)
                            preview = df.head(20).to_string(index=False)
                            tail = f"\n...({len(df) - 20} more rows)" if len(df) > 20 else ""
                            content = f"{summary}\nColumns: {cols_str}\n{preview}{tail}"
                        else:
                            content = summary

                    else:
                        content = f"Unknown tool: {block.name}"

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": content,
                    })

                api_messages.append({"role": "user", "content": tool_results})

            else:
                break

        return "I wasn't able to complete that analysis. Please try rephrasing.", data_frames

    except Exception as e:
        del api_messages[checkpoint:]
        raise
