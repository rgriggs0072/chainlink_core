# utils/diagnostics.py
"""
Zero-Results Diagnostic Protocol for AI Data Query.

When the Data Query page returns 0 rows for an ANALYSIS-intent question,
this module runs a single UNION ALL count query across the four key tables,
then interprets the counts into a plain-English narrative via build_narrative().

Rules:
- No Streamlit imports — pure Python, returns strings.
- No new connections — takes the cached session conn.
- All queries use %s positional params to prevent SQL injection.
- All queries include TENANT_ID filters (except GAP_REPORT_TMP which has none).
"""

from __future__ import annotations
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Count query — single UNION ALL across all four diagnostic tables
# Params: (pattern, tenant_id, pattern, tenant_id, pattern, tenant_id, pattern)
# ─────────────────────────────────────────────────────────────────────────────

_COUNT_QUERY = """
SELECT 'PRODUCTS' AS TABLE_NAME, COUNT(*) AS CNT
FROM PRODUCTS
WHERE UPPER(TRIM(SUPPLIER)) LIKE UPPER(TRIM(%s))
  AND TENANT_ID = %s

UNION ALL

SELECT 'DISTRO_GRID', COUNT(*)
FROM DISTRO_GRID DG
JOIN PRODUCTS P
    ON DG.PRODUCT_ID = P.PRODUCT_ID
   AND P.TENANT_ID   = DG.TENANT_ID
WHERE UPPER(TRIM(P.SUPPLIER)) LIKE UPPER(TRIM(%s))
  AND DG.TENANT_ID = %s

UNION ALL

SELECT 'SUPPLIER_COUNTY', COUNT(*)
FROM SUPPLIER_COUNTY
WHERE UPPER(TRIM(SUPPLIER)) LIKE UPPER(TRIM(%s))
  AND TENANT_ID = %s

UNION ALL

SELECT 'GAP_REPORT_TMP', COUNT(*)
FROM GAP_REPORT_TMP
WHERE UPPER(TRIM(SUPPLIER)) LIKE UPPER(TRIM(%s))
"""

_GENERAL_CHECKS = [
    ("PRODUCTS table row count",
     "SELECT COUNT(*) AS CNT FROM PRODUCTS WHERE TENANT_ID = %s"),
    ("SUPPLIER_COUNTY active authorizations",
     "SELECT COUNT(*) AS CNT FROM SUPPLIER_COUNTY WHERE STATUS = 'Yes' AND TENANT_ID = %s"),
    ("CUSTOMERS active stores",
     "SELECT COUNT(*) AS CNT FROM CUSTOMERS WHERE UPPER(TRIM(ACCOUNT_STATUS)) = 'ACTIVE' AND TENANT_ID = %s"),
    ("DISTRO_GRID active placements",
     "SELECT COUNT(*) AS CNT FROM DISTRO_GRID WHERE YES_NO = 1 AND UPPER(TRIM(COUNTY)) != 'NONE' AND TENANT_ID = %s"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Query helper
# ─────────────────────────────────────────────────────────────────────────────

def _run_diag_query(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a diagnostic query. Returns empty DataFrame on any error."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# General fallback (no supplier extracted)
# ─────────────────────────────────────────────────────────────────────────────

def run_general_diagnostic(conn, tenant_id: str) -> str:
    """Run table-level row counts when no specific supplier can be identified."""
    lines = ["**General data availability check:**\n"]
    for label, sql in _GENERAL_CHECKS:
        df = _run_diag_query(conn, sql, (tenant_id,))
        count = int(df.iloc[0, 0]) if not df.empty else 0
        status = "✅" if count > 0 else "⚠️ **0 rows**"
        lines.append(f"- {label}: {status} ({count:,})")
    lines.append(
        "\n\nIf any count above shows 0, that table is missing data — "
        "uploading fresh data for that table should resolve the empty results."
    )
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Narrative layer
# ─────────────────────────────────────────────────────────────────────────────

def build_narrative(supplier: str, counts: dict) -> str:
    """
    Interprets diagnostic count results and returns a plain-English
    narrative with root cause and fix instructions.

    Args:
        supplier: The supplier name extracted from the user's question
        counts: Dict of table name -> row count from diagnostic queries
            Expected keys: 'PRODUCTS', 'DISTRO_GRID', 'SUPPLIER_COUNTY', 'GAP_REPORT_TMP'
            Value of -1 means the query errored (table may not exist)

    Returns:
        A markdown-formatted string ready for st.markdown()
    """

    products_count        = counts.get('PRODUCTS', 0)
    distro_count          = counts.get('DISTRO_GRID', 0)
    supplier_county_count = counts.get('SUPPLIER_COUNTY', 0)
    gap_report_count      = counts.get('GAP_REPORT_TMP', 0)

    # Build the data summary lines
    products_line = (
        f"✅ **Products table** — {products_count} {'product' if products_count == 1 else 'products'} loaded for {supplier}"
        if products_count > 0
        else f"❌ **Products table** — No products found for {supplier}"
    )

    distro_line = (
        f"✅ **Distro Grid** — {distro_count} store {'placement' if distro_count == 1 else 'placements'} exist for {supplier}"
        if distro_count > 0
        else f"❌ **Distro Grid** — No store placements found for {supplier}"
    )

    sc_line = (
        f"✅ **Supplier County** — {supplier_county_count} county {'authorization' if supplier_county_count == 1 else 'authorizations'} on file"
        if supplier_county_count > 0
        else f"❌ **Supplier County** — 0 entries — {supplier} has no county authorizations on file"
    )

    gap_line = (
        f"✅ **Gap Report** — {gap_report_count} {'row' if gap_report_count == 1 else 'rows'} found for {supplier}"
        if gap_report_count > 0
        else f"❌ **Gap Report** — 0 rows — {supplier} does not appear in gap analysis"
    )

    summary = (
        f"Here's what the investigation found:\n"
        f"- {products_line}\n"
        f"- {distro_line}\n"
        f"- {sc_line}\n"
        f"- {gap_line}\n"
    )

    # ── ROOT CAUSE DETERMINATION ──────────────────────────────────────────────

    # Root Cause 1: No products loaded at all
    if products_count == 0:
        return (
            f"❌ **Root cause found: {supplier} has no products in your Products table.**\n\n"
            f"{summary}\n"
            f"**Why this matters:** If a supplier has no products loaded, they cannot "
            f"appear anywhere in Chainlink — not in the Distro Grid, not in the gap report.\n\n"
            f"**Fix:** Upload a Products file that includes {supplier} entries."
        )

    # Root Cause 2: No Supplier County entries
    if supplier_county_count == 0:
        return (
            f"❌ **Root cause found: {supplier} is missing from your Supplier County table.**\n\n"
            f"{summary}\n"
            f"**Why this matters:** The gap report only includes suppliers that are "
            f"authorized to sell in at least one county. Since {supplier} has no county "
            f"authorizations on file, they are completely invisible to gap reporting — "
            f"even though they have {products_count} products and {distro_count} store placements.\n\n"
            f"**Fix:** Add {supplier} to your Supplier County table with the appropriate "
            f"counties and STATUS = 'Yes', then re-run the gap report stored procedure."
        )

    # Root Cause 3: Supplier County exists but gap report still empty
    if supplier_county_count > 0 and gap_report_count == 0 and distro_count == 0:
        return (
            f"❌ **Root cause found: {supplier} has county authorizations but no Distro Grid placements.**\n\n"
            f"{summary}\n"
            f"**Why this matters:** The gap report requires a supplier's products to appear "
            f"in the Distro Grid with YES_NO = 1 for at least one store. {supplier} is "
            f"authorized to sell in {supplier_county_count} "
            f"{'county' if supplier_county_count == 1 else 'counties'} but has no active "
            f"store placements in the Distro Grid.\n\n"
            f"**Fix:** Upload a Distro Grid file that includes {supplier} products "
            f"with YES_NO = 1 for the appropriate stores, then re-run the gap report "
            f"stored procedure."
        )

    # Root Cause 4: Everything exists but gap report temp table is empty
    if supplier_county_count > 0 and distro_count > 0 and gap_report_count == 0:
        return (
            f"⚠️ **Root cause found: Gap report tables need to be refreshed.**\n\n"
            f"{summary}\n"
            f"**Why this matters:** {supplier} has {products_count} products, "
            f"{distro_count} Distro Grid placements, and {supplier_county_count} county "
            f"authorizations — all the data is there. But the gap report temp tables "
            f"are showing 0 rows, which means the stored procedure has not been run "
            f"since the last data upload.\n\n"
            f"**Fix:** Re-run the `PROCESS_GAP_REPORT` stored procedure to rebuild "
            f"the gap report tables, then try your question again."
        )

    # Root Cause 5: Everything looks good — gap report has data
    if gap_report_count > 0:
        return (
            f"✅ **{supplier} is present in the gap report.**\n\n"
            f"{summary}\n"
            f"**What this means:** {supplier} has {gap_report_count} eligible "
            f"product-store combinations in the gap report. If you are not seeing "
            f"them in your gap report view, check your filters — you may be filtering "
            f"by county, salesperson, or purchase status in a way that excludes them.\n\n"
            f"Try asking: \"Show me the full gap report for {supplier}\" to see all rows."
        )

    # Fallback — unknown state
    return (
        f"⚠️ **Could not determine a clear root cause for {supplier}.**\n\n"
        f"{summary}\n"
        f"**Recommendation:** Review the Supplier County table and re-run the "
        f"`PROCESS_GAP_REPORT` stored procedure, then try your question again."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Supplier diagnostic sequence
# ─────────────────────────────────────────────────────────────────────────────

def run_diagnostic(
    original_question: str,
    generated_sql: str | None,
    conn,
    tenant_id: str,
    extracted_supplier: str | None = None,
) -> tuple[str, dict]:
    """
    Run the zero-results diagnostic sequence.
    Returns (narrative_str, counts_dict).
    generated_sql is None when called directly from DIAGNOSTIC intent.
    counts_dict is empty when no supplier was identified.
    """
    if not extracted_supplier:
        return run_general_diagnostic(conn, tenant_id), {}

    supplier = extracted_supplier
    pattern = f"%{supplier}%"

    df = _run_diag_query(
        conn,
        _COUNT_QUERY,
        (pattern, tenant_id, pattern, tenant_id, pattern, tenant_id, pattern),
    )

    if df.empty:
        counts = {
            "PRODUCTS": -1,
            "DISTRO_GRID": -1,
            "SUPPLIER_COUNTY": -1,
            "GAP_REPORT_TMP": -1,
        }
    else:
        counts = {row["TABLE_NAME"]: int(row["CNT"]) for _, row in df.iterrows()}
        for key in ("PRODUCTS", "DISTRO_GRID", "SUPPLIER_COUNTY", "GAP_REPORT_TMP"):
            counts.setdefault(key, -1)

    return build_narrative(supplier, counts), counts
