# utils/gap_report_builder.py
"""
Gap Report Builder (Streamlit-free)

Page overview for future devs:
- Runs PROCESS_GAP_REPORT() using the provided tenant connection.
- Reads from GAP_REPORT view/table with optional filters.
- Writes a temp Excel file and returns the filepath.

Hard rules:
- NO streamlit imports / st.session_state.
- Uses bind params for filters (no f-string SQL injection).
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from typing import Optional

import pandas as pd


def create_gap_report(
    conn,
    salesperson: str = "All",
    chain: str = "All",
    supplier: str = "All",
    *,
    proc_fqn: Optional[str] = None,
    view_fqn: Optional[str] = None,
) -> str:
    """
    Build a gap report Excel file from the GAP_REPORT view.

    Args:
        conn: Tenant Snowflake connection (already scoped to tenant DB/Schema context).
        salesperson, chain, supplier: Filter values or 'All'.
        proc_fqn: Optional fully-qualified stored procedure name.
                  Example: 'MYDB.MYSCHEMA.PROCESS_GAP_REPORT'
                  If None, calls 'PROCESS_GAP_REPORT()' in current context.
        view_fqn: Optional fully-qualified view/table name.
                  Example: 'MYDB.MYSCHEMA.GAP_REPORT'
                  If None, uses 'GAP_REPORT' in current context.

    Returns:
        Path to generated .xlsx file.
    """
    proc_sql = f"CALL {proc_fqn}()" if proc_fqn else "CALL PROCESS_GAP_REPORT()"
    view_name = view_fqn or "GAP_REPORT"

    # 1) Run procedure (unfiltered)
    cur = conn.cursor()
    try:
        cur.execute(proc_sql)
    finally:
        cur.close()

    # 2) Build filtered SELECT using bind params
    where = []
    params = []

    if salesperson and salesperson != "All":
        where.append("SALESPERSON = %s")
        params.append(salesperson)

    if chain and chain != "All":
        where.append("CHAIN_NAME = %s")
        params.append(chain)

    if supplier and supplier != "All":
        where.append("SUPPLIER = %s")
        params.append(supplier)

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM {view_name} {where_clause}"

    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        df = cur.fetch_pandas_all()
    finally:
        cur.close()

    # 3) Write temp Excel
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fd, path = tempfile.mkstemp(prefix=f"gap_report_{ts}_", suffix=".xlsx")
    os.close(fd)

    df.to_excel(path, index=False)
    return path

