# utils/customers_helpers.py
# -*- coding: utf-8 -*-
"""
Customers upload helpers — ownership change detection and SALES_CONTACTS coverage.

No Streamlit imports. Pure data logic only.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd


def detect_ownership_changes(
    con,
    tenant_id,
    new_customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compare incoming Customers data against the current CUSTOMERS table.

    Call this BEFORE the TRUNCATE so CUSTOMERS still holds the old data.

    Returns a DataFrame with columns:
        CHAIN_NAME, STORE_NUMBER, OLD_SALESPERSON, NEW_SALESPERSON
    Empty DataFrame if no changes detected or if CUSTOMERS is empty (first upload).
    """
    sql = """
        SELECT CHAIN_NAME, STORE_NUMBER, SALESPERSON
        FROM CUSTOMERS
        WHERE TENANT_ID = %s
    """
    with con.cursor() as cur:
        cur.execute(sql, (str(tenant_id),))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []

    if not rows:
        return pd.DataFrame()

    current_df = pd.DataFrame(rows, columns=cols)

    current_df["CHAIN_KEY"] = current_df["CHAIN_NAME"].fillna("").str.upper().str.strip()
    current_df["SALESPERSON_NORM"] = current_df["SALESPERSON"].fillna("").str.upper().str.strip()

    new_df = new_customers_df.copy()
    new_df.columns = [str(c).strip().upper() for c in new_df.columns]
    new_df["CHAIN_KEY"] = new_df["CHAIN_NAME"].fillna("").str.upper().str.strip()
    new_df["SALESPERSON_NORM"] = new_df["SALESPERSON"].fillna("").str.upper().str.strip()

    # Join on CHAIN_KEY + STORE_NUMBER — inner join means only stores present in both
    merged = current_df.merge(
        new_df[["CHAIN_KEY", "STORE_NUMBER", "SALESPERSON_NORM", "SALESPERSON"]],
        on=["CHAIN_KEY", "STORE_NUMBER"],
        suffixes=("_OLD", "_NEW"),
        how="inner",
    )

    changed = merged[
        merged["SALESPERSON_NORM_OLD"] != merged["SALESPERSON_NORM_NEW"]
    ].copy()

    if changed.empty:
        return pd.DataFrame()

    return (
        changed[["CHAIN_NAME", "STORE_NUMBER", "SALESPERSON_OLD", "SALESPERSON_NEW"]]
        .rename(columns={
            "SALESPERSON_OLD": "OLD_SALESPERSON",
            "SALESPERSON_NEW": "NEW_SALESPERSON",
        })
        .reset_index(drop=True)
    )


def log_ownership_changes(
    con,
    tenant_id,
    changes_df: pd.DataFrame,
    batch_id: Optional[str] = None,
) -> int:
    """
    Write detected ownership changes to SALESPERSON_CHANGE_LOG.

    Should be called AFTER the main CUSTOMERS insert succeeds but BEFORE commit,
    so both changes are atomic. SALESPERSON_CHANGE_LOG.TENANT_ID is VARCHAR.

    Returns the number of records written.
    """
    if changes_df is None or changes_df.empty:
        return 0

    sql = """
        INSERT INTO SALESPERSON_CHANGE_LOG
            (TENANT_ID, CHAIN_NAME, STORE_NUMBER, OLD_SALESPERSON, NEW_SALESPERSON, UPLOAD_BATCH_ID)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    records = [
        (
            str(tenant_id),
            str(row["CHAIN_NAME"]),
            int(row["STORE_NUMBER"]),
            str(row["OLD_SALESPERSON"]),
            str(row["NEW_SALESPERSON"]),
            batch_id,
        )
        for _, row in changes_df.iterrows()
    ]

    with con.cursor() as cur:
        cur.executemany(sql, records)

    return len(records)


def check_sales_contacts_coverage(
    con,
    tenant_id,
    changes_df: pd.DataFrame,
) -> List[str]:
    """
    Check whether each NEW_SALESPERSON in the ownership changes has an active
    SALES_CONTACTS entry. SALES_CONTACTS.TENANT_ID is NUMBER.

    Returns list of rep names (uppercased) missing from SALES_CONTACTS.
    Empty list means all new reps are covered.
    """
    if changes_df is None or changes_df.empty:
        return []

    new_reps = (
        changes_df["NEW_SALESPERSON"]
        .dropna()
        .str.upper()
        .str.strip()
        .unique()
        .tolist()
    )

    if not new_reps:
        return []

    sql = """
        SELECT UPPER(TRIM(SALESPERSON_NAME)) AS SALESPERSON_NAME_NORM
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND IS_ACTIVE = TRUE
    """
    with con.cursor() as cur:
        cur.execute(sql, (int(tenant_id),))
        rows = cur.fetchall()

    if not rows:
        return new_reps

    active_reps = {str(r[0]) for r in rows}
    return [rep for rep in new_reps if rep not in active_reps]
