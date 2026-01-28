# ---------------- utils/sales_contacts.py ----------------
# -*- coding: utf-8 -*-
"""
Sales Contacts helpers (Streamlit-aware, UI-free)

Overview for future devs
------------------------
Centralized helpers around the SALES_CONTACTS table.
Used by:
  - Email Gap Report (resolve salesperson + manager emails)
  - Admin UI pages (display + lookup)
  - Salesperson reassignment (update operational tables)

Design rules
------------
- This module must NOT render Streamlit UI (no st.error/st.warning). Raise exceptions instead.
- It MAY read tenant_id from st.session_state for convenience, but all public functions accept
  an explicit tenant_id so they can be used outside Streamlit.
- Prefer cursor.execute + fetch to avoid Snowflake/pandas edge cases.

SALES_CONTACTS structure (per-tenant DB)
----------------------------------------
SALES_CONTACTS (
    TENANT_ID         NUMBER(10,0)   NOT NULL,
    SALESPERSON_ID    NUMBER(10,0),
    SALESPERSON_NAME  STRING         NOT NULL,
    SALESPERSON_EMAIL STRING         NOT NULL,
    MANAGER_ID        NUMBER(10,0),
    MANAGER_NAME      STRING,
    MANAGER_EMAIL     STRING,
    MANAGER_EMAIL_2   STRING,                -- optional, but supported
    EXTRA_CC_EMAIL    STRING,                -- optional, but supported
    IS_ACTIVE         BOOLEAN        DEFAULT TRUE,
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ
)

Key matching
------------
- Admin UX upserts by (TENANT_ID + UPPER(SALESPERSON_NAME)).
- This module supports:
    * upsert_contact_by_name (recommended / matches admin)
    * upsert_contact_by_id   (legacy / optional)

Salesperson reassignment behavior (IMPORTANT)
---------------------------------------------
Reassignment should update OPERATIONAL tables (CUSTOMERS, GAP tables, etc.)
and should NOT rename the old SALES_CONTACTS row by default.

Correct behavior:
- Update operational tables: OLD -> NEW
- Ensure NEW contact exists/active (admin page can update details)
- Deactivate OLD contact (kept for history, won't receive emails)

So:
- apply_salesperson_reassignment(update_sales_contacts=False) is the default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st


# =============================================================================
# Errors
# =============================================================================

class SalesContactsError(RuntimeError):
    """Base exception for SALES_CONTACTS helper failures."""


class MissingTenantIdError(SalesContactsError):
    """Raised when tenant_id is required but cannot be resolved."""


class InvalidInputError(SalesContactsError):
    """Raised when required function inputs are missing/invalid."""


# =============================================================================
# Data model (optional but helpful)
# =============================================================================

@dataclass(frozen=True)
class SalesContact:
    """
    Canonical SalesContact record.

    Notes:
    - Not all columns are always present in the table (MANAGER_EMAIL_2 / EXTRA_CC_EMAIL
      are optional depending on your schema migration). Fetch helpers are defensive.
    """
    tenant_id: int
    salesperson_name: str
    salesperson_email: str
    is_active: bool = True

    salesperson_id: Optional[int] = None
    manager_id: Optional[int] = None
    manager_name: Optional[str] = None
    manager_email: Optional[str] = None
    manager_email_2: Optional[str] = None
    extra_cc_email: Optional[str] = None


# =============================================================================
# Small utilities
# =============================================================================

def _resolve_tenant_id(tenant_id: Optional[int]) -> int:
    """
    Resolve tenant_id.

    Priority:
    1) explicit tenant_id argument
    2) st.session_state["tenant_id"]

    Raises:
        MissingTenantIdError
    """
    if tenant_id is not None:
        return int(tenant_id)

    ss_val = st.session_state.get("tenant_id")
    if ss_val is None:
        raise MissingTenantIdError(
            "tenant_id not provided and not found in st.session_state['tenant_id']."
        )
    return int(ss_val)


def _fetch_df(cur) -> pd.DataFrame:
    """Build a DataFrame from the active cursor results."""
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _fetch_one_dict(cur) -> Optional[Dict[str, Any]]:
    """Fetch one row and return as dict, or None."""
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description] if cur.description else []
    return dict(zip(cols, row))


def _req_str(val: str, field: str) -> str:
    """Required string validator."""
    s = (val or "").strip()
    if not s:
        raise InvalidInputError(f"{field} is required.")
    return s


def _normalize_salesperson_label(name: str) -> str:
    """
    Normalize salesperson labels to match your pipeline.

    Your system tends to store salesperson labels as uppercase strings.
    """
    return (name or "").strip().upper()


def _qualify_ident(ident: str) -> str:
    """
    Minimal identifier guardrail.

    We do not accept arbitrary SQL here. Table/column names in this module
    are owned by your code, not user input.
    """
    s = (ident or "").strip()
    if not s:
        raise InvalidInputError("Identifier is required.")
    # Allow letters, numbers, underscore, dot (schema.table), and quotes if you use them.
    # If you need more, expand intentionally.
    bad = any(ch in s for ch in [";", "--", "/*", "*/"])
    if bad:
        raise InvalidInputError(f"Unsafe identifier: {ident}")
    return s


# =============================================================================
# Column discovery (defensive for optional columns)
# =============================================================================

def table_columns(conn) -> List[str]:
    """
    Return column names for SALES_CONTACTS in the current schema.

    Defensive for optional columns (MANAGER_EMAIL_2 / EXTRA_CC_EMAIL).
    """
    with conn.cursor() as cur:
        cur.execute("DESC TABLE SALES_CONTACTS")
        df = _fetch_df(cur)

    if df.empty:
        return []

    for col_name in ["name", "NAME"]:
        if col_name in df.columns:
            return [str(x).strip().upper() for x in df[col_name].tolist()]

    # fallback: first column
    return [str(x).strip().upper() for x in df.iloc[:, 0].tolist()]


def _select_cols_for_fetch(conn) -> List[str]:
    """Build a safe SELECT column list based on the table's current schema."""
    cols = set(table_columns(conn))

    base = [
        "TENANT_ID",
        "SALESPERSON_ID",
        "SALESPERSON_NAME",
        "SALESPERSON_EMAIL",
        "MANAGER_ID",
        "MANAGER_NAME",
        "MANAGER_EMAIL",
        "IS_ACTIVE",
        "CREATED_AT",
        "UPDATED_AT",
    ]

    if "MANAGER_EMAIL_2" in cols:
        base.insert(base.index("MANAGER_EMAIL") + 1, "MANAGER_EMAIL_2")

    if "EXTRA_CC_EMAIL" in cols:
        insert_pos = base.index("MANAGER_EMAIL") + (2 if "MANAGER_EMAIL_2" in cols else 1)
        base.insert(insert_pos, "EXTRA_CC_EMAIL")

    return base


# =============================================================================
# Public fetchers
# =============================================================================

def fetch_sales_contacts(
    conn,
    tenant_id: Optional[int] = None,
    *,
    active_only: bool = False,
) -> pd.DataFrame:
    """
    Fetch SALES_CONTACTS rows for a tenant.

    Returns:
        DataFrame ordered by UPPER(SALESPERSON_NAME)
    """
    tid = _resolve_tenant_id(tenant_id)
    select_cols = _select_cols_for_fetch(conn)

    sql = f"""
        SELECT
            {", ".join(select_cols)}
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
    """
    params: List[Any] = [tid]

    if active_only:
        sql += " AND IS_ACTIVE = TRUE"

    sql += " ORDER BY UPPER(SALESPERSON_NAME);"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return _fetch_df(cur)


def lookup_contact_by_salesperson_name(
    conn,
    salesperson_name: str,
    tenant_id: Optional[int] = None,
    *,
    active_only: bool = True,
) -> Optional[Dict[str, Any]]:
    """Lookup one contact by salesperson name (case-insensitive)."""
    tid = _resolve_tenant_id(tenant_id)
    name = (salesperson_name or "").strip()
    if not name:
        return None

    select_cols = _select_cols_for_fetch(conn)
    cols_no_audit = [c for c in select_cols if c not in {"CREATED_AT", "UPDATED_AT"}]

    sql = f"""
        SELECT
            {", ".join(cols_no_audit)}
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND UPPER(SALESPERSON_NAME) = UPPER(%s)
    """
    params: List[Any] = [tid, name]

    if active_only:
        sql += " AND IS_ACTIVE = TRUE"

    sql += """
        QUALIFY ROW_NUMBER() OVER (
            ORDER BY UPDATED_AT DESC NULLS LAST, CREATED_AT DESC NULLS LAST
        ) = 1
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return _fetch_one_dict(cur)


def lookup_contact_by_salesperson_email(
    conn,
    salesperson_email: str,
    tenant_id: Optional[int] = None,
    *,
    active_only: bool = True,
) -> Optional[Dict[str, Any]]:
    """Lookup one contact by salesperson email (case-insensitive)."""
    tid = _resolve_tenant_id(tenant_id)
    email = (salesperson_email or "").strip()
    if not email:
        return None

    select_cols = _select_cols_for_fetch(conn)
    cols_no_audit = [c for c in select_cols if c not in {"CREATED_AT", "UPDATED_AT"}]

    sql = f"""
        SELECT
            {", ".join(cols_no_audit)}
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND UPPER(SALESPERSON_EMAIL) = UPPER(%s)
    """
    params: List[Any] = [tid, email]

    if active_only:
        sql += " AND IS_ACTIVE = TRUE"

    sql += """
        QUALIFY ROW_NUMBER() OVER (
            ORDER BY UPDATED_AT DESC NULLS LAST, CREATED_AT DESC NULLS LAST
        ) = 1
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return _fetch_one_dict(cur)


# =============================================================================
# Upserts / updates (name-key)
# =============================================================================

def upsert_contact_by_name(
    conn,
    *,
    tenant_id: int,
    salesperson_name: str,
    salesperson_email: str,
    manager_name: Optional[str] = None,
    manager_email: Optional[str] = None,
    manager_email_2: Optional[str] = None,
    extra_cc_email: Optional[str] = None,
    is_active: bool = True,
) -> None:
    """
    Upsert a contact keyed by (TENANT_ID, UPPER(SALESPERSON_NAME)).
    """
    tid = int(tenant_id)
    name = _req_str(salesperson_name, "salesperson_name")
    email = _req_str(salesperson_email, "salesperson_email")

    cols = set(table_columns(conn))
    has_mgr2 = "MANAGER_EMAIL_2" in cols
    has_extra = "EXTRA_CC_EMAIL" in cols

    insert_cols = [
        "TENANT_ID",
        "SALESPERSON_NAME",
        "SALESPERSON_EMAIL",
        "MANAGER_NAME",
        "MANAGER_EMAIL",
        "IS_ACTIVE",
    ]
    insert_vals = [
        "src.TENANT_ID",
        "src.SALESPERSON_NAME",
        "src.SALESPERSON_EMAIL",
        "src.MANAGER_NAME",
        "src.MANAGER_EMAIL",
        "src.IS_ACTIVE",
    ]
    update_sets = [
        "tgt.SALESPERSON_EMAIL = src.SALESPERSON_EMAIL",
        "tgt.MANAGER_NAME      = src.MANAGER_NAME",
        "tgt.MANAGER_EMAIL     = src.MANAGER_EMAIL",
        "tgt.IS_ACTIVE         = src.IS_ACTIVE",
        "tgt.UPDATED_AT        = CURRENT_TIMESTAMP()",
    ]

    src_select_parts = [
        "%s AS TENANT_ID",
        "%s AS SALESPERSON_NAME",
        "%s AS SALESPERSON_EMAIL",
        "%s AS MANAGER_NAME",
        "%s AS MANAGER_EMAIL",
        "%s AS IS_ACTIVE",
    ]
    params: List[Any] = [
        tid,
        name,
        email,
        (manager_name or None),
        (manager_email or None),
        bool(is_active),
    ]

    if has_mgr2:
        src_select_parts.append("%s AS MANAGER_EMAIL_2")
        params.append(manager_email_2 or None)
        insert_cols.append("MANAGER_EMAIL_2")
        insert_vals.append("src.MANAGER_EMAIL_2")
        update_sets.insert(3, "tgt.MANAGER_EMAIL_2  = src.MANAGER_EMAIL_2")

    if has_extra:
        src_select_parts.append("%s AS EXTRA_CC_EMAIL")
        params.append(extra_cc_email or None)
        insert_cols.append("EXTRA_CC_EMAIL")
        insert_vals.append("src.EXTRA_CC_EMAIL")
        insert_at = 4 if has_mgr2 else 3
        update_sets.insert(insert_at, "tgt.EXTRA_CC_EMAIL   = src.EXTRA_CC_EMAIL")

    sql = f"""
        MERGE INTO SALES_CONTACTS AS tgt
        USING (
            SELECT
                {", ".join(src_select_parts)}
        ) AS src
        ON  tgt.TENANT_ID = src.TENANT_ID
        AND UPPER(tgt.SALESPERSON_NAME) = UPPER(src.SALESPERSON_NAME)
        WHEN MATCHED THEN UPDATE SET
            {", ".join(update_sets)}
        WHEN NOT MATCHED THEN INSERT (
            {", ".join(insert_cols)}
        ) VALUES (
            {", ".join(insert_vals)}
        )
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)


def deactivate_contact_by_name(conn, *, tenant_id: int, salesperson_name: str) -> None:
    """Soft-deactivate a contact by (TENANT_ID, UPPER(SALESPERSON_NAME))."""
    tid = int(tenant_id)
    name = _req_str(salesperson_name, "salesperson_name")

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE SALES_CONTACTS
               SET IS_ACTIVE = FALSE,
                   UPDATED_AT = CURRENT_TIMESTAMP()
             WHERE TENANT_ID = %s
               AND UPPER(SALESPERSON_NAME) = UPPER(%s)
            """,
            (tid, name),
        )


# =============================================================================
# Legacy / optional: upsert + deactivate by salesperson_id
# =============================================================================

def upsert_contact_by_id(
    conn,
    *,
    tenant_id: int,
    salesperson_id: int,
    salesperson_name: str,
    salesperson_email: str,
    manager_id: Optional[int] = None,
    manager_name: Optional[str] = None,
    manager_email: Optional[str] = None,
    manager_email_2: Optional[str] = None,
    extra_cc_email: Optional[str] = None,
    is_active: bool = True,
) -> None:
    """
    Legacy upsert keyed by (TENANT_ID, SALESPERSON_ID).
    """
    tid = int(tenant_id)
    sid = int(salesperson_id)
    name = _req_str(salesperson_name, "salesperson_name")
    email = _req_str(salesperson_email, "salesperson_email")

    cols = set(table_columns(conn))
    has_mgr2 = "MANAGER_EMAIL_2" in cols
    has_extra = "EXTRA_CC_EMAIL" in cols

    update_sets = [
        "SALESPERSON_NAME  = %s",
        "SALESPERSON_EMAIL = %s",
        "MANAGER_ID        = %s",
        "MANAGER_NAME      = %s",
        "MANAGER_EMAIL     = %s",
    ]
    update_params: List[Any] = [
        name,
        email,
        (int(manager_id) if manager_id is not None else None),
        (manager_name or None),
        (manager_email or None),
    ]

    insert_cols = [
        "TENANT_ID",
        "SALESPERSON_ID",
        "SALESPERSON_NAME",
        "SALESPERSON_EMAIL",
        "MANAGER_ID",
        "MANAGER_NAME",
        "MANAGER_EMAIL",
        "IS_ACTIVE",
        "CREATED_AT",
        "UPDATED_AT",
    ]
    insert_vals = [
        "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",
        "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP()",
    ]
    insert_params: List[Any] = [
        tid,
        sid,
        name,
        email,
        (int(manager_id) if manager_id is not None else None),
        (manager_name or None),
        (manager_email or None),
        bool(is_active),
    ]

    if has_mgr2:
        update_sets.append("MANAGER_EMAIL_2   = %s")
        update_params.append(manager_email_2 or None)
        # NOTE: if you truly use this path, revisit the placeholder indexing logic.
        insert_cols.insert(insert_cols.index("IS_ACTIVE"), "MANAGER_EMAIL_2")
        insert_vals.insert(7, "%s")
        insert_params.insert(7, manager_email_2 or None)

    if has_extra:
        update_sets.append("EXTRA_CC_EMAIL    = %s")
        update_params.append(extra_cc_email or None)
        insert_cols.insert(insert_cols.index("IS_ACTIVE"), "EXTRA_CC_EMAIL")
        insert_vals.insert(7 + (1 if has_mgr2 else 0), "%s")
        insert_params.insert(7 + (1 if has_mgr2 else 0), extra_cc_email or None)

    update_sets.append("IS_ACTIVE         = %s")
    update_sets.append("UPDATED_AT        = CURRENT_TIMESTAMP()")
    update_params.append(bool(is_active))

    sql = f"""
        MERGE INTO SALES_CONTACTS tgt
        USING (
            SELECT
                %s AS TENANT_ID,
                %s AS SALESPERSON_ID
        ) src
        ON tgt.TENANT_ID = src.TENANT_ID
       AND tgt.SALESPERSON_ID = src.SALESPERSON_ID
        WHEN MATCHED THEN UPDATE SET
            {", ".join(update_sets)}
        WHEN NOT MATCHED THEN INSERT (
            {", ".join(insert_cols)}
        ) VALUES (
            {", ".join(insert_vals)}
        )
    """

    params: List[Any] = [tid, sid] + update_params + insert_params

    with conn.cursor() as cur:
        cur.execute(sql, params)


def deactivate_contact_by_id(conn, *, tenant_id: int, salesperson_id: int) -> None:
    """Soft-deactivate a contact keyed by (TENANT_ID, SALESPERSON_ID)."""
    tid = int(tenant_id)
    sid = int(salesperson_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE SALES_CONTACTS
               SET IS_ACTIVE = FALSE,
                   UPDATED_AT = CURRENT_TIMESTAMP()
             WHERE TENANT_ID = %s
               AND SALESPERSON_ID = %s
            """,
            (tid, sid),
        )


# =============================================================================
# Reassignment logic (operational tables)
# =============================================================================

# IMPORTANT:
# When adding new operational tables that contain salesperson labels,
# they MUST be added here or reassignment will silently skip them.


REASSIGNMENT_TABLE_MAP: Dict[str, str] = {
    "CUSTOMERS": "SALESPERSON",
    "EXECUTION_SUMMARY_TMP": "SALESPERSON",
    "EXECUTION_SUMMARY_TMP2": "SALESPERSON",
    "GAP_REPORT_SNAPSHOT": "SALESPERSON_NAME",
    "GAP_REPORT_TMP": "SALESPERSON",
    "GAP_REPORT_TMP2": "SALESPERSON",
    "SALESPERSON_EXECUTION_SUMMARY_TBL": "SALESPERSON",
    "SALES_REPORT": "SALESPERSON",
}


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    """
    Return True if the table has the column (case-insensitive).

    Uses DESC TABLE to avoid INFORMATION_SCHEMA issues/permissions.
    """
    table_name = _qualify_ident(table_name)
    col = (column_name or "").strip().upper()
    if not col:
        return False

    with conn.cursor() as cur:
        cur.execute(f"DESC TABLE {table_name}")
        rows = cur.fetchall()

    cols = {str(r[0]).strip().upper() for r in rows} if rows else set()
    return col in cols


def preview_salesperson_reassignment(
    conn,
    *,
    tenant_id: int,
    old_salesperson: str,
    table_map: Dict[str, str] = REASSIGNMENT_TABLE_MAP,
) -> Dict[str, int]:
    """
    Return counts by table of rows that would be updated for the old salesperson label.
    """
    tid = int(tenant_id)
    old_norm = _normalize_salesperson_label(old_salesperson)
    if not old_norm:
        raise InvalidInputError("old_salesperson is required.")

    results: Dict[str, int] = {}

    with conn.cursor() as cur:
        for table_name, col_name in table_map.items():
            table_name = _qualify_ident(table_name)
            col_name = _qualify_ident(col_name)

            has_tenant = _table_has_column(conn, table_name, "TENANT_ID")

            if has_tenant:
                sql = f"""
                    SELECT COUNT(*)
                      FROM {table_name}
                     WHERE TENANT_ID = %s
                       AND UPPER({col_name}) = UPPER(%s)
                """
                params = (tid, old_norm)
            else:
                sql = f"""
                    SELECT COUNT(*)
                      FROM {table_name}
                     WHERE UPPER({col_name}) = UPPER(%s)
                """
                params = (old_norm,)

            cur.execute(sql, params)
            results[table_name] = int(cur.fetchone()[0])

    return results


def apply_salesperson_reassignment(
    conn,
    *,
    tenant_id: int,
    old_salesperson: str,
    new_salesperson: str,
    table_map: Dict[str, str] = REASSIGNMENT_TABLE_MAP,
    update_sales_contacts: bool = False,  # ✅ SAFE DEFAULT
) -> Dict[str, int]:
    """
    Update operational tables replacing old salesperson label with new label.

    By default, this DOES NOT rename SALES_CONTACTS rows.
    If update_sales_contacts=True:
      - ensures the new salesperson contact is active (does not overwrite email fields)
      - deactivates old salesperson contact
    """
    tid = int(tenant_id)
    old_norm = _normalize_salesperson_label(old_salesperson)
    new_norm = _normalize_salesperson_label(new_salesperson)

    if not old_norm:
        raise InvalidInputError("old_salesperson is required.")
    if not new_norm:
        raise InvalidInputError("new_salesperson is required.")
    if old_norm == new_norm:
        raise InvalidInputError("old_salesperson and new_salesperson are the same after normalization.")

    updated_counts: Dict[str, int] = {}

    with conn.cursor() as cur:
        for table_name, col_name in table_map.items():
            table_name = _qualify_ident(table_name)
            col_name = _qualify_ident(col_name)

            has_tenant = _table_has_column(conn, table_name, "TENANT_ID")

            if has_tenant:
                sql = f"""
                    UPDATE {table_name}
                       SET {col_name} = %s
                     WHERE TENANT_ID = %s
                       AND UPPER({col_name}) = UPPER(%s)
                """
                params = (new_norm, tid, old_norm)
            else:
                sql = f"""
                    UPDATE {table_name}
                       SET {col_name} = %s
                     WHERE UPPER({col_name}) = UPPER(%s)
                """
                params = (new_norm, old_norm)

            cur.execute(sql, params)
            updated_counts[table_name] = int(cur.rowcount or 0)

        if update_sales_contacts:
            # DO NOT rename the old row to the new name.
            # Instead: deactivate old and ensure new is active.
            cur.execute(
                """
                UPDATE SALES_CONTACTS
                   SET IS_ACTIVE = FALSE,
                       UPDATED_AT = CURRENT_TIMESTAMP()
                 WHERE TENANT_ID = %s
                   AND UPPER(SALESPERSON_NAME) = UPPER(%s)
                """,
                (tid, old_norm),
            )
            updated_counts["SALES_CONTACTS_DEACTIVATED"] = int(cur.rowcount or 0)

            cur.execute(
                """
                UPDATE SALES_CONTACTS
                   SET IS_ACTIVE = TRUE,
                       UPDATED_AT = CURRENT_TIMESTAMP()
                 WHERE TENANT_ID = %s
                   AND UPPER(SALESPERSON_NAME) = UPPER(%s)
                """,
                (tid, new_norm),
            )
            updated_counts["SALES_CONTACTS_ACTIVATED"] = int(cur.rowcount or 0)

    

    return updated_counts
