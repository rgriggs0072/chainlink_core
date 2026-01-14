# ---------------- utils/sales_contacts.py ----------------
"""
Sales Contacts helpers (Streamlit-aware, UI-free)

Overview for future devs
------------------------
Centralized helpers around the SALES_CONTACTS table.
Used by:
  - Email Gap Report (resolve salesperson + manager emails)
  - Admin UI pages (display + lookup)

Design rules
------------
- This module must NOT render Streamlit UI (no st.error/st.warning). Raise exceptions instead.
- It MAY read tenant_id from st.session_state for convenience, but all public functions accept
  an explicit tenant_id so they can be used outside Streamlit.
- Prefer cursor.execute + fetch to avoid Snowflake/pandas edge cases.

Assumed table structure (per-tenant DB)
---------------------------------------
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
    * upsert_by_name (recommended / matches admin)
    * upsert_by_id   (legacy / optional)

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

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


# =============================================================================
# Column discovery (defensive for optional columns)
# =============================================================================

def table_columns(conn) -> List[str]:
    """
    Return column names for SALES_CONTACTS in the current schema.

    This lets us be defensive when MANAGER_EMAIL_2 / EXTRA_CC_EMAIL are not present.
    """
    with conn.cursor() as cur:
        cur.execute("DESC TABLE SALES_CONTACTS")
        df = _fetch_df(cur)
    if df.empty:
        return []
    # Snowflake DESC TABLE returns column name in "name" (lower) or "NAME" depending on driver
    for col_name in ["name", "NAME"]:
        if col_name in df.columns:
            return [str(x).upper() for x in df[col_name].tolist()]
    # fallback: first column
    return [str(x).upper() for x in df.iloc[:, 0].tolist()]


def _select_cols_for_fetch(conn) -> List[str]:
    """
    Build a safe SELECT column list based on the table's current schema.
    """
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

    # Optional columns
    if "MANAGER_EMAIL_2" in cols:
        base.insert(base.index("MANAGER_EMAIL") + 1, "MANAGER_EMAIL_2")
    if "EXTRA_CC_EMAIL" in cols:
        base.insert(base.index("MANAGER_EMAIL") + (2 if "MANAGER_EMAIL_2" in cols else 1), "EXTRA_CC_EMAIL")

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

    Args:
        conn: tenant Snowflake connection
        tenant_id: optional; resolved from session if not provided
        active_only: if True, only rows where IS_ACTIVE = TRUE

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
    """
    Lookup a single contact by salesperson name (case-insensitive).

    Returns:
        dict(row) or None
    """
    tid = _resolve_tenant_id(tenant_id)
    name = (salesperson_name or "").strip()
    if not name:
        return None

    select_cols = _select_cols_for_fetch(conn)

    sql = f"""
        SELECT
            {", ".join([c for c in select_cols if c not in {"CREATED_AT", "UPDATED_AT"}])}
        FROM SALES_CONTACTS
        WHERE TENANT_ID = %s
          AND UPPER(SALESPERSON_NAME) = UPPER(%s)
    """
    params: List[Any] = [tid, name]

    if active_only:
        sql += " AND IS_ACTIVE = TRUE"

    # Pick the most recently updated record if duplicates exist
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
    """
    Lookup a single contact by salesperson email (case-insensitive).

    Useful for de-dupe checks / admin tooling.

    Returns:
        dict(row) or None
    """
    tid = _resolve_tenant_id(tenant_id)
    email = (salesperson_email or "").strip()
    if not email:
        return None

    select_cols = _select_cols_for_fetch(conn)

    sql = f"""
        SELECT
            {", ".join([c for c in select_cols if c not in {"CREATED_AT", "UPDATED_AT"}])}
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

    This matches the admin page behavior and avoids reliance on SALESPERSON_ID.
    """
    tid = int(tenant_id)
    name = _req_str(salesperson_name, "salesperson_name")
    email = _req_str(salesperson_email, "salesperson_email")

    cols = set(table_columns(conn))
    has_mgr2 = "MANAGER_EMAIL_2" in cols
    has_extra = "EXTRA_CC_EMAIL" in cols

    # Build SQL dynamically so we don't reference columns that don't exist.
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
        update_sets.insert(3, "tgt.MANAGER_EMAIL_2  = src.MANAGER_EMAIL_2")  # before IS_ACTIVE

    if has_extra:
        src_select_parts.append("%s AS EXTRA_CC_EMAIL")
        params.append(extra_cc_email or None)
        insert_cols.append("EXTRA_CC_EMAIL")
        insert_vals.append("src.EXTRA_CC_EMAIL")
        # place before IS_ACTIVE update if mgr2 exists, otherwise still fine
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
    """
    Soft-deactivate a contact by (TENANT_ID, UPPER(SALESPERSON_NAME)).
    """
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

    Keep this only if you later decide to rely on numeric IDs.
    """
    tid = int(tenant_id)
    sid = int(salesperson_id)
    name = _req_str(salesperson_name, "salesperson_name")
    email = _req_str(salesperson_email, "salesperson_email")

    cols = set(table_columns(conn))
    has_mgr2 = "MANAGER_EMAIL_2" in cols
    has_extra = "EXTRA_CC_EMAIL" in cols

    # Dynamic insert/update to avoid referencing missing optional columns.
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
        # insert before IS_ACTIVE or anywhere, just keep order aligned
        insert_cols.insert(insert_cols.index("IS_ACTIVE"), "MANAGER_EMAIL_2")
        insert_vals.insert(insert_vals.index("%s", 0) + 7, "%s")  # after MANAGER_EMAIL placeholder
        insert_params.insert(7, manager_email_2 or None)

    if has_extra:
        update_sets.append("EXTRA_CC_EMAIL    = %s")
        update_params.append(extra_cc_email or None)
        insert_cols.insert(insert_cols.index("IS_ACTIVE"), "EXTRA_CC_EMAIL")
        insert_vals.insert(insert_vals.index("%s", 0) + (8 if has_mgr2 else 7), "%s")
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
