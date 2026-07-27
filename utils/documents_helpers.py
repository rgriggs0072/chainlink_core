# ----------- utils/documents_helpers.py -----------
"""
Documents Library helpers.

DOCUMENTS lives in TENANTUSERDB.CHAINLINK_SCH — platform-level content,
not tenant-specific data — so it's queried with a fully-qualified path
rather than the tenant's own database/schema. TENANT_ID on each row is
nullable: NULL means visible to all tenants, a specific tenant_id means
visible to that tenant only.
"""

import pandas as pd


def fetch_documents(conn, tenant_id: str) -> list[dict]:
    """
    Fetch all active documents visible to the given tenant.

    Documents with TENANT_ID = NULL are shown to all tenants. Documents
    with a specific TENANT_ID are shown only to that tenant.

    Args:
        conn:      Active Snowflake connection (tenant-scoped).
        tenant_id: Current tenant identifier.

    Returns:
        List of dicts with keys: title, description, category,
        file_name, display_order. Ordered by category, then
        display_order.
    """
    query = """
        SELECT TITLE, DESCRIPTION, CATEGORY, FILE_NAME, DISPLAY_ORDER
        FROM TENANTUSERDB.CHAINLINK_SCH.DOCUMENTS
        WHERE ACTIVE = TRUE
          AND (TENANT_ID IS NULL OR TENANT_ID = %s)
        ORDER BY CATEGORY, DISPLAY_ORDER
    """
    cur = conn.cursor()
    try:
        cur.execute(query, (tenant_id,))
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    finally:
        cur.close()

    df = pd.DataFrame(rows, columns=cols)

    return [
        {
            "title": row.TITLE,
            "description": row.DESCRIPTION,
            "category": row.CATEGORY,
            "file_name": row.FILE_NAME,
            "display_order": row.DISPLAY_ORDER,
        }
        for row in df.itertuples(index=False)
    ]
