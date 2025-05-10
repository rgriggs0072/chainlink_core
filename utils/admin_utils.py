# utils/admin_utils.py

import pandas as pd
import snowflake.connector

def fetch_reset_logs(conn, tenant_id):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT EMAIL, RESET_TOKEN, SUCCESS, TIMESTAMP, IP_ADDRESS, REASON
            FROM TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
            WHERE TENANT_ID = %s
            ORDER BY TIMESTAMP DESC
        """, (tenant_id,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        return pd.DataFrame(columns=["Error"], data=[[str(e)]])

