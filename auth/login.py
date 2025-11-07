import streamlit as st
from sf_connector.service_connector import get_service_account_connection

def fetch_user_credentials():
    conn = get_service_account_connection()
    cur = conn.cursor()
    query = """
        SELECT TENANT_ID, EMAIL, HASHED_PASSWORD
        FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    credentials = {"usernames": {}}  # ? REQUIRED FORMAT
    for tenant_id, email, hashed_pw in rows:
        email_lc = email.lower()
        credentials["usernames"][email_lc] = {
            "name": email_lc.split("@")[0].replace(".", " ").title(),
            "email": email_lc,
            "password": hashed_pw,
            "tenant_id": tenant_id
        }

    return credentials
