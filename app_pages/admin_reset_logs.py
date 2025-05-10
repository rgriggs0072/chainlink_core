
import streamlit as st
import pandas as pd
import snowflake.connector
from sf_connector.service_connector import get_service_account_connection

# --- Admin: Reset Logs Viewer ---
def render():
   # st.title("🔐 Password Reset Logs")
    st.markdown("View recent password reset attempts across your tenant.")

    try:
        conn = get_service_account_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT ID, TENANT_ID, EMAIL, RESET_TOKEN, SUCCESS, TIMESTAMP, IP_ADDRESS, REASON
            FROM TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
            ORDER BY TIMESTAMP DESC
            LIMIT 500
        """)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=columns)

        # Optional filters
        with st.expander("🔍 Filter Results"):
            selected_email = st.text_input("Filter by Email")
            success_filter = st.selectbox("Success Status", ["All", "Success", "Failure"])

            if selected_email:
                df = df[df["EMAIL"].str.contains(selected_email, case=False)]

            if success_filter == "Success":
                df = df[df["SUCCESS"] == True]
            elif success_filter == "Failure":
                df = df[df["SUCCESS"] == False]

        st.dataframe(df, use_container_width=True)

    except Exception as e:
        st.error("Failed to load reset logs.")
        st.exception(e)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
