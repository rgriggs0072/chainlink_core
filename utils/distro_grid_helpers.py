
# ----------- utils/distro_grid_helpers.py -----------

from datetime import datetime, date
import getpass
import numpy as np
import pandas as pd
import streamlit as st
from sf_connector.service_connector import connect_to_tenant_snowflake


def format_non_pivot_table(workbook, stream=None, selected_option=None):
    df = pd.DataFrame(workbook.active.values)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    df.reset_index(drop=True, inplace=True)

    # Validate required fields
    required_fields = ["STORE NAME", "STORE NUMBER", "UPC"]
    missing_rows = []

    for idx, row in df.iterrows():
        for field in required_fields:
            if pd.isna(row.get(field)):
                missing_rows.append((idx + 2, field))  # Add 2 to account for header + 0-indexing

    if missing_rows:
        with st.expander("❗ Missing Values Detected", expanded=True):
            for row_idx, col in missing_rows:
                st.error(f"Row {row_idx}: Missing {col}")
            st.stop()

    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    return df


def format_pivot_table(workbook, selected_chain):
    sheet = workbook.active
    data = sheet.values
    columns = next(data)
    df = pd.DataFrame(data, columns=columns)

    id_vars = list(df.columns[:5])
    value_vars = list(df.columns[5:])

    df_melted = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                        var_name="STORE_NUMBER", value_name="YES_NO")
    df_melted["YES_NO"] = df_melted["YES_NO"].apply(lambda x: 1 if x == 1 else 0)

    df_melted.insert(0, "STORE_NAME", "")
    df_melted["CHAIN_NAME"] = selected_chain
    df_melted["SKU"] = 0
    df_melted["ACTIVATION_STATUS"] = ""
    df_melted["COUNTY"] = ""

    df_melted.rename(columns={
        "Name": "PRODUCT_NAME",
        "SKU #": "SKU",
        "UPC": "UPC",
        "Yes/No": "YES_NO"
    }, inplace=True)

    df_melted['UPC'] = pd.to_numeric(df_melted['UPC'].astype(str).str.replace('-', ''), errors='coerce')
    if df_melted['UPC'].isna().any():
        st.error("❌ Some UPCs could not be converted to numeric. Please fix and try again.")
        st.dataframe(df_melted[df_melted['UPC'].isna()])
        st.stop()

    return df_melted




# -----------------------------
# 🔧 Utility Functions
# -----------------------------

def get_local_ip():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception as e:
        print(f"Error getting IP: {e}")
        return None

def insert_log_entry(user_id, activity_type, description, success, ip_address, selected_option):
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        return
    try:
        conn = connect_to_tenant_snowflake(toml_info)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO LOG (TIMESTAMP, USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS, USERAGENT)
            VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s)
            """,
            (user_id, activity_type, description, success, ip_address, selected_option)
        )
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Log insert error: {e}")

def update_spinner(message):
    st.text(f"{message} ...")

# -----------------------------
# 📦 Archive + Cleanup
# -----------------------------

def archive_data(chain_name, rows):
    if not rows:
        return
    conn = connect_to_tenant_snowflake(st.session_state["toml_info"])
    cur = conn.cursor()
    today = date.today()
    placeholders = ', '.join(['%s'] * 13)
    query = f"""
        INSERT INTO DISTRO_GRID_ARCHIVE (
            STORE_NAME, STORE_NUMBER, UPC, SKU, PRODUCT_NAME,
            MANUFACTURER, SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY,
            CHAIN_NAME, ARCHIVE_DATE
        )
        VALUES ({placeholders})
    """
    archive_data = [row + (today,) for row in rows]
    cur.executemany(query, archive_data)
    conn.commit()
    cur.close()
    conn.close()

def remove_archived_records(chain_name):
    conn = connect_to_tenant_snowflake(st.session_state["toml_info"])
    cur = conn.cursor()
    cur.execute("DELETE FROM DISTRO_GRID WHERE CHAIN_NAME = %s", (chain_name,))
    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# 📤 Upload Logic
# -----------------------------


def call_procedure(selected_chain: str):
    try:
        conn = connect_to_tenant_snowflake(st.session_state["toml_info"])
        cur = conn.cursor()
        cur.execute("CALL UPDATE_DISTRO_GRID(%s)", (selected_chain,))
        result = cur.fetchone()
        st.write(f"Procedure result: {result[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"Procedure call failed: {e}")


# -----------------------------
# 🚀 Main Upload Orchestrator
# -----------------------------

def upload_distro_grid_to_snowflake(df: pd.DataFrame, selected_chain: str, update_spinner_callback):
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")
    if not toml_info or not tenant_id:
        st.error("❌ Tenant configuration missing.")
        return

    df.replace("NAN", np.nan, inplace=True)
    df.fillna("", inplace=True)
    df['UPC'] = df['UPC'].apply(lambda x: str(x)[:-1] if str(x).endswith('S') else x)
    df['UPC'] = df['UPC'].astype(np.int64)
    df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0).astype(np.int64)

    conn = connect_to_tenant_snowflake(toml_info)

    try:
        update_spinner_callback(f"📦 Archiving existing records for {selected_chain}")
        cur = conn.cursor()
        cur.execute("SELECT STORE_NAME, STORE_NUMBER, UPC, SKU, PRODUCT_NAME, MANUFACTURER, SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY, CHAIN_NAME FROM DISTRO_GRID WHERE CHAIN_NAME = %s", (selected_chain,))
        archive_data(selected_chain, cur.fetchall())
        cur.close()

        update_spinner_callback(f"🧹 Removing existing records for {selected_chain}")
        remove_archived_records(selected_chain)

        update_spinner_callback(f"📤 Uploading new data for {selected_chain}")
        load_data_into_distro_grid(conn, df, selected_chain)

        update_spinner_callback(f"⚙️ Running post-upload update procedure")
        call_procedure()

        update_spinner_callback(f"✅ Upload complete for {selected_chain}")

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Upload failed: {e}")
    finally:
        conn.close()





#====================================================================================================================

# Function to insert Activity to the log table

#====================================================================================================================


def insert_log_entry(user_id, activity_type, description, success, ip_address, selected_option):
    
        # Retrieve toml_info from session
    toml_info = st.session_state.get('toml_info')
    #st.write(toml_info)
    if not toml_info:
        st.error("TOML information is not available. Please check the tenant ID and try again.")
        return
    try:
    
    
        conn_toml = connect_to_tenant_snowflake(st.session_state["toml_info"])

        # Create a cursor object
        cursor = conn_toml.cursor()
        
        # Replace 'LOG' with the actual name of your log table
        insert_query = """
        INSERT INTO LOG (TIMESTAMP, USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS, USERAGENT)
        VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (user_id, "SQL Activity", description, True, ip_address, selected_option))

        cursor.close()
    except Exception as e:
        # Handle any exceptions that might occur while logging
        print(f"Error occurred while inserting log entry: {str(e)}")

#====================================================================================================================
# Function to insert Activity to the log table
#====================================================================================================================
