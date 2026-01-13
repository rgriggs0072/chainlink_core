# -------------- snowflake_utils.py ------------------------------------------------------------------------------------------------


import streamlit as st
import snowflake.connector
import logging
import pandas as pd
import os
import jwt
from datetime import datetime, timedelta
from sf_connector.service_connector import get_service_account_connection
from utils.dashboard_data.home_dashboard import fetch_chain_schematic_data

import numpy as np
import getpass
import socket

#--------------- Custom Import Modules ----------------------------------------------------------------------------------

#from db_utils.snowflake_utils import create_gap_report, get_snowflake_connection, execute_query_and_close_connection, get_snowflake_toml, validate_toml_info, fetch_and_store_toml_info, fetch_chain_schematic_data


def _q(*parts: str) -> str:
    return ".".join(f'"{p}"' for p in parts if p)


def current_timestamp():
    return datetime.now()



def get_tenant_sales_report(conn=None, tenant_config=None, days: int = 90) -> pd.DataFrame:
    """
    Fetch recent sales rows for the current tenant.

    Prefers the active Streamlit session connection/tenant_config.
    Falls back to the provided args if given explicitly.
    Does NOT close the connection if it didn't open it.
    """
    # --- normalize inputs ---
    conn = conn or st.session_state.get("conn")
    tenant_config = tenant_config or st.session_state.get("tenant_config")


    if not conn or not tenant_config:
        st.error("❌ Missing connection or tenant configuration.")
        return pd.DataFrame()

    db = tenant_config.get("database")
    sch = tenant_config.get("schema")
    if not db or not sch:
        st.error("❌ Tenant configuration missing database/schema.")
        return pd.DataFrame()

    # NOTE: qualify db + schema; keep PURCHASED_YES_NO quoted if it’s case-sensitive
    query = f"""
        SELECT
            STORE_NUMBER,
            STORE_NAME,
            PRODUCT_NAME,
            UPC,
            "PURCHASED_YES_NO",
            LAST_UPLOAD_DATE
        FROM {_q(db, sch, "SALES_REPORT")}
        WHERE LAST_UPLOAD_DATE >= DATEADD(day, -{int(days)}, CURRENT_DATE)
        ORDER BY LAST_UPLOAD_DATE DESC
    """

    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error("❌ Failed to fetch Sales Report data")
        st.exception(e)
        return pd.DataFrame()




def validate_toml_info(toml_info):
    required_keys = ["account", "snowflake_user", "password", "warehouse", "database", "schema"]
    missing_keys = [key for key in required_keys if key not in toml_info or not toml_info[key]]
    if missing_keys:
        logging.error(f"TOML configuration is incomplete or invalid. Missing: {missing_keys}")
        st.error(f"TOML configuration is incomplete or invalid. Check the configuration.")
        return False
    return True



# -------------------------------------------------------------------------------------------------------------------------------------------

# ===========================================================================================================================================
# Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in row 1 column 2
# ===========================================================================================================================================

def fetch_chain_schematic_data(toml_info):
    try:
        conn = get_service_account_connection(toml_info)
        if not conn:
            st.error("❌ Failed to connect using RSA authentication.")
            return pd.DataFrame()

        query = """
            SELECT 
                CHAIN_NAME, 
                SUM("In_Schematic") AS TOTAL_IN_SCHEMATIC, 
                SUM("PURCHASED_YES_NO") AS PURCHASED, 
                SUM("PURCHASED_YES_NO") / COUNT(*) AS PURCHASED_PERCENTAGE 
            FROM GAP_REPORT 
            GROUP BY CHAIN_NAME
        """

        df = pd.read_sql(query, conn)
        st.write("🔍 RAW RESULTS:", df)

        if df.empty:
            st.warning("⚠️ Query returned no data.")

        df['PURCHASED_PERCENTAGE'] = (df['PURCHASED_PERCENTAGE'].astype(float) * 100).round(2).astype(str) + '%'
        return df

    except Exception as e:
        st.error(f"⚠️ Query failed: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()


# ===========================================================================================================================================
# END Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in column 3




def fetch_supplier_names():
    
    
    
    # Retrieve toml_info from session state
    toml_info = st.session_state.get('toml_info')
    if not toml_info:
        st.error("TOML information is not available. Please check the tenant ID and try again.")
        return

    query = "SELECT DISTINCT supplier FROM supplier_county order by supplier"  # Adjust the query as needed
    # Create a connection to Snowflake
    conn_toml = get_snowflake_toml(toml_info)
    
    # Create a cursor object
    cursor = conn_toml.cursor()

    cursor.execute(query)
    result = cursor.fetchall()
    
    # Safely iterate over the result
    supplier_names = [row[0] for row in result]
    return supplier_names

#===================================================================================================
# Function to create the gap report from data pulled from snowflake and button to download gap report
#=====================================================================================================




def create_gap_report_LEGACY_DO_NOT_USE(conn, salesperson, store, supplier):
    raise RuntimeError("Legacy function. Use utils.gap_report_builder.create_gap_report instead.")
    """
    Retrieves data from a Snowflake view and creates a button to download the data as a CSV report.
    """
   
    # Retrieve toml_info from session state
    toml_info = st.session_state.get('toml_info')
    if not toml_info:
        st.error("TOML information is not available. Please check the tenant ID and try again.")
        return
 
        # Create a connection to Snowflake
        conn_toml = snowflake_connection.get_snowflake_toml(toml_info)

        # Create a cursor object
        cursor = conn_toml.cursor()
    
        # Execute the stored procedure without filters
        #cursor = conn.cursor()
        cursor.execute("CALL PROCESS_GAP_REPORT()")
        cursor.close()

    # Execute SQL query and retrieve data from the Gap_Report view with filters
    if salesperson != "All":
        query = f"SELECT * FROM Gap_Report WHERE SALESPERSON = '{salesperson}'"
        if store != "All":
            query += f" AND STORE_NAME = '{store}'"
            if supplier != "All":
                query += f" AND SUPPLIER = '{supplier}'"
    elif store != "All":
        query = f"SELECT * FROM Gap_Report WHERE STORE_NAME = '{store}'"
        if supplier != "All":
            query += f" AND SUPPLIER = '{supplier}'"
    else:
        if supplier != "All":
            query = f"SELECT * FROM Gap_Report WHERE SUPPLIER = '{supplier}'"
        else:
            query = "SELECT * FROM Gap_Report"
    df = pd.read_sql(query, conn)

    # Get the user's download folder
    download_folder = os.path.expanduser(r"~\Downloads")

    # Write the updated dataframe to a temporary file
    temp_file_name = 'temp.xlsx'

    # Create the full path to the temporary file
    #temp_file_path = os.path.join(download_folder, temp_file_name)
    temp_file_path = "temp.xlsx"
    #df.to_excel(temp_file_path, index=False)
    #st.write(df)

    df.to_excel(temp_file_path, index=False)  # Save the DataFrame to a temporary file


    # # Create the full path to the temporary file
    # temp_file_name = 'temp.xlsx'
    # temp_file_path = os.path.join(download_folder, temp_file_name)

    return temp_file_path  # Return the file path



def get_local_ip():
    try:
        host_name = socket.gethostname()
        ip_address = socket.gethostbyname(host_name)
        return ip_address
    except Exception as e:
        st.error(f"An error occurred while getting the IP address: {e}")
        return None


def get_local_ip():
    try:
        # Get the local host name
        host_name = socket.gethostname()
        
        # Get the IP address associated with the host name
        ip_address = socket.gethostbyname(host_name)
        
        return ip_address
    except Exception as e:
        print(f"An error occurred while getting the IP address: {e}")
        return None

 #====================================================================================================================
# End Function to get IP address of the user carring out the activity
#====================================================================================================================

#--------------------------------------------------------------------------------------------------------------------



def update_spinner(message):
    st.text(f"{message} ...")




# ============================================================================================================================================================
# Function to check if todays privot table data has processed.  If so will give user option to overwrite the data and if not the procedure BUILD_GAP_TRACKING()
# Procedure will update the table SALESPERSON_EXECUTION_SUMMARY_TBL with todays data
# ============================================================================================================================================================


def check_and_process_data(conn=None) -> None:
    """
    Check and (optionally) refresh the salesperson gap history snapshot.

    Behavior:
    - Uses SALESPERSON_EXECUTION_SUMMARY as the source.
    - Writes a daily snapshot into SALESPERSON_EXECUTION_SUMMARY_TBL via BUILD_GAP_TRACKING().
    - If today's LOG_DATE already exists in the table, prompts the user
      to overwrite or keep the existing snapshot.

    Args:
        conn: Optional Snowflake connection. If None, falls back to
              st.session_state["conn"] (the per-tenant connection used
              by the Chainlink Core app).
    """
    # ---------------------------
    # Resolve connection
    # ---------------------------
    if conn is None:
        conn = st.session_state.get("conn")

    if conn is None:
        st.error("No active Snowflake connection found. Please log in again.")
        return

    # ---------------------------
    # Check if today's snapshot exists
    # ---------------------------
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM SALESPERSON_EXECUTION_SUMMARY_TBL
                WHERE LOG_DATE = CURRENT_DATE()
                """
            )
            (count_today,) = cur.fetchone()
    except Exception as e:
        st.error("Failed to check existing gap history snapshot.")
        st.exception(e)
        return

    # ---------------------------
    # If data exists for today: prompt to overwrite
    # ---------------------------
    if count_today > 0:
        st.warning(
            "Gap history for today already exists in "
            "SALESPERSON_EXECUTION_SUMMARY_TBL.\n\n"
            "Do you want to overwrite today's snapshot?"
        )

        col_yes, col_no = st.columns(2)

        overwrite = col_yes.button(
            "Yes, overwrite today's snapshot",
            key="gap_overwrite_yes",
        )
        keep = col_no.button(
            "No, keep existing snapshot",
            key="gap_overwrite_no",
        )

        if overwrite:
            try:
                with conn.cursor() as cur:
                    # Remove today's rows
                    cur.execute(
                        """
                        DELETE FROM SALESPERSON_EXECUTION_SUMMARY_TBL
                        WHERE LOG_DATE = CURRENT_DATE()
                        """
                    )
                    # Rebuild snapshot from current SALESPERSON_EXECUTION_SUMMARY
                    cur.execute("CALL BUILD_GAP_TRACKING()")

                st.success(
                    "Today's gap history snapshot was overwritten via BUILD_GAP_TRACKING()."
                )
                st.rerun()

            except Exception as e:
                st.error("Failed to overwrite today's gap history snapshot.")
                st.exception(e)

        elif keep:
            st.info("Existing snapshot kept. No changes were made.")

    # ---------------------------
    # If no snapshot for today: create it
    # ---------------------------
    else:
        try:
            with conn.cursor() as cur:
                cur.execute("CALL BUILD_GAP_TRACKING()")

            st.success(
                "Gap history snapshot created for today via BUILD_GAP_TRACKING()."
            )
            st.rerun()

        except Exception as e:
            st.error("Failed to create today's gap history snapshot.")
            st.exception(e)







def fetch_distinct_values(conn, table_name: str, column_name: str) -> list:
    """
    Fetch distinct non-null values from a given table and column in Snowflake.

    Args:
        conn: Active Snowflake connection object.
        table_name (str): Name of the table.
        column_name (str): Name of the column.

    Returns:
        List of distinct non-null values from the column.
    """
    try:
        query = f'SELECT DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL'
        df = pd.read_sql(query, conn)
        return sorted(df[column_name].dropna().unique().tolist())
    except Exception as e:
        st.error(f"❌ Error fetching distinct values from {table_name}.{column_name}: {e}")
        return []
