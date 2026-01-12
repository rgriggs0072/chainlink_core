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

# 

# def fetch_and_store_toml_info(tenant_id):
   
#     try:
#         conn = get_snowflake_connection()  # Fetch Snowflake connection
#         cursor = conn.cursor()
       
#         # Fetch TOML information based on the tenant_id
#         query = """
#         SELECT snowflake_user, password, account, warehouse, database, schema, logo_path, tenant_name
#         FROM TOML
#         WHERE TENANT_ID =  %s
#         """


#         cursor.execute(query, (tenant_id,))
#         toml_info = cursor.fetchone()
        
#         cursor.close()
#         conn.close()

#         if toml_info:
#             keys = ["snowflake_user", "password", "account", "warehouse", "database", "schema", "logo_path", "tenant_name"]
#             toml_dict = dict(zip(keys, toml_info))
           
            
#             # Store the TOML info in session state
#             st.session_state['toml_info'] = toml_dict
#             st.session_state['tenant_name'] = toml_dict['tenant_name']
#             return True  # Indicate successful fetch and store
#         else:
#             logging.error(f"No TOML configuration found for tenant_id: {tenant_id}")
#             return False  # Indicate failure in fetching TOML info
#     except Exception as e:
#         logging.error(f"Failed to fetch TOML info due to: {str(e)}")
#         return False  # Handle the error appropriately






# def validate_user_email(email):
#     """
#     Validates if the email or username exists in the database.
#     """
#     conn = get_snowflake_connection()
#     cursor = conn.cursor()

#     try:
#         # Query to check if the email or username exists and get the associated tenant_id
#         cursor.execute("""
#             SELECT tenant_id
#             FROM userdata
#             WHERE email = %s 
#         """, (email))

#         result = cursor.fetchone()
#         if result:
#             st.session_state['tenant_id'] = result[0]  # Store the tenant_id in session_state
#             return True
#         else:
#             return False
#     except Exception as e:
#         print(f"Error validating user: {e}")
#         return False
#     finally:
#         cursor.close()
#         conn.close()




# def fetch_user_credentials():
#     """
#     Fetches all user credentials from the USERDATA table and returns them
#     in a format compatible with streamlit-authenticator.
#     """
#     try:
#         conn = get_snowflake_connection()
#         cursor = conn.cursor()

#         # Query to get all users, including their active status
#         query = """
#         SELECT u.USERNAME, u.HASHED_PASSWORD, u.TENANT_ID, u.EMAIL, r.ROLE_NAME, u.IS_ACTIVE
#         FROM USERDATA u
#         LEFT JOIN USER_ROLES ur ON u.USER_ID = ur.USER_ID
#         LEFT JOIN ROLES r ON ur.ROLE_ID = r.ROLE_ID
        
#         """
#         cursor.execute(query)
#         users = cursor.fetchall()

#        # print ("users")

#         # Initialize credentials dictionary for streamlit-authenticator
#         credentials = {
#             'usernames': {}
#         }

#         for user in users:
#             username = user[0]
#             password_hash = user[1]  # Already hashed
#             tenant_id = user[2]
#             email = user[3]
#             role_name = user[4]
#             is_active = user[5]  # Active status


#             # Include the user's active status in the credentials dictionary
#             credentials['usernames'][username] = {
#                 'name': username,
#                 'password': password_hash,  # Hashed password for streamlit-authenticator
#                 'tenant_id': tenant_id,
#                 'useremail': email,
#                 'roles': [role_name],  # Store roles in an array
#                 'is_active': is_active  # Include active status
#             }

#           #  print (f"user name and password are : ", user, password_hash)
#         cursor.close()
#         conn.close()

#         #st.write("DEBUG: Fetched Users →", users)


#         return credentials

#     except Exception as e:
#         print(f"Error fetching user credentials: {e}")
#         return None








# ============================================================================================================================================================
# 11/28/2023 Randy Griggs - Function will be called to handle the DB query and closing the the connection and return the results to the calling function
# ============================================================================================================================================================

# def execute_query_and_close_connection(query, conn_toml):
#     """
#     Executes the given SQL query and closes the connection after completion.
#     If the connection is closed prematurely, it tries to re-establish the connection.
#     """
#     cursor = None  # Initialize cursor to None to avoid unbound variable error
#     try:
#         # Ensure the connection is open
#         if conn_toml.is_closed():
#             # Try to re-establish the connection if it's closed
#             conn_toml = get_snowflake_toml(st.session_state['toml_info'])
        
#         # If the connection is still closed, raise an error
#         if conn_toml is None or conn_toml.is_closed():
#             st.error("Unable to establish a connection to Snowflake.")
#             return None

#         cursor = conn_toml.cursor()
#         cursor.execute(query)
#         result = cursor.fetchall()

#         return result

#     except Exception as e:
#         st.error(f"Error executing query: {str(e)}")
#         return None

#     finally:
#         # Close the cursor and connection after execution
#         if cursor:
#             cursor.close()
#         if conn_toml and not conn_toml.is_closed():
#             conn_toml.close()



# ============================================================================================================================================================
# END 11/28/2023 Randy Griggs - Function will be called to handle the DB query and closing the the connection
# ============================================================================================================================================================
    

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



# def fetch_supplier_schematic_summary_data(selected_suppliers):
#     toml_info = st.session_state.get('toml_info')
#     supplier_conditions = ", ".join([f"'{supplier}'" for supplier in selected_suppliers])

#     query = f"""
#     SELECT 
#     PRODUCT_NAME,
#     "dg_upc" AS UPC,
#     SUM("In_Schematic") AS Total_In_Schematic,
#     SUM(PURCHASED_YES_NO) AS Total_Purchased,
#     (SUM(PURCHASED_YES_NO) / SUM("In_Schematic")) * 100 AS Purchased_Percentage
#     FROM
#         GAP_REPORT_TMP2
#     WHERE
#         "sc_STATUS" = 'Yes' AND SUPPLIER IN ({supplier_conditions})
#     GROUP BY
#         SUPPLIER, PRODUCT_NAME, "dg_upc"
#     ORDER BY Purchased_Percentage ASC;
#     """

#     # Create a connection using get_snowflake_toml which should return a connection object
#     conn_toml = get_snowflake_toml(toml_info)

#     if conn_toml:
#         # Execute the query and get the result using the independent function
#         result = execute_query_and_close_connection(query, conn_toml)

#         if result:
#             df = pd.DataFrame(result, columns=["PRODUCT_NAME", "UPC", "Total_In_Schematic", "Total_Purchased", "Purchased_Percentage"])
#             return df
#         else:
#             st.error("No data was returned from the query.")
#             return pd.DataFrame()
#     else:
#         st.error("Failed to establish a connection.")
#         return pd.DataFrame()







def current_timestamp():
    return datetime.now()



def get_local_ip():
    try:
        host_name = socket.gethostname()
        ip_address = socket.gethostbyname(host_name)
        return ip_address
    except Exception as e:
        st.error(f"An error occurred while getting the IP address: {e}")
        return None

#=================================================================================================================================================================================
# The following procedure will upload the reset schedule worksheet into snowflake.  Te data frame is passed from "reset_data_update.py" with a couple of checks for valid entries
# it will delete the entire table for the selected chain and then reload the reset_schedule table with the data for that chain
#=================================================================================================================================================================================

# def upload_reset_data(df, selected_chain):
#     if df['CHAIN_NAME'].isnull().any():
#         st.warning("CHAIN_NAME field cannot be empty. Please provide a value for CHAIN_NAME empty cell and try again.")
#         return
#     if df['STORE_NAME'].isnull().any():
#         st.warning("STORE_NAME field cannot be empty. Please provide a value for the STORE_NAME empty cell and try again.")
#         return

#     selected_chain = selected_chain.upper()
#     chain_name_matches = df['CHAIN_NAME'].str.upper().eq(selected_chain)
#     num_mismatches = len(chain_name_matches) - chain_name_matches.sum()

#     if num_mismatches != 0:
#         st.warning(f"The selected chain ({selected_chain}) does not match {num_mismatches} name(s) in the CHAIN_NAME column. Please select the correct chain and try again.")
#         return

#     try:
#         toml_info = st.session_state.get('toml_info')
#         if not toml_info:
#             st.error("TOML information is not available. Please check the tenant ID and try again.")
#             return

#         conn_toml = get_snowflake_toml(toml_info)
#         cursor = conn_toml.cursor()

#         user_id = getpass.getuser()
#         local_ip = get_local_ip()
#         selected_option = st.session_state.selected_option

#         description = f"Started {selected_option} delete from reset table"
#         # create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

#         remove_query = f"DELETE FROM RESET_SCHEDULE WHERE CHAIN_NAME = '{selected_chain}'"
#         cursor.execute(remove_query)

#         description = f"Completed {selected_option} delete from reset table"
#         # create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
#         cursor.close()

#         cursor = conn_toml.cursor()
#         df = df.replace('NAN', np.nan).fillna(value='', method=None)
#         df = df.astype({'RESET_DATE': str, 'TIME': str})

#         # Add the current timestamp to each row in a new column for LAST_UPLOAD_DATE
#         df['LAST_UPLOAD_DATE'] = datetime.now()

#         description = f"Started {selected_option} insert into reset table"
#         # create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

#         placeholders = ', '.join(['%s'] * len(df.columns))
#         insert_query = f"INSERT INTO RESET_SCHEDULE VALUES ({placeholders})"
#         cursor.executemany(insert_query, df.values.tolist())

#         description = f"Completed {selected_option} insert into reset table"
#         # create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

#         conn_toml.commit()
#         # create_log_entry(user_id, "SQL Activity", "Transaction committed", True, local_ip, selected_option)
#         st.success("Data has been successfully written to Snowflake.")
#     except snowflake.connector.errors.ProgrammingError as pe:
#         st.error(f"An error occurred while writing to Snowflake: {str(pe)}")
#         if 'Date' in str(pe) and 'is not recognized' in str(pe):
#             st.warning("Invalid date format in the data. Please ensure all date values are formatted correctly.")
#         elif 'Time' in str(pe) and 'is not recognized' in str(pe):
#             st.warning("Invalid time format in the data. Please ensure all time values are formatted correctly.")
#         else:
#             st.exception(pe)
#     finally:
#         if 'conn_toml' in locals():
#             conn_toml.close()

    
       

#===============================================================================================================================================================
# END of function to import reset data for the selected chain.
#======================================================================================================================================================================





#=====================================================================================================================
# Function to get current date and time for log entry
#=====================================================================================================================
def current_timestamp():
    return datetime.now()

#=====================================================================================================================
# End Function to get current date and time for log entry
#=====================================================================================================================

#----------------------------------------------------------------------------------------------------------------------

#====================================================================================================================

# Function to insert Activity to the log table

#====================================================================================================================


# def insert_log_entry(user_id, activity_type, description, success, ip_address, selected_option):
    
#         # Retrieve toml_info from session
#     toml_info = st.session_state.get('toml_info')
#     #st.write(toml_info)
#     if not toml_info:
#         st.error("TOML information is not available. Please check the tenant ID and try again.")
#         return
#     try:
    
    
#         conn_toml = get_snowflake_toml(toml_info)

#         # Create a cursor object
#         cursor = conn_toml.cursor()
        
#         # Replace 'LOG' with the actual name of your log table
#         insert_query = """
#         INSERT INTO LOG (TIMESTAMP, USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS, USERAGENT)
#         VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s)
#         """
#         cursor.execute(insert_query, (user_id, "SQL Activity", description, True, ip_address, selected_option))

#         cursor.close()
#     except Exception as e:
#         # Handle any exceptions that might occur while logging
#         print(f"Error occurred while inserting log entry: {str(e)}")

#====================================================================================================================
# Function to insert Activity to the log table
#====================================================================================================================

#--------------------------------------------------------------------------------------------------------------------

#====================================================================================================================
# Function to get IP address of the user carring out the activity
#====================================================================================================================

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


# def archive_data(selected_option, data_to_archive):
    
#     # Retrieve toml_info from session
#     toml_info = st.session_state.get('toml_info')
#     st.write(toml_info)
#     if not toml_info:
#         st.error("TOML information is not available. Please check the tenant ID and try again.")
#         return

#     # Create a connection to Snowflake
#     conn_toml = get_snowflake_connection(toml_info)

#     # Create a cursor object
#     cursor = conn_toml.cursor()

#     if data_to_archive:
#         current_date = date.today().isoformat()
#         placeholders = ', '.join(['%s'] * (len(data_to_archive[0]) + 1))
#         insert_query = f"""
#             INSERT INTO DISTRO_GRID_ARCHIVE (
#                 STORE_NAME, STORE_NUMBER, UPC, SKU, PRODUCT_NAME, 
#                 MANUFACTURER, SEGMENT, YES_NO, ACTIVATION_STATUS, 
#                 COUNTY, CHAIN_NAME, ARCHIVE_DATE
#             )
#             VALUES ({placeholders})
#         """
        
#         # Add current_date to each row in data_to_archive
#         data_to_archive_with_date = [row + (current_date,) for row in data_to_archive]
        
#         # Chunk the data into smaller batches
#         chunk_size = 5000
#         chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]
        
#         # Execute the query with parameterized values for each chunk
#         cursor_archive = conn.cursor()
#         for chunk in chunks:
#             cursor_archive.executemany(insert_query, chunk)
#         cursor_archive.close()


# def remove_archived_records(selected_option):
    
#     # Retrieve toml_info from session
#     toml_info = st.session_state.get('toml_info')
#     st.write(toml_info)
#     if not toml_info:
#         st.error("TOML information is not available. Please check the tenant ID and try again.")
#         return

#     # Create a connection to Snowflake
#     conn_toml = get_snowflake_connection(toml_info)
#     #cursor_to_remove = conn_toml_.cursor()
#     # Create a cursor object
#     cursor_to_remove = conn_toml.cursor()

    
#     delete_query = "DELETE FROM DISTRO_GRID WHERE CHAIN_NAME = %s"
    
#     # Execute the delete query with the selected option (store_name)
#     cursor_to_remove.execute(delete_query, (selected_option,))
    
#     # Commit the delete operation
#     conn_toml.commit()
#     cursor_to_remove.close()


# def load_data_into_distro_grid(conn, df, selected_option):
#     user_id = getpass.getuser()
#     local_ip = get_local_ip()
    
#     # Retrieve toml_info from session
#     toml_info = st.session_state.get('toml_info')
#     st.write(toml_info)
#     if not toml_info:
#         st.error("TOML information is not available. Please check the tenant ID and try again.")
#         return

#     # Create a connection to Snowflake
#     conn_toml = get_snowflake_connection(toml_info)
    
#     # Log the start of the SQL activity
#     description = f"Started {selected_option} Loading data into the Distro_Grid Table"
#     insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    
#     # Generate the SQL query for loading data into the Distribution Grid table
#     placeholders = ', '.join(['%s'] * len(df.columns))
#     insert_query = f"""
#         INSERT INTO Distro_Grid (
#             {', '.join(df.columns)}
#         )
#         VALUES ({placeholders})
#     """
    
#     # Create a cursor object
#     cursor = conn_toml.cursor()
    
#     # Chunk the DataFrame into smaller batches
#     chunk_size = 5000  # Adjust the chunk size as per your needs
#     chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
#     # Execute the query with parameterized values for each chunk
#     for chunk in chunks:
#         cursor.executemany(insert_query, chunk.values.tolist())
    
#     # Log the start of the SQL activity
#     description = f"Completed {selected_option} Loading data into the Distro_Grid Table"
#     insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    


# def call_procedure():
#     try:
        
#         # Retrieve toml_info from session
#         toml_info = st.session_state.get('toml_info')
#         #st.write(toml_info)
#         if not toml_info:
#             st.error("TOML information is not available. Please check the tenant ID and try again.")
#             return

#         # Create a connection to Snowflake
#         conn_toml = get_snowflake_connection(toml_info)
#         # Call the procedure
#         cursor = conn_toml.cursor()
#         cursor.execute("CALL UPDATE_DISTRO_GRID()")
        
#         # Fetch and print the result
#         result = cursor.fetchone()
#         print(result[0])  # Output: Update completed successfully.
#     except snowflake.connector.errors.ProgrammingError as e:
#         print(f"Error: {e}")
#     finally:
#         # Close the cursor and the connection to Snowflake
#         cursor.close()
#         conn_toml.close()


# def upload_distro_grid_to_snowflake(df, selected_option, update_spinner_callback):
#     #conn = create_snowflake_connection()[0]  # Get connection object
    

#     # Retrieve toml_info from session
#     toml_info = st.session_state.get('toml_info')
#     #st.write(toml_info)
#     if not toml_info:
#         st.error("TOML information is not available. Please check the tenant ID and try again.")
#         return

#     # Create a connection to Snowflake
#     conn_toml = get_snowflake_connection(toml_info)
#     # Call the procedure
#     cursor = conn_toml.cursor()
    
#     # Replace 'NAN' values with NULL
#     df = df.replace('NAN', np.nan).fillna(value='', method=None)
       
    
#     # Remove 'S' from the end of UPC if it exists
#     df['UPC'] = df['UPC'].apply(lambda x: str(x)[:-1] if str(x).endswith('S') else x)


  

#     # Convert 'UPC' column to np.int64
#     df['UPC'] = df['UPC'].astype(np.int64)
    
#     # Fill missing and non-numeric values in the "SKU" column with zeros
#     df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)
    
#     # Convert the "SKU" column to np.int64 data type, which supports larger integers
#     df['SKU'] = df['SKU'].astype(np.int64)
    
    

#     # Log the start of the SQL activity
#     user_id = getpass.getuser()
#     local_ip = get_local_ip()
#     description = f"Started {selected_option} Start Archive Process for distro_grid table"
#     insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    

#     # Update spinner message for archive completion
#     update_spinner_callback(f"Starting {selected_option} Archive Process")
    
#     # Step 1: Fetch data for archiving
#     cursor_archive = conn_toml.cursor()
#     cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE CHAIN_NAME = %s", (selected_option,))
#     data_to_archive = cursor_archive.fetchall()
    
#     # Step 2: Archive data
#     archive_data(conn_toml, selected_option, data_to_archive)
    
#     # Update spinner message for archive completion
#     update_spinner_callback(f"Completed {selected_option} Archive Process")
    
#     # Step 3: Remove archived records from distro_grid table
#     remove_archived_records(selected_option)
    
#     # Update spinner message for removal completion
#     update_spinner_callback(f"Completed {selected_option} Removal of Archived Records")
    
#    # Update spinner message for data loading completion
#     update_spinner_callback(f"Started Loading New Data into Distro_Grid Table for {selected_option}")
    
#     # Load new data into distro_grid table
#     load_data_into_distro_grid(df, selected_option)
    
#     # Update spinner message for data loading completion
#     update_spinner_callback(f"Completed {selected_option} Loading Data into Distro_Grid Table")
    
#     update_spinner_callback(f"Starting Final Update to the Distro Grid for {selected_option}")
    
#     # Call procedure to update the distro Grid table with county and update the manufacturer and the product name
#     call_procedure()
    
#     # Update spinner message for procedure completion
#     update_spinner_callback(f"Completed Final {selected_option} Update Procedure")
#     st.write("Data has been imported into Snowflake table: Distro_Grid")





    # ============================================================================================================================================================
# Function to check if todays privot table data has processed.  If so will give user option to overwrite the data and if not the procedure BUILD_GAP_TRACKING()
# Procedure will update the table SALESPERSON_EXECUTION_SUMMARY_TBL with todays data
# ============================================================================================================================================================

import streamlit as st

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
