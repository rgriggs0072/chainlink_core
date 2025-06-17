
# ----------- utils/distro_grid_helpers.py -----------

from datetime import datetime, date
import getpass
import socket
import traceback
import numpy as np
import pandas as pd
import streamlit as st
from utils.snowflake_utils import fetch_distinct_values
from sf_connector.service_connector import connect_to_tenant_snowflake



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


def get_season_options():
    current_year = datetime.now().year
    return [f"Spring {current_year}", f"Fall {current_year}", f"Spring {current_year + 1}", f"Fall {current_year + 1}"]


def format_non_pivot_table(workbook, stream=None, selected_option=None):
    """
    Formats a standard column-format Distribution Grid Excel workbook and returns a cleaned DataFrame.
    Cleans apostrophes in STORE_NAME and hyphens in UPC, and validates required fields and chain name.
    """
  

    # Load and parse the Excel sheet
    df = pd.DataFrame(workbook.active.values)
    header = df.iloc[0]
    df = df[1:]
    df.columns = header
    df.reset_index(drop=True, inplace=True)

    # Standardize column names
    df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]

    # Tracking for reporting
    rows_with_missing_values = []
    rows_with_apostrophe_issues = []
    rows_with_upc_hyphens = []

    smart_quote = "\u2019"

    for idx, row in df.iterrows():
        missing_fields = []

        # Extract fields safely
        store_name = str(row.get("STORE_NAME", "")).strip()
        store_number = row.get("STORE_NUMBER")
        upc = str(row.get("UPC", "")).strip()
        chain_name = str(row.get("CHAIN_NAME", "")).strip()

        # Clean STORE_NAME apostrophes and smart quotes
        normalized_store_name = store_name.replace("'", "").replace(smart_quote, "")
        if normalized_store_name != store_name:
            rows_with_apostrophe_issues.append(idx)
            df.at[idx, "STORE_NAME"] = normalized_store_name

        # Clean UPC hyphens
        if "-" in upc:
            cleaned_upc = upc.replace("-", "")
            df.at[idx, "UPC"] = cleaned_upc
            rows_with_upc_hyphens.append(idx)

        # Required field validation
        if not store_name:
            missing_fields.append("STORE_NAME")
        if pd.isna(store_number):
            missing_fields.append("STORE_NUMBER")
        if not upc:
            missing_fields.append("UPC")
        if not chain_name:
            missing_fields.append("CHAIN_NAME")

        if missing_fields:
            rows_with_missing_values.append(f"Row {idx + 2}: Missing {', '.join(missing_fields)}")

    # Block on missing required fields
    if rows_with_missing_values:
        st.session_state['warnings_present'] = True
        with st.expander("❌ Missing Required Values", expanded=True):
            for msg in rows_with_missing_values:
                st.error(msg)
        st.error("Please fix these errors and re-upload the file.")
        st.stop()

    # Chain name validation (if a selection was made)
    if selected_option:
        df["CHAIN_NAME"] = df["CHAIN_NAME"].astype(str).str.strip()
        chain_mismatch_rows = df[df["CHAIN_NAME"] != selected_option]
        if not chain_mismatch_rows.empty:
            st.error(f"❌ {len(chain_mismatch_rows)} row(s) have CHAIN_NAME values that do not match your selection: '{selected_option}'")
            st.dataframe(chain_mismatch_rows)
            st.warning("Please correct the chain name in the file and try again.")
            if st.button("🔁 Clear Upload and Try Again"):
                st.session_state.pop("distro_grid_final_upload", None)
                st.rerun()
            st.stop()

    # Informational cleanup feedback
    if rows_with_apostrophe_issues:
        st.info(f"Cleaned apostrophes or smart quotes from {len(rows_with_apostrophe_issues)} store name(s).")

    if rows_with_upc_hyphens:
        st.info(f"Removed hyphens from {len(rows_with_upc_hyphens)} UPC(s) in the sheet.")

    st.success("✅ Formatting complete. File cleaned and ready for upload.")
    return df




def format_pivot_table(workbook, selected_option):
    import pandas as pd
    import streamlit as st
    import openpyxl
    from openpyxl.utils.dataframe import dataframe_to_rows

    sheet = workbook.active
    data = sheet.values
    columns = next(data)
    df = pd.DataFrame(data, columns=columns)

    # Melt the store columns (everything after first 5 columns)
    store_ids = df.columns[5:]
    df_melted = pd.melt(
        df,
        id_vars=df.columns[:5],
        value_vars=store_ids,
        var_name="STORE_NUMBER",
        value_name="Yes/No"
    )

    # Replace Yes/No/checkmarks with binary values
    df_melted["Yes/No"] = df_melted["Yes/No"].apply(lambda x: 'Yes' if x == 1 else ('No' if pd.isna(x) else '*'))

    # Reorder & rename
    df_melted.insert(0, "STORE_NAME", "")
    df_melted.rename(columns={
        "Name": "PRODUCT_NAME",
        "Yes/No": "YES_NO",
        "SKU #": "SKU"
    }, inplace=True)

    # Reorder for import structure
    df_melted = df_melted[["STORE_NAME", "STORE_NUMBER", "UPC"] + [col for col in df_melted.columns if col not in ["STORE_NAME", "STORE_NUMBER", "UPC"]]]

    # Clean characters and normalize
    df_melted = df_melted.replace({"'": "", ",": "", r"\*": "", "Yes": "1", "No": "0"}, regex=True)

    # UPC cleanup and validation
    df_melted["UPC"] = df_melted["UPC"].astype(str).str.replace("-", "", regex=True)
    temp_upc_numeric = pd.to_numeric(df_melted["UPC"], errors="coerce")

    invalid_upcs = df_melted[temp_upc_numeric.isna()]
    if not invalid_upcs.empty:
        st.error("❌ Some UPC values could not be converted to numeric.")
        st.dataframe(invalid_upcs[["UPC"]])
        st.stop()

    df_melted["UPC"] = temp_upc_numeric

    # Add required empty columns
    df_melted["SKU"] = 0
    df_melted["ACTIVATION_STATUS"] = ""
    df_melted["COUNTY"] = ""
    df_melted["CHAIN_NAME"] = selected_option
    df_melted["STORE_NAME"] = selected_option

    # Convert to Excel workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    for r in dataframe_to_rows(df_melted, index=False, header=True):
        ws.append(r)

    return wb





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
            INSERT INTO LOG (TIMESTAMP, USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS)
            VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s)
            """,
            (user_id, activity_type, description, success, ip_address)
        )
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Log insert error: {e}")


def update_spinner(message):
    st.text(f"{message} ...")

def call_procedure_update_DG(selected_chain: str):

    """
    Sets the selected_chain session variable and calls the UPDATE_DISTRO_GRID procedure
    for the current tenant's database and schema.
    """
    try:
        conn = connect_to_tenant_snowflake(st.session_state["toml_info"])
        cur = conn.cursor()

        # Escape single quotes in chain name
        safe_chain = selected_chain.replace("'", "''")
        set_cmd = f"SET selected_chain = '{safe_chain}'"
        cur.execute(set_cmd)

        # Build fully qualified procedure call
        db = st.session_state["toml_info"]["database"]
        schema = st.session_state["toml_info"]["schema"]
        proc_call = f'CALL "{db}"."{schema}".UPDATE_DISTRO_GRID()'

        # Log full execution call to UI
       # st.code(f"{set_cmd};\n{proc_call}", language="sql")

        # Execute the procedure
        cur.execute(proc_call)
        result = cur.fetchone()

        # Show result or fallback message
        if result:
            st.success(f"✅ Procedure result: {result[0]}")
        else:
            st.warning("⚠️ Procedure completed but returned no result.")

        cur.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Procedure call failed: {e}")



def log_update_result(conn, user_id, success, message, ip_address=""):
    """
    Logs the result of the update_distro_grid procedure into the LOG table.
    """
    activity = 'UPDATE_DISTRO_GRID'
    query = f"""
        INSERT INTO DELTAPACIFIC_DB.DELTAPACIFIC_SCH.LOG 
        (USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS)
        VALUES (%s, %s, %s, %s, %s)
    """
    conn.cursor().execute(query, (user_id, activity, message, success, ip_address))




def load_data_into_distro_grid(conn, df, selected_chain, season):
    """
    Inserts cleaned distro grid DataFrame into the tenant-specific DISTRO_GRID table in Snowflake,
    with archive protection to ensure a chain is archived only once per season.

    Parameters:
        conn (Snowflake Connection): Active Snowflake connection.
        df (DataFrame): Cleaned and enriched distro grid data.
        selected_chain (str): The chain being uploaded (used for filtering and logging).
        season (str): The user-selected season (e.g., "Spring 2025") used for archive validation.
    """
    cur = conn.cursor()
    toml_info = st.session_state.get("toml_info", {})
    db, schema = toml_info.get("database"), toml_info.get("schema")

    if not db or not schema:
        raise ValueError("Missing database or schema in session state (toml_info).")

    dg_table = f'"{db}"."{schema}".DISTRO_GRID'
    dg_archive_table = f'"{db}"."{schema}".DISTRO_GRID_ARCHIVE'
    archive_tracking_table = f'"{db}"."{schema}".DG_ARCHIVE_TRACKING'

    chain_upper = selected_chain.strip().upper()

    if "TENANT_ID" not in df.columns:
        tenant_id = st.session_state.get("tenant_id")
        if not tenant_id:
            raise ValueError("Missing tenant_id in session. Cannot upload.")
        df["TENANT_ID"] = tenant_id
        df["PRODUCT_ID"] = None

    # 🔍 Step 1: Check archive tracking
    try:
        cur.execute(
            f"SELECT 1 FROM {archive_tracking_table} WHERE CHAIN_NAME = %s AND SEASON = %s",
            (chain_upper, season)
        )
        already_archived = cur.fetchone() is not None
        #st.info(f"🔍 Archive check for {chain_upper} - {season}: {'Already archived ✅' if already_archived else 'Not archived yet 🚫'}")
    except Exception as e:
        st.error(f"❌ Failed archive check: {e}")
        raise

    # 📦 Step 2: Archive current DG records
    if not already_archived:
        try:
            #st.info(f"📦 Archiving distribution grid records for {chain_upper} ...")

            archive_insert_query = f"""
                INSERT INTO {dg_archive_table} (
                    TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME, STORE_NUMBER,
                    PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
                    SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY,
                    ARCHIVE_DATE, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
                )
                SELECT
                    TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME, STORE_NUMBER,
                    PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
                    SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY,
                    CURRENT_DATE(), CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
                FROM {dg_table}
                WHERE TRIM(UPPER(CHAIN_NAME)) = %s
            """
            cur.execute(archive_insert_query, (chain_upper,))
            cur.execute(
                f"SELECT COUNT(*) FROM {dg_archive_table} WHERE TRIM(UPPER(CHAIN_NAME)) = %s AND ARCHIVE_DATE = CURRENT_DATE()",
                (chain_upper,)
            )
            archived_count = cur.fetchone()[0]
            #st.success(f"✅ Archived {archived_count} records for {chain_upper} - {season}")

            # 🗂️ Log tracking
            cur.execute(
                f"INSERT INTO {archive_tracking_table} (CHAIN_NAME, SEASON) VALUES (%s, %s)",
                (chain_upper, season)
            )
            #st.info(f"🗃️ Tracking entry added for archive: {chain_upper} - {season}")

        except Exception as e:
            st.error(f"❌ Archive step failed for {chain_upper}: {e}")
            raise
    else:
        st.warning(f"⚠️ Archive already exists for {chain_upper} - {season}. Skipping archive step.")

    # 🧹 Step 3: Delete old DISTRO_GRID rows
    try:
       # st.info(f"🧹 Removing existing records for {chain_upper} from distribution grid table ...")
        cur.execute(f"DELETE FROM {dg_table} WHERE CHAIN_NAME = %s", (selected_chain,))
        #st.success(f"✅ Deleted existing records for {chain_upper}")
    except Exception as e:
        st.error(f"❌ Delete step failed: {e}")
        raise

    # 📥 Step 4: Insert new data
    insert_columns = [
        "CUSTOMER_ID", "CHAIN_NAME", "STORE_NAME", "STORE_NUMBER",
        "UPC", "SKU", "PRODUCT_ID", "PRODUCT_NAME", "MANUFACTURER", "SEGMENT",
        "YES_NO", "ACTIVATION_STATUS", "COUNTY", "TENANT_ID"
    ]
    insert_query = f"""
        INSERT INTO {dg_table} (
            {", ".join(insert_columns)},
            CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
        )
        VALUES ({", ".join(["%s"] * len(insert_columns))},
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_DATE())
    """
    records = df[insert_columns].values.tolist()

    # 🚦 Validate rows
    nullable = {"CUSTOMER_ID", "PRODUCT_ID", "MANUFACTURER", "COUNTY", "SEGMENT", "ACTIVATION_STATUS"}
    for i, row in enumerate(records):
        for j, val in enumerate(row):
            if insert_columns[j] not in nullable and (pd.isna(val) or str(val).strip().upper() == "NAN"):
                raise ValueError(f"❌ Invalid null in row {i} column '{insert_columns[j]}': {row}")

    # 🚀 Insert data
    try:
       # st.info(f"📤 Inserting {len(records)} new records into DISTRO_GRID ...")
        cur.executemany(insert_query, records)
       # st.success(f"✅ Inserted {len(records)} records into DISTRO_GRID")
    except Exception as e:
        st.error(f"❌ Insert into DISTRO_GRID failed: {e}")
        raise
    finally:
        cur.close()
        conn.commit()





def sanitize_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures DataFrame is safe for Snowflake insert:
    - Replaces NaN/None with safe defaults
    - Ensures no unquoted identifiers sneak in
    - Casts all numerics properly
    """
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str).replace(r'(?i)^nan$', '', regex=True).fillna('')
        else:
            df[col] = df[col].fillna('')
    return df

def upload_distro_grid_to_snowflake(df: pd.DataFrame, selected_chain: str, selected_season: str, update_spinner_callback):
    """
    Uploads cleaned distro grid data to Snowflake for the selected chain and season.

    Parameters:
        df (DataFrame): Cleaned distro grid data.
        selected_chain (str): Chain name being uploaded.
        selected_season (str): Season string (e.g., "Spring 2025") for archive tracking.
        update_spinner_callback (function): Function to update progress spinner text.
    """
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")
    user_id = st.session_state.get("user_id", "unknown")
    ip_address = st.session_state.get("ip_address", "unknown")

    if not toml_info or not tenant_id:
        st.error("❌ Tenant configuration missing.")
        return

    # ✅ Add missing columns before enrichment
    if "TENANT_ID" not in df.columns:
        df["TENANT_ID"] = tenant_id
    if "PRODUCT_ID" not in df.columns:
        df["PRODUCT_ID"] = 0

    # 🔄 Enrich with CUSTOMER_ID and correct STORE_NAME
    conn = connect_to_tenant_snowflake(toml_info)
    df = enrich_with_customer_data(df, conn)

    # 🧼 Sanitize after enrichment
    df = sanitize_dataframe_for_snowflake(df)

    # ⚠️ Warn about unmatched CUSTOMER_IDs
    if "CUSTOMER_ID" in df.columns:
        df["CUSTOMER_ID"] = df["CUSTOMER_ID"].replace({0: None})
        unmatched = df[df["CUSTOMER_ID"].isnull()]
        # if not unmatched.empty:
        #     st.warning(f"⚠️ {len(unmatched)} rows had no CUSTOMER_ID match and were set to NULL.")

    try:
        st.markdown("### 🚚 Upload Progress")

        # 1️⃣ Archive Step
        update_spinner_callback(f"1️⃣ Archiving existing records for {selected_chain} ...")
        # Archive logic is inside load_data_into_distro_grid

        # 2️⃣ Remove Step
        update_spinner_callback(f"2️⃣ Removing archived records from distribution grid table for {selected_chain} ...")
        # Delete logic is inside load_data_into_distro_grid

        # 3️⃣ Insert Step
        update_spinner_callback(f"3️⃣ Uploading new data for {selected_chain} ...")
        load_data_into_distro_grid(conn, df, selected_chain, selected_season)
        st.success(f"✅ Uploaded {len(df)} records for '{selected_chain}' into DISTRIBUTION GRID Table.")

        # 4️⃣ Post-procedure
        update_spinner_callback(f"4️⃣ Running post-upload update procedure ...")
        result = call_procedure_update_DG(selected_chain)
        #st.success(f"✅ Procedure result: {result if result else 'None'}")

        # 🧾 Log success
        insert_log_entry(
            user_id, "UPDATE_DISTRO_GRID",
            f"Upload complete for chain: {selected_chain}",
            True, ip_address, selected_chain
        )
        update_spinner_callback(f"✅ Upload complete for {selected_chain}")

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Upload failed: {e}")
    finally:
        conn.close()
       # update_spinner_callback("🔚 Upload finished.")



   


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



def enrich_with_customer_data(distro_df, conn):
    """
    Enriches distro_df with CUSTOMER_ID, COUNTY, and optionally corrects STORE_NAME
    using chain + store_number matches from the customers table.
    """
    query = """
        SELECT CUSTOMER_ID, CHAIN_NAME, STORE_NUMBER, STORE_NAME AS CORRECT_STORE_NAME, COUNTY
        FROM CUSTOMERS
    """
    customer_df = pd.read_sql(query, conn)

    # Normalize casing and whitespace
    for col in ["CHAIN_NAME", "CORRECT_STORE_NAME"]:
        customer_df[col] = customer_df[col].str.strip().str.upper()
    distro_df["CHAIN_NAME"] = distro_df["CHAIN_NAME"].str.strip().str.upper()

    # Merge on CHAIN_NAME + STORE_NUMBER
    merged = pd.merge(
        distro_df,
        customer_df,
        on=["CHAIN_NAME", "STORE_NUMBER"],
        how="left"
    )

    # Assign CUSTOMER_ID from merged columns
    if "CUSTOMER_ID_y" in merged.columns:
        merged["CUSTOMER_ID"] = merged["CUSTOMER_ID_y"].fillna(merged.get("CUSTOMER_ID_x"))
        merged.drop(columns=["CUSTOMER_ID_x", "CUSTOMER_ID_y"], inplace=True)

    # ✅ Only overwrite STORE_NAME if CORRECT_STORE_NAME is non-null
    has_store_name_corrections = merged["CORRECT_STORE_NAME"].notnull()
    merged.loc[has_store_name_corrections, "STORE_NAME"] = merged.loc[has_store_name_corrections, "CORRECT_STORE_NAME"]

    # Clean up
    merged.drop(columns=["CORRECT_STORE_NAME"], inplace=True)

    # ✅ Ensure COUNTY column exists even if null
    if "COUNTY" not in merged.columns:
        merged["COUNTY"] = None

    return merged


def render_distro_grid_uploader_section():
    st.subheader("📤 Upload Distribution Grid to Snowflake")

    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Snowflake connection not found.")
        return

    try:
        chain_options = fetch_distinct_values(conn, "CUSTOMERS", "CHAIN_NAME")
        chain_options.sort()
    except Exception as e:
        st.error(f"❌ Could not load chain names: {e}")
        return

    selected_chain = st.selectbox("Select Chain Name", chain_options, key="distro_grid_chain_select")
    uploaded_file = st.file_uploader("Upload Formatted Distro Grid File", type=["xlsx"], key="distro_grid_final_upload")

    if uploaded_file and selected_chain:
        try:
            df = pd.read_excel(uploaded_file, engine="openpyxl")
           # st.write("📋 Preview of formatted distro grid:")
            st.dataframe(df.head())
            df = enrich_with_customer_data(df, conn)

            if st.button("Upload Distribution Grid to Database", key="upload_distro_grid_btn"):
                with st.spinner("📤 Uploading to Database..."):
                    upload_distro_grid_to_snowflake(df, selected_chain, st.spinner)
        except Exception as e:
            st.error(f"❌ Failed to upload distro grid: {e}")

def update_spinner(message):
    st.text(f"{message} ...")
