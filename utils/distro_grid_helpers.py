# ----------- utils/distro_grid_helpers.py -----------
"""
Distro Grid helpers

Overview for future devs:
- Backend support for the Distro Grid workflow:
  * IP + logging helpers (LOG table).
  * Legacy formatters (format_non_pivot_table / format_pivot_table).
  * Core upload pipeline:
      - enrich_with_customer_data()
      - sanitize_dataframe_for_snowflake()
      - load_data_into_distro_grid()
      - upload_distro_grid_to_snowflake()
  * Snowflake procedure invocation: call_procedure_update_DG().

Notes:
- All Streamlit *UI* (forms, selectboxes, layout) must live in
  app_pages/distro_grid_sections.py.
- This module may emit Streamlit messages (st.error, st.success, etc.)
  but should not define pages or layout components.
"""

from __future__ import annotations

from datetime import datetime
import socket

import pandas as pd
import streamlit as st
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

from utils.distro_grid.schema import infer_season_label
from sf_connector.service_connector import connect_to_tenant_snowflake


# ====================================================================================================================
# IP helper
# ====================================================================================================================

def get_local_ip() -> str | None:
    """
    Return the local IP address for logging purposes.

    Falls back to None and logs to stdout on failure; does not raise.
    """
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception as e:
        print(f"Error getting IP: {e}")
        return None


# ====================================================================================================================
# Legacy season helper (no longer used by new flow; safe to delete once old code is gone)
# ====================================================================================================================

def get_season_options():
    """
    Legacy helper to build a static season list.

    New flow uses infer_season_label() at upload time and does not expose
    a season picker in the UI, but this function is left for backward
    compatibility until all old callers are removed.
    """
    current_year = datetime.now().year
    return [
        f"Spring {current_year}",
        f"Fall {current_year}",
        f"Spring {current_year + 1}",
        f"Fall {current_year + 1}",
    ]


# ====================================================================================================================
# Legacy formatters (kept for compatibility; new standard path uses utils.distro_grid.formatters)
# ====================================================================================================================

def format_non_pivot_table(workbook, stream=None, selected_option=None):
    """
    LEGACY formatter for standard column-format Distribution Grid Excel workbook.

    - Cleans apostrophes in STORE_NAME and hyphens in UPC.
    - Validates required fields and CHAIN_NAME.
    - Emits Streamlit UI warnings/errors and may st.stop().

    New flows should prefer utils.distro_grid.formatters.format_uploaded_grid().
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
            rows_with_missing_values.append(
                f"Row {idx + 2}: Missing {', '.join(missing_fields)}"
            )

    # Block on missing required fields
    if rows_with_missing_values:
        st.session_state["warnings_present"] = True
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
            st.error(
                f"❌ {len(chain_mismatch_rows)} row(s) have CHAIN_NAME values that "
                f"do not match your selection: '{selected_option}'"
            )
            st.dataframe(chain_mismatch_rows)
            st.warning("Please correct the chain name in the file and try again.")
            if st.button("🔁 Clear Upload and Try Again"):
                st.session_state.pop("distro_grid_final_upload", None)
                st.rerun()
            st.stop()

    # Informational cleanup feedback
    if rows_with_apostrophe_issues:
        st.info(
            f"Cleaned apostrophes or smart quotes from "
            f"{len(rows_with_apostrophe_issues)} store name(s)."
        )

    if rows_with_upc_hyphens:
        st.info(
            f"Removed hyphens from {len(rows_with_upc_hyphens)} UPC(s) in the sheet."
        )

    st.success("✅ Formatting complete. File cleaned and ready for upload.")
    return df


def format_pivot_table(workbook, selected_option):
    """
    LEGACY formatter for pivot-style Distribution Grid workbooks.

    New flows will eventually replace this with a standardized pivot formatter
    in utils.distro_grid.formatters, but for now this stays as-is.
    """


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
        value_name="Yes/No",
    )

    # Replace Yes/No/checkmarks with binary values
    df_melted["Yes/No"] = df_melted["Yes/No"].apply(
        lambda x: "Yes" if x == 1 else ("No" if pd.isna(x) else "*")
    )

    # Reorder & rename
    df_melted.insert(0, "STORE_NAME", "")
    df_melted.rename(
        columns={
            "Name": "PRODUCT_NAME",
            "Yes/No": "YES_NO",
            "SKU": "SKU",
        },
        inplace=True,
    )

    # Reorder for import structure
    df_melted = df_melted[
        ["STORE_NAME", "STORE_NUMBER", "UPC"]
        + [col for col in df_melted.columns if col not in ["STORE_NAME", "STORE_NUMBER", "UPC"]]
    ]

    # Clean characters and normalize
    df_melted = df_melted.replace(
        {"'": "", ",": "", r"\*": "", "Yes": "1", "No": "0"}, regex=True
    )

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


# ====================================================================================================================
# Logging helpers
# ====================================================================================================================

def insert_log_entry(
    user_id: str,
    activity_type: str,
    description: str,
    success: bool,
    ip_address: str,
    user_agent: str | None = None,
):
    """
    Insert an activity row into the tenant LOG table.

    LOG table schema:
        LOG_ID    — auto-increment identity
        EVENT_TS  — timestamp, defaults to CURRENT_TIMESTAMP()
        LEVEL     — 'INFO' or 'ERROR'
        TENANT_ID — tenant identifier
        MESSAGE   — human-readable description of the event
        CONTEXT   — JSON variant for structured extra data
                    (stores user_id, activity_type, ip_address, user_agent)

    Parameters:
        user_id:       Application user identifier.
        activity_type: Short label (e.g., 'UPDATE_DISTRO_GRID').
        description:   Human-readable detail about the action.
        success:       True/False flag.
        ip_address:    IP address from session (or 'unknown').
        user_agent:    Optional extra context (e.g., chain name or browser UA).
    """
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        # Don't blow up the app over logging
        print("insert_log_entry: toml_info missing; skipping log insert.")
        return

    try:
        conn = connect_to_tenant_snowflake(toml_info)
        cursor = conn.cursor()

        # Build structured context as JSON — stores fields that don't have
        # dedicated columns in the LOG table
        import json
        context = json.dumps({
            "user_id": user_id,
            "activity_type": activity_type,
            "ip_address": ip_address,
            "user_agent": user_agent or "",
        })

        level = "INFO" if success else "ERROR"
        tenant_id = toml_info.get("tenant_id", "unknown")

        cursor.execute(
            """
            INSERT INTO LOG (EVENT_TS, LEVEL, TENANT_ID, MESSAGE, CONTEXT)
            SELECT CURRENT_TIMESTAMP(), %s, %s, %s, PARSE_JSON(%s)
            FROM (SELECT 1)
            """,
            (
                level,
                tenant_id,
                f"[{activity_type}] {description}",
                context,
            ),
        )


        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error occurred while inserting log entry: {str(e)}")


def update_spinner(message: str):
    """
    Simple text-based progress updater used by upload_distro_grid_to_snowflake.
    """
    st.text(f"{message} ...")


def call_procedure_update_DG(selected_chain: str | None = None):
    """
    Calls the UPDATE_DISTRO_GRID(CHAIN_NAME_FILTER) procedure for the current
    tenant database/schema.

    Args:
        selected_chain: If provided, only updates DISTRO_GRID rows for that
                        chain. If None, updates all chains (full refresh).

    Notes:
    - Passes chain directly as a parameter — no session variable needed.
    - Call with selected_chain=None from a Snowflake worksheet or admin tool
      to run a full refresh across all chains.
    """
    try:
        toml_info = st.session_state["toml_info"]
        conn = connect_to_tenant_snowflake(toml_info)
        cur = conn.cursor()

        db = toml_info["database"]
        schema = toml_info["schema"]

        # Pass chain as parameter (NULL = all chains)
        proc_call = f'CALL "{db}"."{schema}".UPDATE_DISTRO_GRID(%s)'
        cur.execute(proc_call, (selected_chain,))

        result = cur.fetchone()

        if result:
            scope = f"chain '{selected_chain}'" if selected_chain else "all chains"
            st.success(f"✅ UPDATE_DISTRO_GRID complete ({scope}): {result[0]}")
        else:
            st.warning("⚠️ UPDATE_DISTRO_GRID completed but returned no result.")

        cur.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Procedure call failed: {e}")


def log_update_result(conn, user_id, success, message, ip_address=""):
    """
    Legacy helper: logs the result of the update_distro_grid procedure
    into a hard-coded LOG table.

    New flows should prefer insert_log_entry(), which uses tenant toml_info.
    """
    activity = "UPDATE_DISTRO_GRID"
    query = """
        INSERT INTO DELTAPACIFIC_DB.DELTAPACIFIC_SCH.LOG
        (USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS)
        VALUES (%s, %s, %s, %s, %s)
    """
    conn.cursor().execute(query, (user_id, activity, message, success, ip_address))


# ====================================================================================================================
# Core upload pipeline
# ====================================================================================================================

def load_data_into_distro_grid(conn, df, selected_chain, season: str):
    """
    Insert cleaned distro grid DataFrame into the tenant-specific DISTRO_GRID
    table in Snowflake, with archive protection via DG_ARCHIVE_TRACKING.

    All steps (archive, delete, insert) run inside a single transaction.
    If any step fails the entire transaction is rolled back and the error
    is surfaced to the user — leaving DISTRO_GRID untouched.

    Archive strategy (v1.2.0):
    - DISTRO_GRID_ARCHIVE_FULL: receives everything from DISTRO_GRID for the
      chain — all UPCs matched or not. Used for data recovery.
    - DISTRO_GRID_MATCHED_ARCHIVE: receives only Delta Pacific placements
      (PRODUCT_ID <> 0, valid COUNTY, authorized SUPPLIER_COUNTY join).
      Used by Placement Intelligence for season-over-season comparisons.

    Parameters:
        conn:           Active Snowflake connection.
        df:             Cleaned and enriched distro grid data.
        selected_chain: Chain being uploaded (as displayed in UI).
        season:         Season label (e.g., "Spring 2025"). Usually inferred
                        via infer_season_label() in upload_distro_grid_to_snowflake().
    """
    cur = conn.cursor()
    toml_info = st.session_state.get("toml_info", {})
    db, schema = toml_info.get("database"), toml_info.get("schema")

    if not db or not schema:
        raise ValueError("Missing database or schema in session state (toml_info).")

    dg_table = f'"{db}"."{schema}".DISTRO_GRID'
    dg_archive_full_table = f'"{db}"."{schema}".DISTRO_GRID_ARCHIVE_FULL'       # renamed from DISTRO_GRID_ARCHIVE — full recovery backup
    dg_archive_matched_table = f'"{db}"."{schema}".DISTRO_GRID_MATCHED_ARCHIVE' # new — filtered archive for Placement Intelligence
    archive_tracking_table = f'"{db}"."{schema}".DG_ARCHIVE_TRACKING'

    chain_upper = selected_chain.strip().upper()

    if "TENANT_ID" not in df.columns:
        tenant_id = st.session_state.get("tenant_id")
        if not tenant_id:
            raise ValueError("Missing tenant_id in session. Cannot upload.")
        df["TENANT_ID"] = tenant_id
        df["PRODUCT_ID"] = None

    try:
        # Begin explicit transaction — nothing commits until we say so
        conn.autocommit(False)

        # 🔍 Step 1: Check archive tracking — only archive once per chain+season
        cur.execute(
            f"SELECT 1 FROM {archive_tracking_table} WHERE CHAIN_NAME = %s AND SEASON = %s",
            (chain_upper, season),
        )
        already_archived = cur.fetchone() is not None

        # 📦 Step 2: Archive current DG records (once per chain+season)
        if not already_archived:

            # Shared column lists used by both archive INSERT statements
            archive_columns = """
                TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME, STORE_NUMBER,
                PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
                SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY,
                ARCHIVE_DATE, CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
            """
            archive_select = """
                TENANT_ID, CUSTOMER_ID, CHAIN_NAME, STORE_NAME, STORE_NUMBER,
                PRODUCT_ID, UPC, SKU, PRODUCT_NAME, MANUFACTURER,
                SEGMENT, YES_NO, ACTIVATION_STATUS, COUNTY,
                CURRENT_DATE(), CREATED_AT, UPDATED_AT, LAST_LOAD_DATE
            """

            # Step 2A — Write full archive (everything, no filter)
            # Captures all UPCs from the chain grid regardless of whether
            # Delta Pacific carries them. Used for data recovery only.
            cur.execute(f"""
                INSERT INTO {dg_archive_full_table} ({archive_columns})
                SELECT {archive_select}
                FROM {dg_table}
                WHERE TRIM(UPPER(CHAIN_NAME)) = %s
            """, (chain_upper,))

      

            # Step 2B — Write matched archive (filtered to Delta Pacific placements only)
            # Three-way filter ensures only valid placements are archived:
            #   1. PRODUCT_ID <> 0    — product exists in Delta Pacific catalog
            #   2. COUNTY is valid    — store is in a served territory
            #   3. SUPPLIER_COUNTY    — manufacturer is authorized for that county
            # This is the archive that Placement Intelligence reads from.
            # UPPER(TRIM()) wrapping on manufacturer join is intentional —
            # inconsistent casing between tables has caused issues before.
            cur.execute(f"""
                INSERT INTO {dg_archive_matched_table} ({archive_columns})
                SELECT
                    dg.TENANT_ID, dg.CUSTOMER_ID, dg.CHAIN_NAME, dg.STORE_NAME, dg.STORE_NUMBER,
                    dg.PRODUCT_ID, dg.UPC, dg.SKU, dg.PRODUCT_NAME, dg.MANUFACTURER,
                    dg.SEGMENT, dg.YES_NO, dg.ACTIVATION_STATUS, dg.COUNTY,
                    CURRENT_DATE(), dg.CREATED_AT, dg.UPDATED_AT, dg.LAST_LOAD_DATE
                FROM {dg_table} dg
                INNER JOIN "{db}"."{schema}".SUPPLIER_COUNTY sc
                    ON UPPER(TRIM(sc.SUPPLIER)) = UPPER(TRIM(dg.MANUFACTURER))
                    AND UPPER(TRIM(sc.COUNTY)) = UPPER(TRIM(dg.COUNTY))
                    AND sc.STATUS = 'Yes'
                    AND sc.TENANT_ID = dg.TENANT_ID
                WHERE TRIM(UPPER(dg.CHAIN_NAME)) = %s
                AND dg.PRODUCT_ID <> 0
                AND dg.COUNTY IS NOT NULL
                AND dg.COUNTY <> 'None'
            """, (chain_upper,))

            # Step 2C — Update tracking for both archives
            # FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT are stamped together
            # since both archives are written in the same transaction.
            cur.execute(f"""
                INSERT INTO {archive_tracking_table} 
                    (CHAIN_NAME, SEASON, FULL_ARCHIVED_AT, MATCHED_ARCHIVED_AT)
                VALUES (%s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """, (chain_upper, season))

        else:
            st.warning(
                f"⚠️ Archive already exists for {chain_upper} - {season}. Skipping archive step."
            )

        # 🧹 Step 3: Delete old DISTRO_GRID rows for this chain
        cur.execute(
            f"DELETE FROM {dg_table} WHERE TRIM(UPPER(CHAIN_NAME)) = %s",
            (chain_upper,),
        )

        # 📥 Step 4: Validate + insert new data
        insert_columns = [
            "CUSTOMER_ID",
            "CHAIN_NAME",
            "STORE_NAME",
            "STORE_NUMBER",
            "UPC",
            "SKU",
            "PRODUCT_ID",
            "PRODUCT_NAME",
            "MANUFACTURER",
            "SEGMENT",
            "YES_NO",
            "ACTIVATION_STATUS",
            "COUNTY",
            "TENANT_ID",
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

        # Pre-flight null check (before touching DB)
        # Nullable columns are allowed to be None/empty — all others must have a value
        nullable = {
            "CUSTOMER_ID",
            "PRODUCT_ID",
            "MANUFACTURER",
            "COUNTY",
            "SEGMENT",
            "ACTIVATION_STATUS",
        }
        for i, row in enumerate(records):
            for j, val in enumerate(row):
                col_name = insert_columns[j]
                if col_name not in nullable and (
                    pd.isna(val) or str(val).strip().upper() == "NAN"
                ):
                    raise ValueError(
                        f"Row {i + 1}, column '{col_name}' has an invalid null value. "
                        f"Please fix the data and re-upload."
                    )

        cur.executemany(insert_query, records)

        # ✅ All steps succeeded — commit the full transaction
        conn.commit()

    except Exception as e:
        # ❌ Any failure rolls back archive + delete + insert atomically
        # DISTRO_GRID and both archive tables are left completely untouched
        try:
            conn.rollback()
        except Exception:
            pass
        st.error(
            f"❌ Upload failed and was fully rolled back. DISTRO_GRID is unchanged.\n\n"
            f"Error detail: {e}"
        )
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass

def sanitize_dataframe_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure DataFrame is safe for Snowflake insert:
    - Replaces NaN/None with safe defaults.
    - Ensures no literal 'nan' strings.
    - Casts numerics properly.
    """
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(
            df[col]
        ):
            df[col] = (
                df[col]
                .astype(str)
                .replace(r"(?i)^nan$", "", regex=True)
                .fillna("")
            )
        else:
            df[col] = df[col].fillna("")
    return df


def upload_distro_grid_to_snowflake(
    df: pd.DataFrame,
    selected_chain: str,
    selected_season: str | None,
    update_spinner_callback,
):
    """
    Upload cleaned distro grid data to Snowflake for the selected chain.

    Parameters:
        df:              Cleaned distro grid data (prior to CUSTOMER_ID enrichment).
        selected_chain:  Chain name being uploaded.
        selected_season: Optional season label. If None, infer via
                         infer_season_label() (Spring/Fall <year>).
        update_spinner_callback: Function to update progress spinner text.
    """
    toml_info = st.session_state.get("toml_info")
    tenant_id = st.session_state.get("tenant_id")
    user_id = st.session_state.get("user_id", "unknown")
    ip_address = st.session_state.get("ip_address", "unknown")

    if not toml_info or not tenant_id:
        st.error("❌ Tenant configuration missing.")
        return

    # 🔎 Decide season label (UI can pass None; we infer here)
    season = selected_season or infer_season_label()
    st.info(f"Using season label for archive: **{season}**")

    # ✅ Add missing columns before enrichment
    if "TENANT_ID" not in df.columns:
        df["TENANT_ID"] = tenant_id
    if "PRODUCT_ID" not in df.columns:
        df["PRODUCT_ID"] = 0

    # 🔄 Enrich with CUSTOMER_ID and corrected STORE_NAME
    conn = connect_to_tenant_snowflake(toml_info)
    df = enrich_with_customer_data(df, conn)

    # 🧼 Sanitize after enrichment
    df = sanitize_dataframe_for_snowflake(df)

    # ⚠️ Normalize CUSTOMER_ID nulls
    if "CUSTOMER_ID" in df.columns:
        df["CUSTOMER_ID"] = df["CUSTOMER_ID"].replace({0: None})
        # If you want explicit warning, uncomment:
        # unmatched = df[df["CUSTOMER_ID"].isnull()]
        # if not unmatched.empty:
        #     st.warning(f"{len(unmatched)} rows had no CUSTOMER_ID match and were set to NULL.")

    upload_succeeded = False

    try:
        st.markdown("### 🚚 Upload Progress")

        # 1️⃣ Archive step (inside load_data_into_distro_grid)
        update_spinner_callback(
            f"1️⃣ Archiving existing records for {selected_chain} ({season}) ..."
        )

        # 2️⃣ Delete + Insert
        update_spinner_callback(
            f"2️⃣ Deleting old records and inserting new grid data for {selected_chain} ..."
        )
        load_data_into_distro_grid(conn, df, selected_chain, season)
        upload_succeeded = True

        st.success(
            f"✅ Uploaded {len(df)} records for '{selected_chain}' into DISTRO_GRID."
        )

        # 3️⃣ Post-procedure
        update_spinner_callback(
            "3️⃣ Running post-upload update procedure (UPDATE_DISTRO_GRID) ..."
        )
        call_procedure_update_DG(selected_chain)

        # 🧾 Log success
        insert_log_entry(
            user_id,
            "UPDATE_DISTRO_GRID",
            f"Upload complete for chain: {selected_chain}, season: {season}",
            True,
            ip_address,
            selected_chain,
        )
        update_spinner_callback(f"✅ Upload complete for {selected_chain} ({season})")

    except Exception as e:
        if not upload_succeeded:
            # load_data_into_distro_grid already rolled back and showed the error.
            # Log the failure.
            insert_log_entry(
                user_id,
                "UPDATE_DISTRO_GRID",
                f"Upload FAILED for chain: {selected_chain}, season: {season}. Error: {e}",
                False,
                ip_address,
                selected_chain,
            )
        else:
            # Upload succeeded but post-procedure failed — surface the error
            st.error(f"❌ Post-upload procedure failed: {e}")
            insert_log_entry(
                user_id,
                "UPDATE_DISTRO_GRID",
                f"Upload succeeded but post-procedure failed for chain: {selected_chain}. Error: {e}",
                False,
                ip_address,
                selected_chain,
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ====================================================================================================================
# Enrichment helper
# ====================================================================================================================

def enrich_with_customer_data(distro_df: pd.DataFrame, conn) -> pd.DataFrame:
    """
    Enrich distro_df with CUSTOMER_ID, COUNTY, and corrected STORE_NAME
    using CHAIN_NAME + STORE_NUMBER matches from the CUSTOMERS table.
    """
    query = """
        SELECT CUSTOMER_ID,
               CHAIN_NAME,
               STORE_NUMBER,
               STORE_NAME AS CORRECT_STORE_NAME,
               COUNTY
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
        how="left",
    )

    # Assign CUSTOMER_ID from merged columns
    if "CUSTOMER_ID_y" in merged.columns:
        merged["CUSTOMER_ID"] = merged["CUSTOMER_ID_y"].fillna(
            merged.get("CUSTOMER_ID_x")
        )
        merged.drop(columns=["CUSTOMER_ID_x", "CUSTOMER_ID_y"], inplace=True)

    # ✅ Only overwrite STORE_NAME if CORRECT_STORE_NAME is non-null
    has_store_name_corrections = merged["CORRECT_STORE_NAME"].notnull()
    merged.loc[has_store_name_corrections, "STORE_NAME"] = merged.loc[
        has_store_name_corrections, "CORRECT_STORE_NAME"
    ]

    # Clean up
    merged.drop(columns=["CORRECT_STORE_NAME"], inplace=True)

    # ✅ Ensure COUNTY column exists even if null
    if "COUNTY" not in merged.columns:
        merged["COUNTY"] = None

    return merged
