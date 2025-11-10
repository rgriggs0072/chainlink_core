# app_pages/data_exports.py

import streamlit as st
import pandas as pd
from io import BytesIO
from sf_connector.service_connector import connect_to_tenant_snowflake

def render():
    st.title("Data Exports")
    st.markdown("Download existing data for your selected chain or across all chains.")

    report_type = st.selectbox("Select Report Type", ["Distro Grid", "Reset Schedule"])

    # ? Get tenant-specific connection info from session state
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("Tenant configuration not found. Please log in again.")
        return

    # ? Connect to tenant Snowflake using secure key
    conn = connect_to_tenant_snowflake(toml_info)
    if not conn:
        st.error("? Unable to connect to Snowflake.")
        return

    database = toml_info["database"]
    schema = toml_info["schema"]
    full_table_prefix = f"{database}.{schema}"

    # ?? Fetch chains from the DISTRO_GRID table
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT CHAIN_NAME FROM {full_table_prefix}.DISTRO_GRID")
        chain_list = [row[0] for row in cur.fetchall()]

    # Dropdown + checkbox UI
    chain_list = [""] + sorted(chain_list)
    selected_chain = st.selectbox("Select Chain", options=chain_list)
    download_all = st.checkbox("Export All Chains")

    if st.button("Export Data"):
        if not download_all and not selected_chain:
            st.warning("Please select a chain or choose to export all chains.")
            return

        with st.spinner("Fetching and preparing data..."):
            table_map = {
                "Distro Grid": "DISTRO_GRID",
                "Reset Schedule": "RESET_SCHEDULE"
            }

            target_table = table_map.get(report_type)
            if not target_table:
                st.error("Invalid report type selected.")
                return

            query = f"SELECT * FROM {full_table_prefix}.{target_table}"
            if not download_all:
                query += f" WHERE CHAIN_NAME = '{selected_chain}'"

            df = pd.read_sql(query, conn)

            if df.empty:
                st.warning("No data found for the selected criteria.")
            else:
                # Strip timezone info for Excel compatibility
                for col in df.select_dtypes(include=["datetimetz"]).columns:
                    df[col] = df[col].dt.tz_localize(None)

                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name=report_type.replace(" ", "_"))
                output.seek(0)

                filename = f"{report_type.replace(' ', '_')}_{selected_chain if not download_all else 'All'}.xlsx"
                st.download_button(
                    label=f"Download {report_type} Report",
                    data=output,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
