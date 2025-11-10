# ------------- home.py ----------------

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from streamlit.components.v1 import html
from utils.ui_helpers import render_supplier_filter
from utils.dashboard_data import home_dashboard
from utils.dashboard_data.home_dashboard import get_execution_summary, fetch_chain_schematic_data
from sf_connector.service_connector import connect_to_tenant_snowflake
from utils.home_ui_helpers import (
    render_salesperson_and_gap_summary,
    render_supplier_scatter,
    render_execution_summary_card,
    render_chain_bar_chart
)


from io import BytesIO

def render():
    

    display_dashboard()


def display_dashboard():
    from utils.dashboard_data.home_dashboard import (
        get_execution_summary,
        fetch_chain_schematic_data,
        fetch_supplier_names,
        fetch_salesperson_summary,
        fetch_supplier_schematic_summary_data
    )

    # ---------------- Get Tenant Info ----------------
    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("Tenant information not found.")
        return
    tenant_config = st.session_state.get("tenant_config")
    conn = st.session_state.get("conn")
    if not conn:
        st.error("Missing connection or tenant_config in session.")
        return


   

    # ---------------- Header: Tenant Name + Welcome ----------------
    tenant_name = toml_info.get("tenant_name", "Unknown Tenant")
    st.markdown(f"<h1>{tenant_name} Chain Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p style='margin-top: 0rem; font-size: 1rem; color: gray;'>Welcome to your data intelligence hub.</p>", unsafe_allow_html=True)

   # ---------------- 1. Execution Summary + Bar Chart ----------------
    row1_col1, row1_col2 = st.columns(2, gap="large")

    try:
       
    

        total_in_schematic, total_purchased, total_gaps, purchased_pct = get_execution_summary(conn)
        missed_revenue = total_gaps * 40.19

        # --- Styled Execution Summary Card ---
        with row1_col1:
            st.markdown(
               f"""
                <div style='
                    background-color: #F8F2EB;
                    secondaryBackgroundColor: #ff0000;
                    padding: 30px;
                    border-radius: 10px;
                    #box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15);
                    text-align: center;
                    border:border_color;  /* Add dark grey border */
                    height: 50vh;  /* Set a minimum height */'>
                    <p> Execution Summary<p>
                    <p>Total In Schematic: {total_in_schematic}</p>
                    <p>Total Purchased: {total_purchased}</p>
                    <p>Total Gaps: {total_gaps}</p>
                    <p>Overall Purchased Percentage: {purchased_pct:.2f}%</p>
                    <p>Overall Missed Revenue: ${missed_revenue: .2f}</p>

                </div>
                """,
                unsafe_allow_html=True
            )

        # --- Chain Bar Chart ---
        with row1_col2:
            # normalize from session in case local var is None/out-of-scope
            conn = st.session_state.get("conn", conn if "conn" in locals() else None)
            tenant_config = tenant_config or st.session_state.get("tenant_config")

            if not conn or not tenant_config:
                st.error("? Missing connection or tenant_config in session.")
                st.info(f"has_conn={conn is not None}, has_tc={tenant_config is not None}")
                st.stop()

            chain_summary_df = fetch_chain_schematic_data(conn, tenant_config)
            if not chain_summary_df.empty:
                bar_chart = (
                    alt.Chart(chain_summary_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("CHAIN_NAME:N", title="Chain"),
                        y=alt.Y("Total_In_Schematic:Q", title="In Schematic"),
                        color=alt.Color("CHAIN_NAME:N", scale=alt.Scale(scheme="viridis")),
                        tooltip=["CHAIN_NAME", "Total_In_Schematic", "Purchased", "Purchased_Percentage"]
                    )
                    .properties(width=800, height=310, background="#F8F2EB")
                    .configure_title(align="center", fontSize=16)
                    .configure_mark(fontSize=14)
                )
                # Streamlit deprecation: replace use_container_width
                st.altair_chart(bar_chart, width="content")
            else:
                st.warning("No chain summary data available.")

    except Exception as e:
        row1_col1.error("? Failed to render execution summary or bar chart")
        row1_col1.exception(e)

    st.markdown("---")


      # ---------------- 2. Salesperson Table & Gap History ----------------
    with st.container():
        row2_col1, row2_col2 = st.columns([40, 70], gap="small")

        try:
            query = """
                SELECT SALESPERSON, TOTAL_DISTRIBUTION, TOTAL_GAPS, EXECUTION_PERCENTAGE 
                FROM SALESPERSON_EXECUTION_SUMMARY 
                ORDER BY TOTAL_GAPS DESC
            """
            salesperson_df = pd.read_sql_query(query, conn)

            # --- Format columns ---
            salesperson_df["EXECUTION_PERCENTAGE"] = salesperson_df["EXECUTION_PERCENTAGE"].astype(float).round(2)
            salesperson_df = salesperson_df.rename(
                columns={
                    "SALESPERSON": "Salesperson",
                    "TOTAL_DISTRIBUTION": "Distribution",
                    "TOTAL_GAPS": "Gaps",
                    "EXECUTION_PERCENTAGE": "Execution Percentage"
                }
            )

            # --- Limit and apply bold to Salesperson column ---
            limited_df = salesperson_df.head(100)
            table_html = limited_df.to_html(classes=["table", "table-striped"], escape=False, index=False)

            for index, row in limited_df.iterrows():
                table_html = table_html.replace(
                    f"<td>{row['Salesperson']}</td>",
                    f"<td style='font-weight: bold;'>{row['Salesperson']}</td>"
                )

            # --- Styled wrapper around table with smaller font ---
            table_container_style = """
                max-height: 365px;
                overflow-y: auto;
                background-color: #F8F2EB;
                text-align: center;
                padding: 1% 2% 2% 0%;
                border-radius: 10px;
                border-left: 0.5rem solid #9AD8E1 !important;
                box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important;
                width: 100%;
                font-size: 0.85rem;
            """
            table_html_wrapped = f"<div style='{table_container_style}'>{table_html}</div>"

            # --- Display in left column ---
            with row2_col1:
                st.markdown(table_html_wrapped, unsafe_allow_html=True)

                excel_data = BytesIO()
                salesperson_df.to_excel(excel_data, index=False)
                excel_data.seek(0)

                st.download_button(
                    "Download Salesperson Summary",
                    data=excel_data,
                    file_name="salesperson_execution_summary.xlsx"
                )

        except Exception as e:
            row2_col1.error("? Failed to load salesperson summary")
            row2_col1.exception(e)


       # --- Gap Pivot Table ---
    try:
        gap_query = """
            SELECT SALESPERSON, TOTAL_GAPS, EXECUTION_PERCENTAGE, LOG_DATE 
            FROM SALESPERSON_EXECUTION_SUMMARY_TBL 
            ORDER BY TOTAL_GAPS DESC
        """
        gap_df = pd.read_sql_query(gap_query, conn)

        # Rename columns to match production format
        gap_df = gap_df.rename(columns={
            "SALESPERSON": "Salesperson",
            "TOTAL_GAPS": "Gaps",
            "EXECUTION_PERCENTAGE": "Execution Percentage",
            "LOG_DATE": "Log Date"
        })

        # Limit to 100 rows, then pivot on most recent 12 dates
        gap_df_sorted = gap_df.sort_values(by="Log Date", ascending=False)
        latest_dates = gap_df_sorted["Log Date"].drop_duplicates().head(12)

        gap_df_pivot = gap_df.pivot_table(
            index="Salesperson",
            columns="Log Date",
            values="Gaps",
            aggfunc="sum",
            margins=False
        )

        # Reorder columns and format as short dates
        gap_df_pivot_limited = gap_df_pivot[latest_dates]
        gap_df_pivot_limited.columns = pd.to_datetime(gap_df_pivot_limited.columns).strftime("%y/%m/%d")

        # Convert to HTML with styling
        table_html = gap_df_pivot_limited.to_html(classes=["table", "table-striped"], escape=False)

        # Apply layout and font styling
        table_container_style = """
            max-height: 365px;
            overflow-y: auto;
            background-color: #F8F2EB;
            text-align: center;
            padding: 1% 2% 2% 0%;
            border-radius: 10px;
            border-left: 0.5rem solid #9AD8E1 !important;
            box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important;
            width: 100%;
            font-size: 0.85rem;
        """
        colgroup_html = "".join([f"<col style='width: {100 / len(gap_df_pivot_limited.columns):.2f}%;'>" for _ in gap_df_pivot_limited.columns])
        table_with_scroll = f"<div style='{table_container_style}'><table><colgroup>{colgroup_html}</colgroup>{table_html}</table></div>"

        # Display in column
        row2_col2.markdown(table_with_scroll, unsafe_allow_html=True)

        # Download button
        excel_data_pivot = BytesIO()
        gap_df_pivot_limited.to_excel(excel_data_pivot, index=True)
        excel_data_pivot.seek(0)
        row2_col2.download_button(
            "Download Gap History",
            data=excel_data_pivot,
            file_name="gap_history_report.xlsx"
        )
    except Exception as e:
        row2_col2.error("? Failed to load gap pivot table")
        row2_col2.exception(e)


   # ---------------- 3. Supplier Scatter Plot ----------------
    st.subheader("Supplier Performance Scatter")

    st.markdown("""
        <style>
        /* Shrink dropdown options */
        div[data-testid="stMultiSelect"] label {
            font-size: 0.85rem !important;
        }

        /* Shrink selected item text */
        div[data-testid="stMultiSelect"] span {
            font-size: 0.85rem !important;
        }
        </style>
    """, unsafe_allow_html=True)
   


    # ? Inject supplier multiselect just above the chart logic
    render_supplier_filter()

    selected_suppliers = st.session_state.get("selected_suppliers", [])

    if selected_suppliers:
        df_supplier = fetch_supplier_schematic_summary_data(conn, selected_suppliers)
        render_supplier_scatter(df_supplier)
    else:
        st.info("Please select suppliers from the sidebar to view the scatter chart.")


    # ---------------- Close Connection ----------------
    conn.close()
