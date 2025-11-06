import streamlit as st
import pandas as pd
import altair as alt
from io import BytesIO

def render_salesperson_and_gap_summary(col1, col2, df_sales, conn):
    from io import BytesIO

    if not df_sales.empty:
        col1.markdown(
            f"""
            <div style='max-height: 365px; overflow-y: auto; background-color: #F8F2EB; padding: 10px; border-radius: 10px; text-align: center;'>
                {df_sales.to_html(classes=["table", "table-striped"], escape=False, index=False)}
            </div>
            """,
            unsafe_allow_html=True,
        )

        excel_data = BytesIO()
        df_sales.to_excel(excel_data, index=False)
        col1.download_button(
            "Download Salesperson Summary",
            data=excel_data,
            file_name="salesperson_execution_summary.xlsx"
        )
    else:
        col1.info("No salesperson performance data available.")

    # Gap pivot table
    gap_query = """
        SELECT SALESPERSON, TOTAL_GAPS, EXECUTION_PERCENTAGE, LOG_DATE 
        FROM SALESPERSON_EXECUTION_SUMMARY_TBL 
        ORDER BY TOTAL_GAPS DESC
    """
    gap_df = pd.read_sql(gap_query, conn)
    if not gap_df.empty:
        gap_pivot = gap_df.pivot_table(index="SALESPERSON", columns="LOG_DATE", values="TOTAL_GAPS")
        gap_pivot.columns = pd.to_datetime(gap_pivot.columns).strftime("%y/%m/%d")

        col2.markdown(
            f"""
            <div style='max-height: 365px; overflow-y: auto; background-color: #F8F2EB; padding: 10px; border-radius: 10px;'>
                {gap_pivot.to_html(classes=["table", "table-striped"], escape=False)}
            </div>
            """,
            unsafe_allow_html=True,
        )

        excel_data_pivot = BytesIO()
        gap_pivot.to_excel(excel_data_pivot, index=True)
        col2.download_button(
            "Download Gap History",
            data=excel_data_pivot,
            file_name="gap_history_report.xlsx"
        )


def render_supplier_scatter(df_supplier):
   # st.subheader("📦 Supplier Performance Scatter")
    if not df_supplier.empty:
        df_supplier["Purchased_Percentage_Display"] = df_supplier["Purchased_Percentage"] / 100.0

        scatter_chart = (
            alt.Chart(df_supplier)
            .mark_circle(size=80)
            .encode(
                x=alt.X("Total_In_Schematic:Q", title="In Schematic"),
                y=alt.Y("Purchased_Percentage_Display:Q", title="Purchased %"),
                color="PRODUCT_NAME:N",
                tooltip=[
                    "PRODUCT_NAME",
                    "UPC",
                    "Total_In_Schematic",
                    "Total_Purchased",
                    alt.Tooltip("Purchased_Percentage_Display:Q", format=".2%", title="Purchased %")
                ]
            )
            .properties(width=800, height=400, background="#F8F2EB")
            .interactive()
        )

        st.altair_chart(scatter_chart, width="content")
    else:
        st.warning("No supplier data available for selected options.")



def render_execution_summary_card(container, total_in_schematic, total_purchased, total_gaps, purchased_pct, missed_revenue):
    container.markdown(
        f"""
        <div style="background-color: #F8F2EB; border: 2px solid #ccc; border-radius: 10px; padding: 20px; text-align: center;">
            <h4>Execution Summary</h4>
            <p><strong>Total In Schematic:</strong> {total_in_schematic}</p>
            <p><strong>Total Purchased:</strong> {total_purchased}</p>
            <p><strong>Total Gaps:</strong> {total_gaps}</p>
            <p><strong>Purchased %:</strong> {purchased_pct:.2f}%</p>
            <p><strong>Missed Revenue:</strong> ${missed_revenue:,.2f}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

def render_chain_bar_chart(container, df: pd.DataFrame):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("CHAIN_NAME:N", title="Chain Name"),
            y=alt.Y("Total_In_Schematic:Q", title="In Schematic"),
            color="CHAIN_NAME:N",
            tooltip=["CHAIN_NAME", "Total_In_Schematic", "Purchased", "Purchased_Percentage"]
        )
        .properties(width=500, height=300, background="#F8F2EB")
    )
    container.altair_chart(chart, use_container_width=True)