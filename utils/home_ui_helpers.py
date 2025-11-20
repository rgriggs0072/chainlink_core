import streamlit as st
import pandas as pd
import altair as alt

"""
Home Page UI Helpers (Chainlink Core)

Overview for future devs:
- This module holds reusable UI components for the Home page.
- All salesperson + gap history logic now lives directly in app_pages/home.py.
- These helpers focus on:
    * Supplier performance scatter chart
    * Execution summary card
    * Chain-level bar chart
"""


def render_supplier_scatter(df_supplier: pd.DataFrame) -> None:
    """
    Render supplier performance scatter plot.

    Business meaning:
        - Each point is a product for a selected supplier.
        - X-axis: how many schematic placements exist (Total_In_Schematic).
        - Y-axis: what % of those placements actually purchased (Purchased_Percentage).
        - Color: PRODUCT_NAME to visually distinguish SKUs.

    Args:
        df_supplier: DataFrame with at least:
            - PRODUCT_NAME
            - UPC
            - Total_In_Schematic
            - Total_Purchased
            - Purchased_Percentage (0–100 scale)
    """
    if df_supplier.empty:
        st.warning("No supplier data available for selected options.")
        return

    df_plot = df_supplier.copy()
    # Convert 0–100 percentage to 0–1 for Altair's percentage formatting
    df_plot["Purchased_Percentage_Display"] = df_plot["Purchased_Percentage"] / 100.0

    scatter_chart = (
        alt.Chart(df_plot)
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
                alt.Tooltip(
                    "Purchased_Percentage_Display:Q",
                    format=".2%",
                    title="Purchased %",
                ),
            ],
        )
        .properties(width=800, height=400, background="#F8F2EB")
        .interactive()
    )

    st.altair_chart(scatter_chart, width="content")


def render_execution_summary_card(
    container,
    total_in_schematic: int,
    total_purchased: int,
    total_gaps: int,
    purchased_pct: float,
    missed_revenue: float,
) -> None:
    """
    Render a summary card for high-level execution stats.

    Args:
        container: Streamlit container/column to render into.
        total_in_schematic: Total schematic placements across the tenant.
        total_purchased: Total placements with at least one purchase.
        total_gaps: Total placements with zero purchases.
        purchased_pct: Percent purchased (0–100).
        missed_revenue: Estimated missed revenue in dollars.
    """
    container.markdown(
        f"""
        <div style="
            background-color: #F8F2EB;
            border: 2px solid #ccc;
            border-radius: 10px;
            padding: 20px;
            text-align: center;
        ">
            <h4>Execution Summary</h4>
            <p><strong>Total In Schematic:</strong> {total_in_schematic:,}</p>
            <p><strong>Total Purchased:</strong> {total_purchased:,}</p>
            <p><strong>Total Gaps:</strong> {total_gaps:,}</p>
            <p><strong>Purchased %:</strong> {purchased_pct:.2f}%</p>
            <p><strong>Missed Revenue:</strong> ${missed_revenue:,.2f}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chain_bar_chart(container, df: pd.DataFrame) -> None:
    """
    Render a bar chart of schematic counts by chain.

    Args:
        container: Streamlit container/column to render into.
        df: DataFrame with at least:
            - CHAIN_NAME
            - Total_In_Schematic
            - Purchased
            - Purchased_Percentage (0–100 or 0–1; tooltip is raw)
    """
    if df.empty:
        container.warning("No chain-level data available.")
        return

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("CHAIN_NAME:N", title="Chain Name"),
            y=alt.Y("Total_In_Schematic:Q", title="In Schematic"),
            color="CHAIN_NAME:N",
            tooltip=[
                "CHAIN_NAME",
                "Total_In_Schematic",
                "Purchased",
                "Purchased_Percentage",
            ],
        )
        .properties(width=500, height=300, background="#F8F2EB")
    )

    container.altair_chart(chart, width="stretch")
